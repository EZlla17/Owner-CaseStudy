import csv
import concurrent.futures
import json
import os
import re
import time
from typing import Any, Dict, List, Tuple

import requests

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "situations-moments" / "data"
INPUT_CSV = str(BASE_DIR / "call-transcript.csv")
OUTPUT_CALL_LEVEL_JSONL = os.getenv(
    "OUTPUT_CALL_LEVEL_JSONL", str(DATA_DIR / "situation-moment-pairs-first50.jsonl")
)
OUTPUT_MOMENT_LEVEL_CSV = os.getenv("OUTPUT_MOMENT_LEVEL_CSV", str(DATA_DIR / "moment-level.csv"))
START_INDEX = int(os.getenv("START_INDEX", "0"))
MAX_CALLS = int(os.getenv("MAX_CALLS", "50"))
MAX_WORKERS = 10

API_KEY = os.getenv(
    "VERCEL_AI_GATEWAY_API_KEY",
    "",
)
API_URL = "https://ai-gateway.vercel.sh/v1/chat/completions"
MODEL = os.getenv("MODEL", "anthropic/claude-3-haiku")

SYSTEM_PROMPT = """You are an expert sales call analyst.
Return strict JSON only.
No markdown, no backticks, no extra commentary.
"""

USER_PROMPT_TEMPLATE = """Analyze this owner sales call transcript.

Your goal is to extract key moments that can influence whether the prospect books a demo.

Extract 1-5 key moments where the sales rep faced an important situation and responded.

For each extracted moment, return:
- situation: Extract the key situation the sales rep encountered from the prospect during the conversation that could affect demo booking.
- strategies: what strategies the rep used to handle that key situation
- effect: whether this key moment moved the call forward or not, based on context and the prospect's reaction. Use one of ["proceed", "not_proceed", "unclear"]

Important rules:
- A key situation should be a prospect-side challenge, objection, hesitation, confusion, constraint, buying signal, or decision point that changes whether the call can move toward a demo.
- Focus on moments with clear influence on booking outcome; skip low-impact details.
- The situation must be prospect-triggered (objection, hesitation, confusion, request, refusal, delay, concern, or constraint from the prospect).
- Do NOT put rep actions in situation. If the text is about what the rep did/said, it belongs in strategies.
- Examples that belong in strategies (not situation): rep introduction, local references, social proof examples, pitch framing, rebuttals, closing language.
- Exclude generic call-ending moments (polite goodbye, routine wrap-up, contact confirmation, simple sign-off) unless the ending itself is triggered by a clear prospect blocker that impacts demo booking.
- The situation should describe the specific key moment the rep faced, not generic background.
- The strategies field must describe the rep handling approach, not the prospect statement alone.
- Infer effect from surrounding context and reaction after the rep action.
- If the rep does not meaningfully handle the key moment, state that explicitly in strategies.
- Do not invent information.
- Return 1-5 most important moments if available.

Return JSON in this schema exactly:
{{
  "pairs": [
    {{
      "situation": "...",
      "strategies": "...",
      "effect": "proceed|not_proceed|unclear"
    }}
  ]
}}

Transcript:
{transcript}
"""

VALID_EFFECTS = {"proceed", "not_proceed", "unclear"}


def call_llm(transcript: str, retries: int = 3, sleep_s: float = 2.0) -> Dict[str, Any]:
    payload = {
        "model": MODEL,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": USER_PROMPT_TEMPLATE.format(transcript=transcript)},
        ],
        "temperature": 0.2,
    }
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json",
    }

    last_err = None
    for attempt in range(1, retries + 1):
        try:
            resp = requests.post(API_URL, headers=headers, json=payload, timeout=45)
            if resp.status_code >= 400:
                raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:500]}")
            data = resp.json()
            content = data["choices"][0]["message"]["content"]
            parsed = parse_json_content(content)
            return parsed
        except Exception as err:
            last_err = err
            if attempt < retries:
                time.sleep(sleep_s * attempt)
    raise RuntimeError(f"LLM extraction failed after retries: {last_err}")


def parse_json_content(content: str) -> Dict[str, Any]:
    text = content.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    try:
        return json.loads(text)
    except Exception:
        pass

    match = re.search(r"\{[\s\S]*\}", text)
    if match:
        return json.loads(match.group(0))
    raise ValueError(f"Unable to parse JSON from model output: {content[:300]}")


def clean_pair(pair: Dict[str, Any]) -> Dict[str, str]:
    cleaned = {
        "situation": str(pair.get("situation", "")).strip(),
        "strategies": str(pair.get("strategies", "")).strip(),
        "effect": str(pair.get("effect", "unclear")).strip().lower(),
    }
    if cleaned["effect"] not in VALID_EFFECTS:
        cleaned["effect"] = "unclear"
    return cleaned


def load_calls(path: str, start_index: int, max_calls: int) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i < start_index:
                continue
            if len(rows) >= max_calls:
                break
            rows.append(row)
    return rows


def call_id_sort_key(call_id: str) -> int:
    try:
        return int(call_id.split("_")[-1])
    except Exception:
        return 10**9


def process_one_call(idx: int, total: int, call: Dict[str, str]) -> Tuple[Dict[str, Any], List[Dict[str, Any]], str]:
    call_id = call.get("call_id", "").strip()
    transcript = call.get("transcript", "").strip()
    call_outcome = call.get("call_outcome", "").strip()
    success = 1 if call_outcome == "demo_booked" else 0

    extracted_pairs: List[Dict[str, str]] = []
    if transcript:
        try:
            extracted = call_llm(transcript)
            raw_pairs = extracted.get("pairs", [])
            if not isinstance(raw_pairs, list):
                raw_pairs = []
            extracted_pairs = [clean_pair(p) for p in raw_pairs if isinstance(p, dict)]
        except Exception as err:
            return (
                {
                    "call_id": call_id,
                    "call_outcome": call_outcome,
                    "success": success,
                    "pairs": [],
                },
                [],
                f"[WARN] [{idx}/{total}] {call_id} failed: {err}",
            )

    call_level = {
        "call_id": call_id,
        "call_outcome": call_outcome,
        "success": success,
        "pairs": extracted_pairs,
    }

    moments = [
        {
            "call_id": call_id,
            "call_outcome": call_outcome,
            "success": success,
            "situation": pair["situation"],
            "strategies": pair["strategies"],
            "effect": pair["effect"],
        }
        for pair in extracted_pairs
    ]
    log_line = f"[{idx}/{total}] processed {call_id}: {len(extracted_pairs)} moments"
    return call_level, moments, log_line


def main() -> None:
    if not API_KEY:
        raise RuntimeError("Missing VERCEL_AI_GATEWAY_API_KEY")

    calls = load_calls(INPUT_CSV, START_INDEX, MAX_CALLS)
    call_level_rows = []
    moment_rows = []

    total = len(calls)
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(process_one_call, idx, total, call)
            for idx, call in enumerate(calls, start=1)
        ]
        for future in concurrent.futures.as_completed(futures):
            call_level, moments, log_line = future.result()
            call_level_rows.append(call_level)
            moment_rows.extend(moments)
            print(log_line, flush=True)

    call_level_rows.sort(key=lambda x: call_id_sort_key(x["call_id"]))
    moment_rows.sort(key=lambda x: call_id_sort_key(x["call_id"]))

    with open(OUTPUT_CALL_LEVEL_JSONL, "w", encoding="utf-8") as f:
        for row in call_level_rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    fieldnames = [
        "call_id",
        "call_outcome",
        "success",
        "situation",
        "strategies",
        "effect",
    ]
    with open(OUTPUT_MOMENT_LEVEL_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(moment_rows)

    print(f"Saved call-level file: {OUTPUT_CALL_LEVEL_JSONL} ({len(call_level_rows)} calls)")
    print(f"Saved moment-level file: {OUTPUT_MOMENT_LEVEL_CSV} ({len(moment_rows)} moments)")


if __name__ == "__main__":
    main()
