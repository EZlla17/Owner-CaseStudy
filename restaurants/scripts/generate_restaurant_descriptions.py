#!/usr/bin/env python3
import csv
import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List

import requests

BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "restaurants" / "data"
INPUT_CSV = BASE_DIR / "restaurant.csv"
OUTPUT_CSV = DATA_DIR / "restaurant-descriptions.csv"
MAX_WORKERS = 10

API_KEY = os.getenv(
    "VERCEL_AI_GATEWAY_API_KEY",
    "",
)
API_URL = "https://ai-gateway.vercel.sh/v1/chat/completions"
MODEL = os.getenv("MODEL", "openai/gpt-5-chat")

SYSTEM_PROMPT = "You are a concise restaurant profile writer. Return strict JSON only."


def parse_json_content(content: str) -> Dict[str, Any]:
    text = (content or "").strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    try:
        return json.loads(text)
    except Exception:
        match = re.search(r"\{[\s\S]*\}", text)
        if match:
            return json.loads(match.group(0))
    raise ValueError("Unable to parse JSON content")


def clean_to_50_words(text: str) -> str:
    words = text.strip().split()
    if len(words) <= 50:
        return " ".join(words)
    return " ".join(words[:50]).strip()


def call_llm(name: str, website_url: str, retries: int = 3) -> str:
    prompt = (
        "Write a short restaurant description in no more than 50 words.\n"
        "Use only this information:\n"
        f"- Restaurant name: {name}\n"
        f"- Restaurant link: {website_url}\n\n"
        "Rules:\n"
        "- Keep it plain, simple, and sales-friendly.\n"
        "- Do not invent specific facts that cannot be inferred.\n"
        "- If information is limited, write a generic but useful description.\n\n"
        "Return strict JSON:\n"
        '{ "description": "..." }'
    )

    payload = {
        "model": MODEL,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.2,
    }
    headers = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}

    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            resp = requests.post(API_URL, headers=headers, json=payload, timeout=45)
            if resp.status_code >= 400:
                raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:300]}")
            content = resp.json()["choices"][0]["message"]["content"]
            parsed = parse_json_content(content)
            return clean_to_50_words(str(parsed.get("description", "")).strip())
        except Exception as err:
            last_err = err
            if attempt < retries:
                time.sleep(attempt)
    raise RuntimeError(f"LLM generation failed: {last_err}")


def process_row(row: Dict[str, str]) -> Dict[str, str]:
    restaurant_id = (row.get("restaurant_id") or "").strip()
    name = (row.get("name") or "").strip()
    website_url = (row.get("website_url") or "").strip()
    try:
        description = call_llm(name=name, website_url=website_url)
    except Exception:
        description = ""
    return {
        "restaurant_id": restaurant_id,
        "name": name,
        "website_url": website_url,
        "description_50_words": description,
    }


def main() -> None:
    if not API_KEY:
        raise RuntimeError("Missing VERCEL_AI_GATEWAY_API_KEY")

    with INPUT_CSV.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))

    results: List[Dict[str, str] | None] = [None] * len(rows)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        future_map = {pool.submit(process_row, row): idx for idx, row in enumerate(rows)}
        for future in as_completed(future_map):
            idx = future_map[future]
            results[idx] = future.result()
            rid = results[idx]["restaurant_id"] if results[idx] else ""
            print(f"processed {idx + 1}/{len(rows)} {rid}", flush=True)

    fieldnames = ["restaurant_id", "name", "website_url", "description_50_words"]
    with OUTPUT_CSV.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in results:
            writer.writerow(row or {k: "" for k in fieldnames})

    print(f"Done. Wrote {OUTPUT_CSV} ({len(rows)} rows).")


if __name__ == "__main__":
    main()

