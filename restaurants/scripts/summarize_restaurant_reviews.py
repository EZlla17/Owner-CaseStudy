#!/usr/bin/env python3
import csv
import json
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "restaurants" / "data"
INPUT_CSV = BASE_DIR / "restaurant.csv"
OUTPUT_CSV = DATA_DIR / "restaurant-places-review-summary.csv"
MAX_WORKERS = 8

PLACES_API_KEY = os.getenv("PLACES_API_KEY", "").strip()
PLACES_NEW_BASE = "https://places.googleapis.com/v1"

LLM_API_KEY = os.getenv("VERCEL_AI_GATEWAY_API_KEY", "").strip()
LLM_API_URL = "https://ai-gateway.vercel.sh/v1/chat/completions"
LLM_MODEL = os.getenv("MODEL", "openai/gpt-5-chat")

FOOD_ITEM_TERMS = {
    "chicken",
    "beef",
    "pork",
    "fish",
    "shrimp",
    "taco",
    "tacos",
    "burrito",
    "burritos",
    "pizza",
    "noodle",
    "noodles",
    "soup",
    "burger",
    "fries",
    "sauce",
    "rice",
}

NEG_QUALITY_TERMS = {
    "old",
    "chewy",
    "cold",
    "salty",
    "bland",
    "raw",
    "undercooked",
    "overcooked",
    "greasy",
    "burnt",
    "stale",
    "dry",
    "soggy",
    "tough",
}

POS_QUALITY_TERMS = {
    "great",
    "good",
    "fresh",
    "tasty",
    "delicious",
    "flavorful",
    "excellent",
}


def find_place_id(name: str, city: str, state: str) -> Optional[str]:
    query = ", ".join(x for x in [name, city, state] if x)
    headers = {
        "X-Goog-Api-Key": PLACES_API_KEY,
        "X-Goog-FieldMask": "places.id,places.displayName",
        "Content-Type": "application/json",
    }
    payload = {"textQuery": query, "maxResultCount": 1}
    resp = requests.post(f"{PLACES_NEW_BASE}/places:searchText", headers=headers, json=payload, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    places = data.get("places", [])
    if not places:
        return None
    return places[0].get("id")


def get_place_details(place_id: str) -> Dict[str, Any]:
    headers = {
        "X-Goog-Api-Key": PLACES_API_KEY,
        "X-Goog-FieldMask": "id,displayName,rating,userRatingCount,reviews,googleMapsUri,websiteUri",
    }
    resp = requests.get(f"{PLACES_NEW_BASE}/places/{place_id}", headers=headers, timeout=20)
    resp.raise_for_status()
    return resp.json() or {}


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
    raise ValueError("Unable to parse JSON from LLM output")


def _normalize_owner_points(raw: Any, *, max_items: int = 5) -> str:
    """Flatten LLM output (JSON array or legacy string) to a stable cell value."""
    items: List[str] = []
    if raw is None:
        return ""
    if isinstance(raw, list):
        items = [str(x).strip() for x in raw if str(x).strip()]
    elif isinstance(raw, str):
        stripped = raw.strip()
        if not stripped:
            return ""
        items = [p.strip() for p in re.split(r"\s*[|;]\s*|\s*,\s*", stripped) if p.strip()]
    else:
        s = str(raw).strip()
        if s:
            items = [s]
    def _abstract_point(item: str) -> str:
        lower = item.lower()
        has_food_item = any(term in lower for term in FOOD_ITEM_TERMS)
        has_negative_food_detail = any(term in lower for term in NEG_QUALITY_TERMS)
        has_positive_food_detail = any(term in lower for term in POS_QUALITY_TERMS)

        # Hard guardrail: collapse dish-level comments into business-level themes.
        if has_food_item and has_negative_food_detail:
            return "food quality consistency issues"
        if has_food_item and has_positive_food_detail:
            return "strong food quality reputation"
        return item

    normalized_items = [_abstract_point(item) for item in items]

    deduped: List[str] = []
    seen = set()
    for item in normalized_items:
        key = item.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return ", ".join(deduped[:max_items])


def summarize_themes_with_llm(
    reviews: List[str],
    *,
    rating: str = "",
    review_count: str = "",
) -> Dict[str, str]:
    if not reviews:
        return {
            "positive_reviews_points": "",
            "negative_reviews_points": "",
            "llm_status": "no_reviews",
        }
    if not LLM_API_KEY:
        return {
            "positive_reviews_points": "",
            "negative_reviews_points": "",
            "llm_status": "missing_llm_api_key",
        }

    stats_line = ""
    if rating or review_count:
        stats_line = f"Google aggregate rating: {rating or 'unknown'} stars; review count: {review_count or 'unknown'}.\n\n"

    prompt = (
        "You analyze restaurant reviews for Owner sales context.\n\n"
        "Owner helps independent restaurants get online traffic, grow direct online orders, "
        "reduce third-party dependence, increase repeat customers, and grow profitably.\n\n"
        "Task:\n"
        "- Extract 1-5 high-level positive points Owner can amplify.\n"
        "- Extract 1-5 high-level negative points Owner can help fix.\n\n"
        "Important abstraction rule:\n"
        "- Do NOT include dish-specific or one-off details (for example: one old chicken item, one salty sauce, one cold fries incident).\n"
        "- Cluster specific comments into broader business themes.\n"
        "- Prefer business-relevant themes over anecdotes.\n\n"
        "Rewrite examples:\n"
        "- 'old chewy chicken' -> 'food quality consistency issues'\n"
        "- 'great tacos' -> 'strong food quality reputation'\n\n"
        "Use these theme buckets when possible:\n"
        "- Ordering experience\n"
        "- Pickup speed reliability\n"
        "- Delivery reliability\n"
        "- Service quality\n"
        "- Menu clarity accuracy\n"
        "- Value pricing perception\n"
        "- Consistency repeat experience\n"
        "- Customer trust brand experience\n\n"
        "Output style rules:\n"
        "- Each point short and simple: 2-6 words.\n"
        "- No full sentences.\n"
        "- No commas inside one point.\n"
        "- Grounded in review evidence only.\n"
        "- Arrays length 1-5 or [] if unsupported.\n\n"
        + stats_line
        + "Return strict JSON only:\n"
        "{\n"
        '  "positive_reviews_points": [],\n'
        '  "negative_reviews_points": []\n'
        "}\n\n"
        "Reviews:\n- "
        + "\n- ".join(reviews[:8])
    )

    payload = {
        "model": LLM_MODEL,
        "messages": [
            {"role": "system", "content": "Return strict JSON only. No markdown."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.1,
    }
    headers = {"Authorization": f"Bearer {LLM_API_KEY}", "Content-Type": "application/json"}
    resp = requests.post(LLM_API_URL, headers=headers, json=payload, timeout=45)
    resp.raise_for_status()
    content = resp.json()["choices"][0]["message"]["content"]
    parsed = parse_json_content(content)
    return {
        "positive_reviews_points": _normalize_owner_points(parsed.get("positive_reviews_points")),
        "negative_reviews_points": _normalize_owner_points(parsed.get("negative_reviews_points")),
        "llm_status": "ok",
    }


def process_row(row: Dict[str, str]) -> Dict[str, str]:
    out: Dict[str, str] = {
        "restaurant_id": row.get("restaurant_id", ""),
        "name": row.get("name", ""),
        "rating": "",
        "review_count": "",
        "positive_reviews_points": "",
        "negative_reviews_points": "",
    }

    try:
        place_id = find_place_id(
            row.get("name", ""),
            row.get("city", ""),
            row.get("state", ""),
        )
        if not place_id:
            return out
        details = get_place_details(place_id)

        if details.get("rating") is not None:
            out["rating"] = str(details.get("rating"))
        if details.get("userRatingCount") is not None:
            out["review_count"] = str(details.get("userRatingCount"))

        reviews = details.get("reviews", []) or []
        review_texts = []
        for review in reviews:
            text_obj = review.get("text", {}) if isinstance(review, dict) else {}
            text_val = ""
            if isinstance(text_obj, dict):
                text_val = str(text_obj.get("text", "") or text_obj.get("originalText", "")).strip()
            elif isinstance(text_obj, str):
                text_val = text_obj.strip()
            if text_val:
                review_texts.append(text_val)

        llm_result = summarize_themes_with_llm(
            review_texts,
            rating=out["rating"],
            review_count=out["review_count"],
        )
        out["positive_reviews_points"] = llm_result["positive_reviews_points"]
        out["negative_reviews_points"] = llm_result["negative_reviews_points"]
        return out
    except Exception:
        return out


def main() -> None:
    if not PLACES_API_KEY:
        raise RuntimeError("Missing PLACES_API_KEY")

    with INPUT_CSV.open("r", newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    results: List[Optional[Dict[str, str]]] = [None] * len(rows)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(process_row, row): idx for idx, row in enumerate(rows)}
        for future in as_completed(futures):
            idx = futures[future]
            results[idx] = future.result()
            rid = results[idx].get("restaurant_id", "")
            print(f"processed {idx + 1}/{len(rows)} {rid}", flush=True)

    fieldnames = [
        "restaurant_id",
        "name",
        "rating",
        "review_count",
        "positive_reviews_points",
        "negative_reviews_points",
    ]
    with OUTPUT_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in results:
            writer.writerow(row or {k: "" for k in fieldnames})

    print(f"Done. Wrote {OUTPUT_CSV} ({len(rows)} rows).")


if __name__ == "__main__":
    main()

