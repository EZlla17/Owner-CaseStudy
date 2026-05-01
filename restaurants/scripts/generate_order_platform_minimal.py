#!/usr/bin/env python3
import csv
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests

BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "restaurants" / "data"
INPUT_CSV = BASE_DIR / "restaurant.csv"
OUTPUT_CSV = DATA_DIR / "restaurant-order-platform-minimal.csv"
MAX_WORKERS = 8
TOP_N = 20

SEARCH_API_KEY = os.getenv("SEARCH_API_KEY", "").strip()

BRAVE_ENDPOINT = "https://api.search.brave.com/res/v1/web/search"

PLATFORM_PATTERNS: List[Tuple[str, str]] = [
    ("doordash", r"doordash"),
    ("ubereats", r"uber\s*eats|ubereats"),
    ("grubhub", r"grubhub"),
    ("postmates", r"postmates"),
    ("slice", r"\bslice\b"),
    ("chownow", r"chownow"),
    ("toasttab", r"toasttab|toast\s*order"),
    ("square", r"squareup|square\s*online"),
    ("clover", r"clover"),
    ("olo", r"\bolo\b"),
    ("popmenu", r"popmenu"),
    ("bentobox", r"bentobox"),
    ("owner", r"\bowner\.com\b|\bowner\b"),
]

PLATFORM_DOMAIN_HINTS: List[Tuple[str, str]] = [
    ("doordash", "doordash.com"),
    ("ubereats", "ubereats.com"),
    ("grubhub", "grubhub.com"),
    ("postmates", "postmates.com"),
    ("slice", "slicelife.com"),
    ("chownow", "chownow.com"),
    ("toasttab", "toasttab.com"),
    ("square", "square.site"),
    ("square", "squareup.com"),
    ("clover", "clover.com"),
    ("olo", "olo.com"),
    ("popmenu", "popmenu.com"),
    ("bentobox", "bentobox.com"),
]

ORDER_INTENT_PATTERNS = [
    r"\border\s*online\b",
    r"\border\s*now\b",
    r"\bstart\s*order\b",
    r"\bpick[\s-]?up\b",
    r"\bdelivery\b",
    r"\btake[\s-]?out\b",
    r"\bcarry[\s-]?out\b",
    r"\bmenu\b",
]


def normalize_domain(raw_url: str) -> str:
    value = (raw_url or "").strip()
    if not value or value.lower() == "nan":
        return ""
    if not value.startswith(("http://", "https://")):
        value = f"https://{value}"
    parsed = urlparse(value)
    host = (parsed.netloc or "").lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def looks_order_intent(text: str) -> bool:
    lowered = (text or "").lower()
    return any(re.search(pat, lowered) for pat in ORDER_INTENT_PATTERNS)


def detect_platform(text: str) -> Optional[str]:
    lowered = (text or "").lower()
    for platform, pattern in PLATFORM_PATTERNS:
        if re.search(pattern, lowered):
            return platform
    return None


def detect_platform_from_domain(domain: str) -> Optional[str]:
    host = (domain or "").lower()
    for platform, hint in PLATFORM_DOMAIN_HINTS:
        if host == hint or host.endswith("." + hint):
            return platform
    return None


def extract_organic_results(payload: Dict[str, Any]) -> List[Dict[str, str]]:
    # Brave format: payload["web"]["results"]
    web = payload.get("web", {})
    results = web.get("results", []) if isinstance(web, dict) else []
    if isinstance(results, list):
        out = []
        for item in results[:TOP_N]:
            out.append(
                {
                    "title": str(item.get("title", "") or ""),
                    "link": str(item.get("url", "") or ""),
                    "snippet": str(item.get("description", "") or ""),
                }
            )
        return out
    return []


def search_query(query: str) -> List[Dict[str, str]]:
    headers = {
        "X-Subscription-Token": SEARCH_API_KEY,
        "Accept": "application/json",
    }
    resp = requests.get(
        BRAVE_ENDPOINT,
        headers=headers,
        params={"q": query, "count": TOP_N},
        timeout=25,
    )
    resp.raise_for_status()
    return extract_organic_results(resp.json())


def analyze_one(row: Dict[str, str]) -> Dict[str, str]:
    restaurant_id = (row.get("restaurant_id") or "").strip()
    name = (row.get("name") or "").strip()
    website_url = (row.get("website_url") or "").strip()
    restaurant_domain = normalize_domain(website_url)
    query = f"{name} order"

    first_order_platform = "unknown"
    found_platforms: List[str] = []
    has_own = "unknown"

    try:
        own_match_seen = False
        seen_links = set()

        locked_by_own_first = False

        results = search_query(query)
        for result in results:
            title = result["title"]
            link = result["link"]
            snippet = result["snippet"]
            if not link or link in seen_links:
                continue
            seen_links.add(link)

            domain = normalize_domain(link)
            text_blob = f"{title} {link} {snippet}"

            is_own_domain = bool(
                restaurant_domain
                and domain
                and (domain == restaurant_domain or domain.endswith("." + restaurant_domain))
            )
            has_order_signal = looks_order_intent(text_blob) or any(
                token in link.lower() for token in ("/order", "order-online", "online-order", "ordering")
            )

            # Hard rule requested by user:
            # if first relevant result is own domain with ordering intent, first platform is restaurant_own.
            if not locked_by_own_first and first_order_platform == "unknown" and is_own_domain and has_order_signal:
                first_order_platform = "restaurant_own"
                locked_by_own_first = True
                found_platforms.append("restaurant_own")

            platform = detect_platform_from_domain(domain) or detect_platform(text_blob)
            if platform:
                found_platforms.append(platform)
                if not locked_by_own_first and first_order_platform == "unknown" and looks_order_intent(text_blob):
                    first_order_platform = platform
                elif not locked_by_own_first and first_order_platform == "unknown":
                    # fallback: domain-matched platform even if text intent weak
                    first_order_platform = platform

            # restaurant own domain + ordering intent
            if is_own_domain and has_order_signal:
                own_match_seen = True
                found_platforms.append("restaurant_own")

        unique_platforms = sorted(set(found_platforms))
        all_order_platforms = ",".join(unique_platforms)

        if own_match_seen:
            has_own = "yes"
        elif unique_platforms:
            has_own = "no"
        else:
            has_own = "unknown"

        return {
            "restaurant_id": restaurant_id,
            "first_order_platform": first_order_platform,
            "all_order_platforms": all_order_platforms,
            "has_restaurant_own_order_platform": has_own,
        }
    except Exception:
        return {
            "restaurant_id": restaurant_id,
            "first_order_platform": "unknown",
            "all_order_platforms": "",
            "has_restaurant_own_order_platform": "unknown",
        }


def main() -> None:
    if not SEARCH_API_KEY:
        raise RuntimeError("Missing SEARCH_API_KEY")

    with INPUT_CSV.open("r", newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    results: List[Optional[Dict[str, str]]] = [None] * len(rows)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(analyze_one, row): idx for idx, row in enumerate(rows)}
        for future in as_completed(futures):
            idx = futures[future]
            results[idx] = future.result()
            rid = results[idx]["restaurant_id"] if results[idx] else ""
            print(f"processed {idx + 1}/{len(rows)} {rid}", flush=True)

    fieldnames = [
        "restaurant_id",
        "first_order_platform",
        "all_order_platforms",
        "has_restaurant_own_order_platform",
    ]
    with OUTPUT_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in results:
            writer.writerow(row or {k: "" for k in fieldnames})

    print(f"Done. Wrote {OUTPUT_CSV} ({len(rows)} rows).")


if __name__ == "__main__":
    main()

