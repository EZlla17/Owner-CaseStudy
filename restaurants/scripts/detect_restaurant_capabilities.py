#!/usr/bin/env python3
import csv
import re
import socket
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from html import unescape
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse
import requests


BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "restaurants" / "data"
INPUT_CSV = BASE_DIR / "restaurant.csv"
OUTPUT_CSV = DATA_DIR / "restaurant-capabilities.csv"
MAX_WORKERS = 12
TIMEOUT_SECONDS = 15


ONLINE_PATTERNS = [
    r"\border\s*online\b",
    r"\bonline\s*ordering\b",
    r"\bstart\s*order\b",
    r"\border\s*now\b",
    r"\bdoordash\b",
    r"\bubereats?\b",
    r"\bgrubhub\b",
    r"\bchownow\b",
    r"\btoasttab\b",
    r"\bslice\b",
    r"\bdelivery\b",
]

PICKUP_PATTERNS = [
    r"\bpick[\s-]?up\b",
    r"\bcurbside\b",
    r"\bcarry[\s-]?out\b",
    r"\btake[\s-]?out\b",
    r"\bto[\s-]?go\b",
]

CATERING_PATTERNS = [
    r"\bcatering\b",
    r"\bcater\b",
    r"\bparty\s*trays?\b",
    r"\bevent\s*catering\b",
]


@dataclass
class DetectResult:
    status: str
    checked_url: str
    has_online_ordering: str
    online_ordering_evidence: str
    has_pickup: str
    pickup_evidence: str
    has_catering: str
    catering_evidence: str


def normalize_url(raw_url: str) -> Optional[str]:
    value = (raw_url or "").strip()
    if not value or value.lower() == "nan":
        return None
    if not value.startswith(("http://", "https://")):
        value = f"https://{value}"
    parsed = urlparse(value)
    if not parsed.netloc:
        return None
    return value


def fetch_html(url: str) -> str:
    response = requests.get(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            )
        },
        timeout=TIMEOUT_SECONDS,
        allow_redirects=True,
    )
    response.raise_for_status()
    content_type = response.headers.get("Content-Type", "").lower()
    if "text/html" not in content_type and "application/xhtml+xml" not in content_type:
        return ""
    return response.text[:1_500_000]


def html_to_text(html: str) -> str:
    if not html:
        return ""
    no_script = re.sub(r"(?is)<script.*?>.*?</script>", " ", html)
    no_style = re.sub(r"(?is)<style.*?>.*?</style>", " ", no_script)
    no_tags = re.sub(r"(?s)<[^>]+>", " ", no_style)
    text = unescape(no_tags)
    return re.sub(r"\s+", " ", text).strip()


def find_pattern(patterns: list[str], text: str) -> tuple[str, str]:
    lowered = text.lower()
    for pattern in patterns:
        match = re.search(pattern, lowered, flags=re.IGNORECASE)
        if match:
            start, end = match.span()
            left = max(0, start - 45)
            right = min(len(text), end + 45)
            snippet = text[left:right].strip()
            return "yes", snippet
    return "no", ""


def detect_from_url(url: Optional[str]) -> DetectResult:
    if not url:
        return DetectResult(
            status="invalid_url",
            checked_url="",
            has_online_ordering="unknown",
            online_ordering_evidence="",
            has_pickup="unknown",
            pickup_evidence="",
            has_catering="unknown",
            catering_evidence="",
        )
    try:
        html = fetch_html(url)
        if not html:
            return DetectResult(
                status="non_html_or_empty",
                checked_url=url,
                has_online_ordering="unknown",
                online_ordering_evidence="",
                has_pickup="unknown",
                pickup_evidence="",
                has_catering="unknown",
                catering_evidence="",
            )
        text = html_to_text(html)
        online_flag, online_evidence = find_pattern(ONLINE_PATTERNS, text)
        pickup_flag, pickup_evidence = find_pattern(PICKUP_PATTERNS, text)
        catering_flag, catering_evidence = find_pattern(CATERING_PATTERNS, text)
        return DetectResult(
            status="ok",
            checked_url=url,
            has_online_ordering=online_flag,
            online_ordering_evidence=online_evidence,
            has_pickup=pickup_flag,
            pickup_evidence=pickup_evidence,
            has_catering=catering_flag,
            catering_evidence=catering_evidence,
        )
    except requests.HTTPError as e:
        return DetectResult(
            status=f"http_error_{e.response.status_code if e.response else 'unknown'}",
            checked_url=url,
            has_online_ordering="unknown",
            online_ordering_evidence="",
            has_pickup="unknown",
            pickup_evidence="",
            has_catering="unknown",
            catering_evidence="",
        )
    except (requests.RequestException, TimeoutError, socket.timeout):
        return DetectResult(
            status="network_error_or_timeout",
            checked_url=url,
            has_online_ordering="unknown",
            online_ordering_evidence="",
            has_pickup="unknown",
            pickup_evidence="",
            has_catering="unknown",
            catering_evidence="",
        )
    except Exception:
        return DetectResult(
            status="parse_or_unknown_error",
            checked_url=url,
            has_online_ordering="unknown",
            online_ordering_evidence="",
            has_pickup="unknown",
            pickup_evidence="",
            has_catering="unknown",
            catering_evidence="",
        )


def main() -> None:
    with INPUT_CSV.open("r", newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    indexed_rows = [(idx, row) for idx, row in enumerate(rows)]
    results: dict[int, DetectResult] = {}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        future_map = {
            pool.submit(detect_from_url, normalize_url(row.get("website_url", ""))): idx
            for idx, row in indexed_rows
        }
        for future in as_completed(future_map):
            idx = future_map[future]
            results[idx] = future.result()

    output_fields = list(rows[0].keys()) + [
        "checked_url",
        "capability_status",
        "has_online_ordering",
        "online_ordering_evidence",
        "has_pickup",
        "pickup_evidence",
        "has_catering",
        "catering_evidence",
    ]

    with OUTPUT_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=output_fields)
        writer.writeheader()
        for idx, row in indexed_rows:
            res = results[idx]
            out = dict(row)
            out["checked_url"] = res.checked_url
            out["capability_status"] = res.status
            out["has_online_ordering"] = res.has_online_ordering
            out["online_ordering_evidence"] = res.online_ordering_evidence
            out["has_pickup"] = res.has_pickup
            out["pickup_evidence"] = res.pickup_evidence
            out["has_catering"] = res.has_catering
            out["catering_evidence"] = res.catering_evidence
            writer.writerow(out)

    total = len(rows)
    ok = sum(1 for r in results.values() if r.status == "ok")
    print(f"Done. Wrote {OUTPUT_CSV} ({total} rows). OK fetches: {ok}/{total}")


if __name__ == "__main__":
    main()

