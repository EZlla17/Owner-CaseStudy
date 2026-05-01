#!/usr/bin/env python3
import csv
from pathlib import Path
from typing import Dict, List, Optional

BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "restaurants" / "data"
RATING_CSV = DATA_DIR / "restaurant-places-review-summary.csv"
CAPABILITY_CSV = DATA_DIR / "restaurant-capabilities-summary.csv"
PLATFORM_CSV = DATA_DIR / "restaurant-order-platform-minimal.csv"
RESTAURANT_CSV = BASE_DIR / "restaurant.csv"
OUTPUT_CSV = DATA_DIR / "restaurant-fit-ranking.csv"

THIRD_PARTY_PLATFORMS = {"doordash", "ubereats", "grubhub", "postmates"}


def to_bool(value: str) -> bool:
    return (value or "").strip().lower() == "yes"


def to_float_or_none(value: str) -> Optional[float]:
    try:
        num = float((value or "").strip())
        return num
    except Exception:
        return None


def parse_platforms(raw: str) -> List[str]:
    return [p.strip().lower() for p in (raw or "").split(",") if p.strip()]


def infer_ordering_setup(all_platforms: List[str], has_self_owned_ordering: bool) -> str:
    if not all_platforms:
        return "no_online_ordering"
    third_party_count = len([p for p in all_platforms if p in THIRD_PARTY_PLATFORMS])
    if not has_self_owned_ordering and third_party_count > 0:
        return "third_party_only"
    if has_self_owned_ordering and third_party_count == 0:
        return "clear_direct"
    if has_self_owned_ordering and third_party_count > 0:
        return "direct_but_poor"
    return "direct_but_poor"


def infer_direct_ordering_quality(has_self_owned_ordering: bool, third_party_count: int) -> str:
    if not has_self_owned_ordering:
        return "none"
    if third_party_count == 0:
        return "clear_branded_direct"
    return "external_branding"


def calculate_owner_fit_score(
    rating: float,
    has_pickup: bool,
    has_delivery: bool,
    has_catering: bool,
    has_self_owned_ordering: bool,
    direct_ordering_quality: str,
    third_party_count: int,
    ordering_setup: str,
):
    if rating >= 4.6:
        rating_score = 25
    elif rating >= 4.3:
        rating_score = 20
    elif rating >= 4.0:
        rating_score = 15
    elif rating >= 3.7:
        rating_score = 8
    else:
        rating_score = 3

    if ordering_setup == "no_online_ordering":
        ordering_score = 25
    elif ordering_setup == "third_party_only":
        ordering_score = 30
    elif ordering_setup == "direct_but_poor":
        ordering_score = 18
    elif ordering_setup == "clear_direct":
        ordering_score = 5
    elif ordering_setup == "strong_direct_app_loyalty":
        ordering_score = 0
    else:
        ordering_score = 10

    if third_party_count >= 3:
        third_party_score = 20
    elif third_party_count == 2:
        third_party_score = 13
    elif third_party_count == 1:
        third_party_score = 8
    else:
        third_party_score = 3

    if has_pickup and has_delivery and has_catering:
        fulfillment_score = 15
    elif has_pickup and has_delivery:
        fulfillment_score = 12
    elif has_pickup:
        fulfillment_score = 8
    elif has_delivery:
        fulfillment_score = 6
    else:
        fulfillment_score = 2

    if not has_self_owned_ordering:
        ownership_score = 10
    elif direct_ordering_quality == "external_branding":
        ownership_score = 6
    elif direct_ordering_quality == "clear_branded_direct":
        ownership_score = 2
    elif direct_ordering_quality == "strong_direct_app_loyalty":
        ownership_score = 0
    else:
        ownership_score = 5

    total_score = (
        rating_score
        + ordering_score
        + third_party_score
        + fulfillment_score
        + ownership_score
    )

    if total_score >= 80:
        label = "Very Strong Owner Fit"
    elif total_score >= 60:
        label = "Strong Owner Fit"
    elif total_score >= 40:
        label = "Medium Owner Fit"
    elif total_score >= 20:
        label = "Weak Owner Fit"
    else:
        label = "Poor Owner Fit"

    return {
        "rating_score": rating_score,
        "ordering_score": ordering_score,
        "third_party_score": third_party_score,
        "fulfillment_score": fulfillment_score,
        "ownership_score": ownership_score,
        "total_score": total_score,
        "label": label,
    }


def main() -> None:
    with RESTAURANT_CSV.open("r", newline="", encoding="utf-8") as f:
        restaurant_rows = list(csv.DictReader(f))

    rating_map: Dict[str, float] = {}
    with RATING_CSV.open("r", newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            rid = (row.get("restaurant_id") or "").strip()
            rating = to_float_or_none(row.get("rating", ""))
            if rid:
                rating_map[rid] = rating if rating is not None else 0.0

    capability_map: Dict[str, Dict[str, bool]] = {}
    with CAPABILITY_CSV.open("r", newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            rid = (row.get("restaurant_id") or "").strip()
            if not rid:
                continue
            capability_map[rid] = {
                "has_pickup": to_bool(row.get("has_pickup", "")),
                "has_catering": to_bool(row.get("has_catering", "")),
            }

    platform_map: Dict[str, Dict[str, str]] = {}
    with PLATFORM_CSV.open("r", newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            rid = (row.get("restaurant_id") or "").strip()
            if not rid:
                continue
            platform_map[rid] = {
                "all_order_platforms": row.get("all_order_platforms", "") or "",
                "has_own": row.get("has_restaurant_own_order_platform", "") or "",
            }

    scored_rows = []
    for row in restaurant_rows:
        rid = (row.get("restaurant_id") or "").strip()
        name = (row.get("name") or "").strip()

        rating = rating_map.get(rid, 0.0)
        has_pickup = capability_map.get(rid, {}).get("has_pickup", False)
        has_catering = capability_map.get(rid, {}).get("has_catering", False)

        all_platforms = parse_platforms(platform_map.get(rid, {}).get("all_order_platforms", ""))
        has_self_owned_ordering = (
            platform_map.get(rid, {}).get("has_own", "").strip().lower() == "yes"
        )
        third_party_count = len({p for p in all_platforms if p in THIRD_PARTY_PLATFORMS})
        has_delivery = third_party_count > 0

        ordering_setup = infer_ordering_setup(all_platforms, has_self_owned_ordering)
        direct_ordering_quality = infer_direct_ordering_quality(
            has_self_owned_ordering, third_party_count
        )

        score = calculate_owner_fit_score(
            rating=rating,
            has_pickup=has_pickup,
            has_delivery=has_delivery,
            has_catering=has_catering,
            has_self_owned_ordering=has_self_owned_ordering,
            direct_ordering_quality=direct_ordering_quality,
            third_party_count=third_party_count,
            ordering_setup=ordering_setup,
        )

        scored_rows.append(
            {
                "restaurant_id": rid,
                "name": name,
                "rating": f"{rating:.1f}",
                "ordering_setup": ordering_setup,
                "third_party_count": str(third_party_count),
                "all_order_platforms": ",".join(sorted(set(all_platforms))),
                "has_self_owned_ordering": "yes" if has_self_owned_ordering else "no",
                **{k: str(v) for k, v in score.items()},
            }
        )

    scored_rows.sort(
        key=lambda r: (
            -int(r["total_score"]),
            -int(r["ordering_score"]),
            -int(r["third_party_score"]),
            -float(r["rating"]),
            r["restaurant_id"],
        )
    )
    for idx, row in enumerate(scored_rows, start=1):
        row["rank"] = str(idx)

    fieldnames = [
        "rank",
        "restaurant_id",
        "name",
        "total_score",
        "label",
        "rating_score",
        "ordering_score",
        "third_party_score",
        "fulfillment_score",
        "ownership_score",
        "rating",
        "ordering_setup",
        "third_party_count",
        "has_self_owned_ordering",
        "all_order_platforms",
    ]
    with OUTPUT_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(scored_rows)

    print(f"Done. Wrote {OUTPUT_CSV} ({len(scored_rows)} rows).")


if __name__ == "__main__":
    main()

