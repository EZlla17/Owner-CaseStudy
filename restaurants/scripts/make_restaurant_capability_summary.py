#!/usr/bin/env python3
import csv
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "restaurants" / "data"
INPUT_CSV = DATA_DIR / "restaurant-capabilities.csv"
OUTPUT_CSV = DATA_DIR / "restaurant-capabilities-summary.csv"

KEEP_COLUMNS = [
    "restaurant_id",
    "name",
    "website_url",
    "has_online_ordering",
    "has_pickup",
    "has_catering",
]


def main() -> None:
    with INPUT_CSV.open("r", newline="", encoding="utf-8") as f_in:
        reader = csv.DictReader(f_in)
        rows = list(reader)

    with OUTPUT_CSV.open("w", newline="", encoding="utf-8") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=KEEP_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in KEEP_COLUMNS})

    print(f"Wrote {OUTPUT_CSV} with {len(rows)} rows.")


if __name__ == "__main__":
    main()

