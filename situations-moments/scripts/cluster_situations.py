import csv
import json
import math
import os
from collections import Counter, defaultdict
from typing import Dict, List, Tuple

import requests
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "situations-moments" / "data"
INPUT_CSV = str(DATA_DIR / "moment-level.csv")
OUTPUT_CLUSTER_SUMMARY_CSV = os.getenv(
    "OUTPUT_CLUSTER_SUMMARY_CSV", str(DATA_DIR / "situation-clusters.csv")
)
OUTPUT_SITUATION_ASSIGNMENTS_CSV = os.getenv(
    "OUTPUT_SITUATION_ASSIGNMENTS_CSV", str(DATA_DIR / "situation-cluster-assignments.csv")
)
FORCE_K = int(os.getenv("FORCE_K", "0"))

API_KEY = os.getenv(
    "VERCEL_AI_GATEWAY_API_KEY",
    "",
)
EMBEDDING_MODEL = "text-embedding-3-small"
NAMING_MODEL = "openai/gpt-5-chat"
EMBEDDING_URL = "https://ai-gateway.vercel.sh/v1/embeddings"
CHAT_URL = "https://ai-gateway.vercel.sh/v1/chat/completions"


def load_situations(path: str) -> Tuple[List[Dict[str, str]], List[str]]:
    rows: List[Dict[str, str]] = []
    situations: List[str] = []
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            s = (row.get("situation") or "").strip()
            if not s:
                continue
            rows.append(row)
            situations.append(s)
    return rows, situations


def embed_texts(texts: List[str], batch_size: int = 64) -> List[List[float]]:
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json",
    }
    vectors: List[List[float]] = []
    for i in range(0, len(texts), batch_size):
        batch = texts[i : i + batch_size]
        payload = {"model": EMBEDDING_MODEL, "input": batch}
        resp = requests.post(EMBEDDING_URL, headers=headers, json=payload, timeout=120)
        if resp.status_code >= 400:
            raise RuntimeError(f"Embedding failed: HTTP {resp.status_code} {resp.text[:400]}")
        data = resp.json()
        chunk = data.get("data", [])
        chunk = sorted(chunk, key=lambda x: x.get("index", 0))
        vectors.extend([x["embedding"] for x in chunk])
    return vectors


def choose_k(vectors: List[List[float]]) -> int:
    n = len(vectors)
    if n < 8:
        return max(2, n // 2)

    k_min = 6
    k_max = min(30, max(8, int(math.sqrt(n)) + 8))
    best_k = k_min
    best_score = -1.0

    for k in range(k_min, k_max + 1):
        model = KMeans(n_clusters=k, random_state=42, n_init=10)
        labels = model.fit_predict(vectors)
        if len(set(labels)) <= 1:
            continue
        score = silhouette_score(vectors, labels, metric="cosine")
        if score > best_score:
            best_score = score
            best_k = k
    return best_k


def name_cluster(examples: List[str]) -> str:
    prompt = """These situations were clustered together from sales call transcripts. Name the recurring situation in plain business language.

Return only a short phrase (3-8 words), no punctuation at the end.

Situations:
""" + "\n".join(f"- {x}" for x in examples[:12])
    payload = {
        "model": NAMING_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.2,
    }
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json",
    }
    resp = requests.post(CHAT_URL, headers=headers, json=payload, timeout=90)
    if resp.status_code >= 400:
        return "Unlabeled recurring situation"
    try:
        text = resp.json()["choices"][0]["message"]["content"].strip()
        return text.splitlines()[0].strip().strip("\"'")
    except Exception:
        return "Unlabeled recurring situation"


def main() -> None:
    if not API_KEY:
        raise RuntimeError("Missing VERCEL_AI_GATEWAY_API_KEY")

    rows, situations = load_situations(INPUT_CSV)
    if not situations:
        raise RuntimeError("No situations found in input file")

    vectors = embed_texts(situations)
    k = FORCE_K if FORCE_K > 1 else choose_k(vectors)
    model = KMeans(n_clusters=k, random_state=42, n_init=20)
    labels = model.fit_predict(vectors)

    cluster_to_situations: Dict[int, List[str]] = defaultdict(list)
    cluster_to_effects: Dict[int, List[str]] = defaultdict(list)
    for row, s, label in zip(rows, situations, labels):
        cluster_id = int(label)
        cluster_to_situations[cluster_id].append(s)
        cluster_to_effects[cluster_id].append((row.get("effect") or "").strip().lower())

    cluster_names: Dict[int, str] = {}
    for cluster_id, texts in cluster_to_situations.items():
        most_common = [t for t, _ in Counter(texts).most_common(12)]
        cluster_names[cluster_id] = name_cluster(most_common)

    # Summary table requested by user
    summary_rows = []
    for cluster_id, texts in cluster_to_situations.items():
        effects = cluster_to_effects.get(cluster_id, [])
        proceed_count = sum(1 for e in effects if e == "proceed")
        proceed_rate = (proceed_count / len(effects)) if effects else 0.0
        summary_rows.append(
            {
                "cluster_id": cluster_id,
                "situation_name": cluster_names.get(cluster_id, "Unlabeled recurring situation"),
                "count": len(texts),
                "proceed_count": proceed_count,
                "proceed_rate": f"{proceed_rate:.4f}",
            }
        )
    summary_rows.sort(key=lambda x: x["count"], reverse=True)

    with open(OUTPUT_CLUSTER_SUMMARY_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=["cluster_id", "situation_name", "count", "proceed_count", "proceed_rate"]
        )
        writer.writeheader()
        writer.writerows(summary_rows)

    # Optional detailed assignments for traceability
    assignment_rows = []
    for row, label in zip(rows, labels):
        assignment_rows.append(
            {
                "call_id": row.get("call_id", ""),
                "situation": row.get("situation", ""),
                "cluster_id": int(label),
                "situation_name": cluster_names.get(int(label), "Unlabeled recurring situation"),
            }
        )
    assignment_rows.sort(key=lambda x: (int(x["call_id"].split("_")[-1]), x["cluster_id"]))

    with open(OUTPUT_SITUATION_ASSIGNMENTS_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["call_id", "situation", "cluster_id", "situation_name"])
        writer.writeheader()
        writer.writerows(assignment_rows)

    print(f"Input moments: {len(situations)}")
    print(f"Chosen clusters (k): {k}")
    print(f"Saved summary: {OUTPUT_CLUSTER_SUMMARY_CSV} ({len(summary_rows)} clusters)")
    print(f"Saved assignments: {OUTPUT_SITUATION_ASSIGNMENTS_CSV} ({len(assignment_rows)} rows)")


if __name__ == "__main__":
    main()
