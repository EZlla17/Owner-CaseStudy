import csv
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
MOMENT_FILE = str(DATA_DIR / "moment-level.csv")
SITUATION_ASSIGN_FILE = str(DATA_DIR / "situation-cluster-assignments.csv")
OUTPUT_SUMMARY_FILE = str(DATA_DIR / "strategy-clusters-by-situation.csv")
OUTPUT_ASSIGNMENTS_FILE = str(DATA_DIR / "strategy-cluster-assignments-by-situation.csv")

API_KEY = os.getenv(
    "VERCEL_AI_GATEWAY_API_KEY",
    "",
)
EMBEDDING_MODEL = "text-embedding-3-small"
NAMING_MODEL = "openai/gpt-5-chat"
EMBEDDING_URL = "https://ai-gateway.vercel.sh/v1/embeddings"
CHAT_URL = "https://ai-gateway.vercel.sh/v1/chat/completions"
MIN_CLUSTER_SIZE = 6


def load_joined_rows() -> List[Dict[str, str]]:
    by_key: Dict[Tuple[str, str], Dict[str, str]] = {}
    with open(MOMENT_FILE, "r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            key = (row.get("call_id", ""), row.get("situation", ""))
            by_key[key] = row

    joined: List[Dict[str, str]] = []
    with open(SITUATION_ASSIGN_FILE, "r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            key = (row.get("call_id", ""), row.get("situation", ""))
            m = by_key.get(key)
            if not m:
                continue
            strategy = (m.get("strategies") or "").strip()
            if not strategy:
                continue
            joined.append(
                {
                    "call_id": row.get("call_id", ""),
                    "situation": row.get("situation", ""),
                    "situation_cluster_id": row.get("cluster_id", ""),
                    "situation_name": row.get("situation_name", ""),
                    "strategies": strategy,
                    "effect": (m.get("effect") or "").strip().lower(),
                }
            )
    return joined


def embed_texts(texts: List[str], batch_size: int = 64) -> List[List[float]]:
    headers = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}
    vectors: List[List[float]] = []
    for i in range(0, len(texts), batch_size):
        batch = texts[i : i + batch_size]
        payload = {"model": EMBEDDING_MODEL, "input": batch}
        resp = requests.post(EMBEDDING_URL, headers=headers, json=payload, timeout=120)
        if resp.status_code >= 400:
            raise RuntimeError(f"Embedding failed: HTTP {resp.status_code} {resp.text[:400]}")
        data = resp.json()
        chunk = sorted(data.get("data", []), key=lambda x: x.get("index", 0))
        vectors.extend([x["embedding"] for x in chunk])
    return vectors


def choose_k(vectors: List[List[float]]) -> int:
    n = len(vectors)
    if n <= 3:
        return 1
    if n < 8:
        return 2

    k_min = 2
    k_max = min(8, max(3, int(math.sqrt(n)) + 1))
    best_k = 2
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


def name_strategy_cluster(situation_name: str, examples: List[str]) -> str:
    prompt = (
        "These snippets are rep responses to a prospect-triggered situation from sales calls.\n"
        "Your task is to summarize how the rep responds to and tackles this situation.\n\n"
        f"Situation context:\n{situation_name}\n\n"
        "Rep response snippets:\n"
        + "\n".join(f"- {x}" for x in examples[:12])
        + "\n\n"
        "Rules:\n"
        "- Return only one name.\n"
        "- Describe what the rep is doing, not what the prospect said.\n"
        "- Make the name useful for sales coaching.\n"
        "- Avoid vague labels such as Good response, Follow-up, Sales pitch, or Objection handling.\n"
        "- Do not include explanations, punctuation, quotes, or numbering.\n"
    )
    payload = {"model": NAMING_MODEL, "messages": [{"role": "user", "content": prompt}], "temperature": 0.2}
    headers = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}
    resp = requests.post(CHAT_URL, headers=headers, json=payload, timeout=90)
    if resp.status_code >= 400:
        return "Unlabeled strategy pattern"
    try:
        text = resp.json()["choices"][0]["message"]["content"].strip()
        return text.splitlines()[0].strip().strip("\"'")
    except Exception:
        return "Unlabeled strategy pattern"


def main() -> None:
    rows = load_joined_rows()
    if not rows:
        raise RuntimeError("No joined rows found")

    # group by situation cluster
    groups: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for r in rows:
        groups[r["situation_cluster_id"]].append(r)

    summary_rows: List[Dict[str, str]] = []
    assignment_rows: List[Dict[str, str]] = []

    for situation_cluster_id, grp in groups.items():
        situation_name = grp[0]["situation_name"]
        strategies = [r["strategies"] for r in grp]
        vectors = embed_texts(strategies)

        if len(grp) < MIN_CLUSTER_SIZE:
            labels = [0] * len(grp)
            k = 1
        else:
            k = choose_k(vectors)
            if k <= 1:
                labels = [0] * len(grp)
                k = 1
            else:
                labels = list(KMeans(n_clusters=k, random_state=42, n_init=20).fit_predict(vectors))

        cluster_to_items: Dict[int, List[Dict[str, str]]] = defaultdict(list)
        for item, label in zip(grp, labels):
            cluster_to_items[int(label)].append(item)

        strategy_names: Dict[int, str] = {}
        for local_id, items in cluster_to_items.items():
            examples = [x["strategies"] for x in items]
            most_common = [t for t, _ in Counter(examples).most_common(12)]
            strategy_names[local_id] = name_strategy_cluster(situation_name, most_common)

        for local_id, items in cluster_to_items.items():
            proceed_count = sum(1 for x in items if x["effect"] == "proceed")
            total = len(items)
            proceed_rate = proceed_count / total if total else 0.0
            summary_rows.append(
                {
                    "situation_cluster_id": situation_cluster_id,
                    "situation_name": situation_name,
                    "strategy_cluster_id": local_id,
                    "strategy_name": strategy_names.get(local_id, "Unlabeled strategy pattern"),
                    "count": total,
                    "proceed_count": proceed_count,
                    "proceed_rate": f"{proceed_rate:.4f}",
                }
            )
            for item in items:
                assignment_rows.append(
                    {
                        "call_id": item["call_id"],
                        "situation": item["situation"],
                        "strategies": item["strategies"],
                        "effect": item["effect"],
                        "situation_cluster_id": situation_cluster_id,
                        "situation_name": situation_name,
                        "strategy_cluster_id": local_id,
                        "strategy_name": strategy_names.get(local_id, "Unlabeled strategy pattern"),
                    }
                )

    summary_rows.sort(key=lambda x: (int(x["situation_cluster_id"]), -int(x["count"])))
    assignment_rows.sort(
        key=lambda x: (
            int(x["situation_cluster_id"]),
            int(x["call_id"].split("_")[-1]) if x["call_id"].split("_")[-1].isdigit() else 10**9,
            int(x["strategy_cluster_id"]),
        )
    )

    with open(OUTPUT_SUMMARY_FILE, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "situation_cluster_id",
                "situation_name",
                "strategy_cluster_id",
                "strategy_name",
                "count",
                "proceed_count",
                "proceed_rate",
            ],
        )
        w.writeheader()
        w.writerows(summary_rows)

    with open(OUTPUT_ASSIGNMENTS_FILE, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "call_id",
                "situation",
                "strategies",
                "effect",
                "situation_cluster_id",
                "situation_name",
                "strategy_cluster_id",
                "strategy_name",
            ],
        )
        w.writeheader()
        w.writerows(assignment_rows)

    print(f"Input joined rows: {len(rows)}")
    print(f"Situation clusters processed: {len(groups)}")
    print(f"Saved strategy summary: {OUTPUT_SUMMARY_FILE} ({len(summary_rows)} rows)")
    print(f"Saved strategy assignments: {OUTPUT_ASSIGNMENTS_FILE} ({len(assignment_rows)} rows)")


if __name__ == "__main__":
    main()
