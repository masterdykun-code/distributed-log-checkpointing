from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.log_manager import LogManager


SNAPSHOT_DIR = PROJECT_ROOT / "snapshots"
METRICS_DIR = PROJECT_ROOT / "metrics"


def load_global_checkpoint(checkpoint_id: int) -> Dict[str, Any]:
    path = SNAPSHOT_DIR / f"global_checkpoint_{checkpoint_id}.json"

    if not path.exists():
        raise FileNotFoundError(
            f"Global checkpoint not found: {path}. "
            f"Run: python scripts/run_global_checkpoint.py --checkpoint-id {checkpoint_id}"
        )

    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def write_json_summary(checkpoint_id: int, summary: Dict[str, Any]) -> Path:
    METRICS_DIR.mkdir(parents=True, exist_ok=True)

    path = METRICS_DIR / f"prune_checkpoint_{checkpoint_id}_summary.json"

    with path.open("w", encoding="utf-8") as file:
        json.dump(summary, file, indent=2, ensure_ascii=False)

    return path


def append_csv_metrics(checkpoint_id: int, results: List[Dict[str, Any]]) -> Path:
    METRICS_DIR.mkdir(parents=True, exist_ok=True)

    path = METRICS_DIR / "checkpoint_metrics.csv"

    fieldnames = [
        "checkpoint_id",
        "site",
        "global_safe_point",
        "protected_tx_count",
        "before_bytes",
        "after_bytes",
        "saved_bytes",
        "saved_percent",
        "pruned_records",
        "remaining_records",
    ]

    write_header = not path.exists()

    with path.open("a", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)

        if write_header:
            writer.writeheader()

        for result in results:
            writer.writerow(
                {
                    "checkpoint_id": checkpoint_id,
                    "site": result["site"],
                    "global_safe_point": result["global_safe_point"],
                    "protected_tx_count": result["protected_tx_count"],
                    "before_bytes": result["before_bytes"],
                    "after_bytes": result["after_bytes"],
                    "saved_bytes": result["saved_bytes"],
                    "saved_percent": result["saved_percent"],
                    "pruned_records": result["pruned_records"],
                    "remaining_records": result["remaining_records"],
                }
            )

    return path


def run_pruning(checkpoint_id: int, include_coordinator: bool) -> Dict[str, Any]:
    global_checkpoint = load_global_checkpoint(checkpoint_id)

    global_safe_point = int(global_checkpoint["global_safe_point"])
    protected_tx_ids = global_checkpoint.get("protected_tx_ids", [])

    sites = list(global_checkpoint.get("sites", ["NodeA", "NodeB", "NodeC"]))

    if include_coordinator:
        sites = ["Coordinator"] + sites

    results: List[Dict[str, Any]] = []

    print(f"Using global_safe_point = {global_safe_point}")
    print(f"Protected transactions = {len(protected_tx_ids)}")
    print(f"Sites to prune = {sites}")
    print()

    for site in sites:
        print(f"Pruning logs for {site}...")

        log_manager = LogManager(site)

        result = log_manager.prune_logs(
            global_safe_point=global_safe_point,
            protected_tx_ids=protected_tx_ids,
        )

        results.append(result)

        print(
            f"{site}: before={result['before_bytes']} bytes, "
            f"after={result['after_bytes']} bytes, "
            f"saved={result['saved_bytes']} bytes "
            f"({result['saved_percent']}%), "
            f"pruned_records={result['pruned_records']}, "
            f"remaining_records={result['remaining_records']}"
        )

    total_before = sum(result["before_bytes"] for result in results)
    total_after = sum(result["after_bytes"] for result in results)
    total_saved = total_before - total_after
    total_saved_percent = round((total_saved / total_before) * 100, 2) if total_before else 0.0

    summary = {
        "checkpoint_id": checkpoint_id,
        "global_safe_point": global_safe_point,
        "protected_tx_count": len(protected_tx_ids),
        "include_coordinator": include_coordinator,
        "results": results,
        "total_before_bytes": total_before,
        "total_after_bytes": total_after,
        "total_saved_bytes": total_saved,
        "total_saved_percent": total_saved_percent,
    }

    json_path = write_json_summary(checkpoint_id, summary)
    csv_path = append_csv_metrics(checkpoint_id, results)

    print("\nLog pruning completed.")
    print(f"Total before: {total_before} bytes")
    print(f"Total after : {total_after} bytes")
    print(f"Total saved : {total_saved} bytes ({total_saved_percent}%)")
    print(f"JSON summary saved to: {json_path}")
    print(f"CSV metrics saved to : {csv_path}")

    return summary


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Prune logs using global checkpoint safe point."
    )

    parser.add_argument(
        "--checkpoint-id",
        type=int,
        default=1,
        help="Checkpoint id.",
    )

    parser.add_argument(
        "--include-coordinator",
        action="store_true",
        help="Also prune Coordinator.log.",
    )

    args = parser.parse_args()

    if args.checkpoint_id <= 0:
        raise ValueError("--checkpoint-id must be greater than 0.")

    run_pruning(
        checkpoint_id=args.checkpoint_id,
        include_coordinator=args.include_coordinator,
    )


if __name__ == "__main__":
    main()