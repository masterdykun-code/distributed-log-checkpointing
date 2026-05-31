from __future__ import annotations

import argparse
import json
import random
import sys
import time
from pathlib import Path
from typing import Dict, List

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.coordinator import Coordinator
from src.node import ParticipantNode


DATASET_PATH = PROJECT_ROOT / "data" / "transactions_100k.jsonl"
METRICS_DIR = PROJECT_ROOT / "metrics"
LOG_DIR = PROJECT_ROOT / "logs"


def load_transactions(path: Path, limit: int | None = None) -> List[dict]:
    """
    Load transactions from JSONL dataset.
    """
    transactions: List[dict] = []

    with path.open("r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()

            if not line:
                continue

            transactions.append(json.loads(line))

            if limit is not None and len(transactions) >= limit:
                break

    return transactions


def get_file_size(path: Path) -> int:
    if not path.exists():
        return 0
    return path.stat().st_size


def get_log_sizes() -> Dict[str, int]:
    return {
        "Coordinator": get_file_size(LOG_DIR / "Coordinator.log"),
        "NodeA": get_file_size(LOG_DIR / "NodeA.log"),
        "NodeB": get_file_size(LOG_DIR / "NodeB.log"),
        "NodeC": get_file_size(LOG_DIR / "NodeC.log"),
    }


def choose_abort_site(abort_rate: float) -> Dict[str, bool]:
    """
    Randomly choose one participant to abort based on abort_rate.

    Return format is compatible with Coordinator.execute_transaction:
    {"NodeB": False}
    """
    if random.random() >= abort_rate:
        return {}

    site = random.choice(["NodeA", "NodeB", "NodeC"])
    return {site: False}


def choose_crash_site(crash_rate: float, excluded_site: str | None) -> str | None:
    """
    Randomly choose one participant to crash after READY.

    A participant that votes abort is excluded because it never enters READY.
    """
    if random.random() >= crash_rate:
        return None

    candidates = [
        site
        for site in ["NodeA", "NodeB", "NodeC"]
        if site != excluded_site
    ]

    if not candidates:
        return None

    return random.choice(candidates)


def make_coordinator(fast: bool) -> Coordinator:
    """
    Build Coordinator with three participants.

    In fast mode, communication delay is set to zero so larger workloads
    can run quickly.
    """
    if fast:
        min_delay = 0.0
        max_delay = 0.0
    else:
        min_delay = 0.001
        max_delay = 0.005

    participants = [
        ParticipantNode("NodeA", min_delay=min_delay, max_delay=max_delay),
        ParticipantNode("NodeB", min_delay=min_delay, max_delay=max_delay),
        ParticipantNode("NodeC", min_delay=min_delay, max_delay=max_delay),
    ]

    return Coordinator(
        participants=participants,
        persist_each_transaction=False,
        store_detailed_history=False,
    )


def run_workload(
    *,
    limit: int,
    abort_rate: float,
    crash_rate: float,
    reset: bool,
    fast: bool,
    seed: int,
    progress_every: int,
) -> dict:
    random.seed(seed)

    if not DATASET_PATH.exists():
        raise FileNotFoundError(
            f"Dataset not found: {DATASET_PATH}. "
            f"Run scripts/generate_dataset.py first."
        )

    transactions = load_transactions(DATASET_PATH, limit=limit)

    coordinator = make_coordinator(fast=fast)

    if reset:
        coordinator.clear_all_logs()

    start_time = time.time()

    commit_count = 0
    abort_count = 0
    crash_after_ready_count = 0
    error_count = 0

    for index, tx in enumerate(transactions, start=1):
        can_commit_by_site = choose_abort_site(abort_rate)
        abort_site = next(iter(can_commit_by_site), None)
        crash_site = choose_crash_site(
            crash_rate=crash_rate,
            excluded_site=abort_site,
        )

        try:
            result = coordinator.execute_transaction(
                tx,
                can_commit_by_site=can_commit_by_site,
                crash_site_after_ready=crash_site,
            )

            if result["global_decision"] == "COMMIT":
                commit_count += 1
            else:
                abort_count += 1

            if crash_site and crash_site in result.get("prepare_errors", {}):
                crash_after_ready_count += 1

        except Exception as exc:
            error_count += 1
            print(f"[ERROR] tx={tx.get('tx_id')} error={exc}")

        if progress_every > 0 and index % progress_every == 0:
            elapsed = time.time() - start_time
            tps = index / elapsed if elapsed > 0 else 0

            print(
                f"Processed {index:,}/{len(transactions):,} transactions | "
                f"COMMIT={commit_count:,} ABORT={abort_count:,} "
                f"CRASH_AFTER_READY={crash_after_ready_count:,} "
                f"ERROR={error_count:,} | TPS={tps:.2f}"
            )

    coordinator.flush_global_tx_table()

    elapsed = time.time() - start_time
    log_sizes = get_log_sizes()

    summary = {
        "input_dataset": str(DATASET_PATH),
        "requested_limit": limit,
        "processed": len(transactions),
        "commit_count": commit_count,
        "abort_count": abort_count,
        "crash_after_ready_count": crash_after_ready_count,
        "error_count": error_count,
        "abort_rate": abort_rate,
        "crash_rate": crash_rate,
        "elapsed_seconds": round(elapsed, 2),
        "transactions_per_second": round(len(transactions) / elapsed, 2)
        if elapsed > 0
        else 0,
        "log_sizes_bytes": log_sizes,
        "total_log_size_bytes": sum(log_sizes.values()),
        "global_tx_table_path": str(PROJECT_ROOT / "data" / "global_tx_table.json"),
    }

    METRICS_DIR.mkdir(parents=True, exist_ok=True)

    summary_path = METRICS_DIR / "workload_summary.json"

    with summary_path.open("w", encoding="utf-8") as file:
        json.dump(summary, file, indent=2, ensure_ascii=False)

    print("\nWorkload completed.")
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    print(f"\nSummary saved to: {summary_path}")

    return summary


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run distributed transaction workload using 2PC."
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=1000,
        help="Number of transactions to process from dataset.",
    )

    parser.add_argument(
        "--abort-rate",
        type=float,
        default=0.0,
        help="Probability that one participant votes abort.",
    )

    parser.add_argument(
        "--crash-rate",
        type=float,
        default=0.0,
        help="Probability that one participant crashes after writing READY.",
    )

    parser.add_argument(
        "--reset",
        action="store_true",
        help="Clear logs and global transaction table before running.",
    )

    parser.add_argument(
        "--fast",
        action="store_true",
        help="Disable communication delay for faster workload execution.",
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed.",
    )

    parser.add_argument(
        "--progress-every",
        type=int,
        default=1000,
        help="Print progress every N transactions.",
    )

    args = parser.parse_args()

    if args.limit <= 0:
        raise ValueError("--limit must be greater than 0.")

    if not (0.0 <= args.abort_rate <= 1.0):
        raise ValueError("--abort-rate must be between 0.0 and 1.0.")

    if not (0.0 <= args.crash_rate <= 1.0):
        raise ValueError("--crash-rate must be between 0.0 and 1.0.")

    run_workload(
        limit=args.limit,
        abort_rate=args.abort_rate,
        crash_rate=args.crash_rate,
        reset=args.reset,
        fast=args.fast,
        seed=args.seed,
        progress_every=args.progress_every,
    )


if __name__ == "__main__":
    main()
