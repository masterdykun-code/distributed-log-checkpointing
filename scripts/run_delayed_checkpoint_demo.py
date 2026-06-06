from __future__ import annotations

import argparse
import json
import queue
import sys
import time
from multiprocessing import Event, Process, Queue, freeze_support
from pathlib import Path
from typing import Any, Dict, List

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.checkpoint_manager import GlobalCheckpointManager
from src.node import ParticipantNode


DATASET_PATH = PROJECT_ROOT / "data" / "transactions_100k.jsonl"
DEMO_LOG_DIR = PROJECT_ROOT / "logs" / "delayed_site_demo"
DEMO_METRICS_DIR = PROJECT_ROOT / "metrics" / "delayed_site_demo"
DEMO_SNAPSHOT_DIR = PROJECT_ROOT / "snapshots" / "delayed_site_demo"
SUMMARY_PATH = PROJECT_ROOT / "metrics" / "delayed_site_demo_summary.json"
SITES = ["NodeA", "NodeB", "NodeC"]


def clear_demo_outputs() -> None:
    """
    Clear only artifacts owned by this isolated demo.
    """
    for directory in [DEMO_LOG_DIR, DEMO_METRICS_DIR, DEMO_SNAPSHOT_DIR]:
        directory.mkdir(parents=True, exist_ok=True)

        for path in directory.iterdir():
            if path.is_file():
                path.unlink()

    if SUMMARY_PATH.exists():
        SUMMARY_PATH.unlink()


def iter_transactions(limit: int):
    with DATASET_PATH.open("r", encoding="utf-8") as file:
        for gseq, line in enumerate(file, start=1):
            if gseq > limit:
                break

            line = line.strip()

            if line:
                yield gseq, json.loads(line)


def site_worker(
    site: str,
    *,
    limit: int,
    delay_seconds: float,
    checkpoint_id: int,
    checkpoint_event: Event,
    output_queue: Queue,
) -> None:
    """
    Process transactions sequentially until the checkpoint event is raised.

    Each process writes its own durable log. The configured delay changes
    actual progress; no checkpoint position is assigned in advance.
    """
    node = ParticipantNode(
        site,
        min_delay=0.0,
        max_delay=0.0,
        log_dir=DEMO_LOG_DIR,
        snapshot_dir=DEMO_SNAPSHOT_DIR,
    )
    started_at = time.perf_counter()
    processed_count = 0

    try:
        for gseq, transaction in iter_transactions(limit):
            if checkpoint_event.is_set():
                break

            node.handle_prepare(
                transaction=transaction,
                gseq=gseq,
                can_commit=True,
            )
            node.handle_global_commit(
                tx_id=str(transaction["tx_id"]),
                gseq=gseq,
            )
            processed_count = gseq

            if delay_seconds > 0:
                time.sleep(delay_seconds)

        if processed_count >= limit:
            checkpoint_event.wait()

        metadata = node.create_checkpoint(checkpoint_id)
        result = metadata.to_dict()
        result.update(
            {
                "processed_count": processed_count,
                "processing_delay_seconds": delay_seconds,
                "elapsed_seconds": round(time.perf_counter() - started_at, 4),
                "active_tx_count": len(metadata.active_tx_ids),
                "in_doubt_tx_count": len(metadata.in_doubt_tx_ids),
            }
        )
        output_queue.put({"status": "ok", "result": result})
    except Exception as exc:
        output_queue.put(
            {
                "status": "error",
                "site": site,
                "error": str(exc),
            }
        )


def collect_results(output_queue: Queue, expected_count: int) -> List[Dict[str, Any]]:
    messages: List[Dict[str, Any]] = []

    for _ in range(expected_count):
        try:
            messages.append(output_queue.get(timeout=30.0))
        except queue.Empty:
            break

    errors = [message for message in messages if message.get("status") == "error"]

    if errors:
        raise RuntimeError(f"Delayed-site worker failed: {errors}")

    results = [
        message["result"]
        for message in messages
        if message.get("status") == "ok"
    ]

    if len(results) != expected_count:
        raise RuntimeError(
            f"Expected {expected_count} checkpoint responses, got {len(results)}."
        )

    return sorted(results, key=lambda result: SITES.index(result["site"]))


def write_local_summary(checkpoint_id: int, results: List[Dict[str, Any]]) -> Path:
    path = DEMO_METRICS_DIR / f"local_checkpoint_{checkpoint_id}_summary.json"

    with path.open("w", encoding="utf-8") as file:
        json.dump(
            {
                "checkpoint_id": checkpoint_id,
                "scenario": "checkpoint_during_asymmetric_site_progress",
                "sites": results,
            },
            file,
            indent=2,
            ensure_ascii=False,
        )

    return path


def run_demo(
    *,
    limit: int,
    slow_site: str,
    fast_delay: float,
    slow_delay: float,
    checkpoint_after: float,
    checkpoint_id: int,
) -> Dict[str, Any]:
    if not DATASET_PATH.exists():
        raise FileNotFoundError(
            f"Dataset not found: {DATASET_PATH}. "
            "Run: python scripts/generate_dataset.py --records 100000"
        )

    clear_demo_outputs()

    checkpoint_event = Event()
    output_queue: Queue = Queue()
    delays = {
        site: slow_delay if site == slow_site else fast_delay
        for site in SITES
    }
    processes = {
        site: Process(
            target=site_worker,
            kwargs={
                "site": site,
                "limit": limit,
                "delay_seconds": delays[site],
                "checkpoint_id": checkpoint_id,
                "checkpoint_event": checkpoint_event,
                "output_queue": output_queue,
            },
            name=f"DelayedCheckpoint-{site}",
        )
        for site in SITES
    }

    print("Starting three participant processes...")
    print(f"Slow site: {slow_site}, delay={slow_delay:.4f}s/transaction")
    print(f"Fast-site delay: {fast_delay:.4f}s/transaction")

    for process in processes.values():
        process.start()

    try:
        time.sleep(checkpoint_after)
        print(f"Requesting checkpoint after {checkpoint_after:.2f} seconds...")
        checkpoint_event.set()

        results = collect_results(output_queue, expected_count=len(SITES))

        for process in processes.values():
            process.join(timeout=5.0)

        write_local_summary(checkpoint_id, results)
        manager = GlobalCheckpointManager(
            metrics_dir=DEMO_METRICS_DIR,
            snapshot_dir=DEMO_SNAPSHOT_DIR,
        )
        global_result = manager.create_global_checkpoint(checkpoint_id)

        global_snapshot_path = (
            DEMO_SNAPSHOT_DIR / f"global_checkpoint_{checkpoint_id}.json"
        )

        with global_snapshot_path.open("r", encoding="utf-8") as file:
            global_snapshot = json.load(file)

        site_progress = {
            result["site"]: int(result["last_checkpointed_gseq"])
            for result in results
        }
        fast_progress = [
            progress
            for site, progress in site_progress.items()
            if site != slow_site
        ]

        if site_progress[slow_site] >= max(fast_progress):
            raise RuntimeError(
                "The configured slow site did not lag behind. "
                "Increase --slow-delay or --checkpoint-after."
            )

        summary = {
            "type": "DELAYED_SITE_CHECKPOINT_DEMO",
            "dataset": str(DATASET_PATH),
            "limit": limit,
            "checkpoint_id": checkpoint_id,
            "checkpoint_after_seconds": checkpoint_after,
            "slow_site": slow_site,
            "fast_delay_seconds": fast_delay,
            "slow_delay_seconds": slow_delay,
            "site_progress": site_progress,
            "global_safe_point": int(global_result["global_safe_point"]),
            "safe_point_formula": "min(site_progress.values())",
            "safe_point_limited_by": [
                site
                for site, progress in site_progress.items()
                if progress == int(global_result["global_safe_point"])
            ],
            "local_checkpoints": results,
            "global_checkpoint": global_snapshot,
            "artifacts": {
                "log_dir": str(DEMO_LOG_DIR),
                "metrics_dir": str(DEMO_METRICS_DIR),
                "snapshot_dir": str(DEMO_SNAPSHOT_DIR),
            },
        }

        SUMMARY_PATH.parent.mkdir(parents=True, exist_ok=True)

        with SUMMARY_PATH.open("w", encoding="utf-8") as file:
            json.dump(summary, file, indent=2, ensure_ascii=False)

        print("\nMeasured site progress:")

        for site in SITES:
            print(f"{site}: last_checkpointed_gseq={site_progress[site]}")

        print(
            "\nglobal_safe_point = min("
            + ", ".join(str(site_progress[site]) for site in SITES)
            + f") = {summary['global_safe_point']}"
        )
        print(f"Safe point limited by: {summary['safe_point_limited_by']}")
        print(f"Summary saved to: {SUMMARY_PATH}")

        return summary
    finally:
        checkpoint_event.set()

        for process in processes.values():
            process.join(timeout=2.0)

            if process.is_alive():
                process.terminate()
                process.join(timeout=2.0)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Run participant processes at different speeds and create a "
            "global checkpoint from their measured progress."
        )
    )
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument(
        "--slow-site",
        choices=SITES,
        default="NodeB",
    )
    parser.add_argument("--fast-delay", type=float, default=0.0)
    parser.add_argument("--slow-delay", type=float, default=0.005)
    parser.add_argument("--checkpoint-after", type=float, default=1.0)
    parser.add_argument("--checkpoint-id", type=int, default=50)
    args = parser.parse_args()

    if args.limit <= 0:
        raise ValueError("--limit must be greater than 0.")

    if args.fast_delay < 0 or args.slow_delay < 0:
        raise ValueError("Delay values must be non-negative.")

    if args.slow_delay <= args.fast_delay:
        raise ValueError("--slow-delay must be greater than --fast-delay.")

    if args.checkpoint_after <= 0:
        raise ValueError("--checkpoint-after must be greater than 0.")

    if args.checkpoint_id <= 0:
        raise ValueError("--checkpoint-id must be greater than 0.")

    run_demo(
        limit=args.limit,
        slow_site=args.slow_site,
        fast_delay=args.fast_delay,
        slow_delay=args.slow_delay,
        checkpoint_after=args.checkpoint_after,
        checkpoint_id=args.checkpoint_id,
    )


if __name__ == "__main__":
    freeze_support()
    main()
