from __future__ import annotations

import argparse
import json
import os
import queue
import random
import sys
import time
from multiprocessing import Process, Queue, freeze_support
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Dict, List

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.checkpoint_manager import GlobalCheckpointManager
from src.log_manager import LogManager
from src.models import (
    LogEvent,
    MessageType,
    NodeRole,
    ProtocolMessage,
    TxState,
    utc_now_iso,
)
from src.node import ParticipantNode


DATASET_PATH = PROJECT_ROOT / "data" / "transactions_100k.jsonl"
DEMO_LOG_DIR = PROJECT_ROOT / "logs" / "concurrent_2pc_demo"
DEMO_METRICS_DIR = PROJECT_ROOT / "metrics" / "concurrent_2pc_demo"
DEMO_SNAPSHOT_DIR = PROJECT_ROOT / "snapshots" / "concurrent_2pc_demo"
SUMMARY_PATH = PROJECT_ROOT / "metrics" / "concurrent_2pc_demo_summary.json"
DECISION_LOG_PATH = DEMO_METRICS_DIR / "global_decisions.jsonl"
GLOBAL_TX_TABLE_PATH = DEMO_METRICS_DIR / "global_tx_table.json"
SITES = ["NodeA", "NodeB", "NodeC"]


def clear_demo_outputs() -> None:
    for directory in [DEMO_LOG_DIR, DEMO_METRICS_DIR, DEMO_SNAPSHOT_DIR]:
        directory.mkdir(parents=True, exist_ok=True)

        for path in directory.iterdir():
            if path.is_file():
                path.unlink()

    if SUMMARY_PATH.exists():
        SUMMARY_PATH.unlink()


def load_transactions(limit: int) -> List[dict]:
    transactions: List[dict] = []

    with DATASET_PATH.open("r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()

            if line:
                transactions.append(json.loads(line))

            if len(transactions) >= limit:
                break

    if len(transactions) < limit:
        raise ValueError(
            f"Dataset has only {len(transactions)} records, requested {limit}."
        )

    return transactions


def append_durable_jsonl(path: Path, record: Dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8") as file:
        file.write(json.dumps(record, ensure_ascii=False) + "\n")
        file.flush()
        os.fsync(file.fileno())


def write_json_atomically(path: Path, data: Dict[str, Any]) -> None:
    with NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        delete=False,
        dir=path.parent,
        suffix=".tmp",
    ) as temp_file:
        temp_path = Path(temp_file.name)
        json.dump(data, temp_file, indent=2, ensure_ascii=False)
        temp_file.flush()
        os.fsync(temp_file.fileno())

    temp_path.replace(path)


def make_message(
    message_type: MessageType,
    *,
    tx_id: str | None = None,
    gseq: int | None = None,
    receiver: str,
    payload: Dict[str, Any] | None = None,
) -> dict:
    return ProtocolMessage(
        message_type=message_type,
        tx_id=tx_id,
        gseq=gseq,
        sender="Coordinator",
        receiver=receiver,
        payload=payload or {},
    ).to_dict()


def participant_worker(
    site: str,
    input_queue: Queue,
    control_queue: Queue,
    output_queue: Queue,
    message_delay: float,
) -> None:
    """
    One participant process with a FIFO 2PC message queue.

    PREPARE and global-decision messages share the FIFO queue. With many
    transactions in flight, a slower site accumulates a real backlog.
    Checkpoint requests use a control queue so the site can snapshot at the
    next message boundary instead of waiting for the backlog to drain.
    """
    node = ParticipantNode(
        site,
        min_delay=0.0,
        max_delay=0.0,
        log_dir=DEMO_LOG_DIR,
        snapshot_dir=DEMO_SNAPSHOT_DIR,
    )

    while True:
        try:
            control = control_queue.get_nowait()
        except queue.Empty:
            control = None

        if control is not None:
            message_type = control.get("message_type")

            if message_type == MessageType.SHUTDOWN.value:
                return

            if message_type == MessageType.CHECKPOINT_REQUEST.value:
                checkpoint_id = int(control["payload"]["checkpoint_id"])
                metadata = node.create_checkpoint(checkpoint_id)
                snapshot_path = (
                    DEMO_SNAPSHOT_DIR
                    / f"{site}_checkpoint_{checkpoint_id}.json"
                )

                with snapshot_path.open("r", encoding="utf-8") as file:
                    snapshot = json.load(file)

                result = metadata.to_dict()
                result.update(
                    {
                        "observed_max_gseq": snapshot["observed_max_gseq"],
                        "previous_high_watermark": snapshot[
                            "previous_high_watermark"
                        ],
                        "contiguous_final_gseq": snapshot[
                            "contiguous_final_gseq"
                        ],
                        "state_by_tx_count": snapshot["state_by_tx_count"],
                        "active_tx_count": len(metadata.active_tx_ids),
                        "in_doubt_tx_count": len(metadata.in_doubt_tx_ids),
                        "message_delay_seconds": message_delay,
                    }
                )
                output_queue.put(
                    {
                        "message_type": MessageType.CHECKPOINT_RESPONSE.value,
                        "sender": site,
                        "payload": result,
                        "timestamp": utc_now_iso(),
                    }
                )
                continue

        try:
            message = input_queue.get(timeout=0.01)
        except queue.Empty:
            continue

        message_type = message.get("message_type")

        try:
            if message_delay > 0:
                time.sleep(message_delay)

            if message_type == MessageType.PREPARE.value:
                response = node.handle_prepare(
                    transaction=message["payload"]["transaction"],
                    gseq=int(message["gseq"]),
                    can_commit=bool(message["payload"]["can_commit"]),
                )
            elif message_type == MessageType.GLOBAL_COMMIT.value:
                response = node.handle_global_commit(
                    tx_id=str(message["tx_id"]),
                    gseq=int(message["gseq"]),
                )
            elif message_type == MessageType.GLOBAL_ABORT.value:
                response = node.handle_global_abort(
                    tx_id=str(message["tx_id"]),
                    gseq=int(message["gseq"]),
                )
            else:
                raise ValueError(f"Unknown participant message: {message_type}")

            output_queue.put(response.to_dict())
        except Exception as exc:
            output_queue.put(
                {
                    "message_type": "ERROR",
                    "sender": site,
                    "tx_id": message.get("tx_id"),
                    "gseq": message.get("gseq"),
                    "payload": {"error": str(exc)},
                    "timestamp": utc_now_iso(),
                }
            )


def choose_abort_site(random_source: random.Random, abort_rate: float) -> str | None:
    if random_source.random() >= abort_rate:
        return None

    return random_source.choice(SITES)


def write_local_checkpoint_summary(
    checkpoint_id: int,
    results: List[Dict[str, Any]],
    coordinator_state: Dict[str, Any],
) -> Path:
    path = DEMO_METRICS_DIR / f"local_checkpoint_{checkpoint_id}_summary.json"

    with path.open("w", encoding="utf-8") as file:
        json.dump(
            {
                "checkpoint_id": checkpoint_id,
                "scenario": "concurrent_pipelined_2pc",
                "coordinator_state_at_request": coordinator_state,
                "sites": results,
            },
            file,
            indent=2,
            ensure_ascii=False,
        )

    return path


def validate_atomicity(
    decisions: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    mismatches: List[dict] = []
    final_state_counts: Dict[str, Dict[str, int]] = {}

    for site in SITES:
        latest = LogManager(site, log_dir=DEMO_LOG_DIR).get_latest_state_by_tx()
        counts = {"COMMIT": 0, "ABORT": 0}

        for tx_id, record in decisions.items():
            expected = str(record["global_decision"])
            actual_state = latest.get(tx_id)
            actual = actual_state.value if actual_state is not None else None

            if actual in counts:
                counts[actual] += 1

            if actual != expected:
                mismatches.append(
                    {
                        "site": site,
                        "tx_id": tx_id,
                        "expected": expected,
                        "actual": actual,
                    }
                )

        final_state_counts[site] = counts

    return {
        "checked_transaction_count": len(decisions),
        "mismatch_count": len(mismatches),
        "mismatch_sample": mismatches[:20],
        "final_state_counts": final_state_counts,
    }


def prune_checkpoint_logs(global_checkpoint: Dict[str, Any]) -> List[dict]:
    results = []

    for site in ["Coordinator", *SITES]:
        results.append(
            LogManager(site, log_dir=DEMO_LOG_DIR).prune_logs(
                global_safe_point=int(global_checkpoint["global_safe_point"]),
                protected_tx_ids=global_checkpoint["protected_tx_ids"],
            )
        )

    return results


def run_demo(
    *,
    limit: int,
    window_size: int,
    abort_rate: float,
    slow_site: str,
    fast_delay: float,
    slow_delay: float,
    checkpoint_after: float,
    checkpoint_id: int,
    seed: int,
    prune: bool,
) -> Dict[str, Any]:
    if not DATASET_PATH.exists():
        raise FileNotFoundError(
            f"Dataset not found: {DATASET_PATH}. "
            "Run: python scripts/generate_dataset.py --records 100000"
        )

    clear_demo_outputs()
    transactions = load_transactions(limit)
    random_source = random.Random(seed)
    delays = {
        site: slow_delay if site == slow_site else fast_delay
        for site in SITES
    }
    input_queues = {site: Queue() for site in SITES}
    control_queues = {site: Queue() for site in SITES}
    output_queue: Queue = Queue()
    processes = {
        site: Process(
            target=participant_worker,
            args=(
                site,
                input_queues[site],
                control_queues[site],
                output_queue,
                delays[site],
            ),
            name=f"Concurrent2PC-{site}",
        )
        for site in SITES
    }
    coordinator_log = LogManager("Coordinator", log_dir=DEMO_LOG_DIR)
    contexts: Dict[str, Dict[str, Any]] = {}
    decisions: Dict[str, Dict[str, Any]] = {}
    checkpoint_results: Dict[str, Dict[str, Any]] = {}
    checkpoint_snapshot: Dict[str, Any] | None = None
    checkpoint_requested = False
    checkpoint_request_state: Dict[str, Any] = {}
    next_transaction_index = 0
    completed_count = 0
    decision_count = 0
    commit_count = 0
    abort_count = 0
    start_time = time.perf_counter()

    def dispatch_next() -> bool:
        nonlocal next_transaction_index

        if next_transaction_index >= limit:
            return False

        gseq = next_transaction_index + 1
        transaction = transactions[next_transaction_index]
        tx_id = str(transaction["tx_id"])
        abort_site = choose_abort_site(random_source, abort_rate)
        contexts[tx_id] = {
            "tx_id": tx_id,
            "gseq": gseq,
            "transaction": transaction,
            "abort_site": abort_site,
            "votes": {},
            "acks": {},
            "decision": None,
            "completed": False,
        }

        coordinator_log.append_log(
            gseq=gseq,
            tx_id=tx_id,
            role=NodeRole.COORDINATOR,
            state=TxState.WAIT,
            event=LogEvent.BEGIN_COMMIT,
            details={
                "message": "Concurrent Coordinator begins 2PC",
                "participants": SITES,
                "abort_site": abort_site,
                "transaction": transaction,
            },
        )

        for site in SITES:
            input_queues[site].put(
                make_message(
                    MessageType.PREPARE,
                    tx_id=tx_id,
                    gseq=gseq,
                    receiver=site,
                    payload={
                        "transaction": transaction,
                        "can_commit": site != abort_site,
                    },
                )
            )

        next_transaction_index += 1
        return True

    for process in processes.values():
        process.start()

    for _ in range(min(window_size, limit)):
        dispatch_next()

    print("Concurrent/pipelined 2PC started.")
    print(
        f"limit={limit}, window={window_size}, abort_rate={abort_rate}, "
        f"slow_site={slow_site}, slow_delay={slow_delay:.4f}s"
    )

    try:
        while completed_count < limit or len(checkpoint_results) < len(SITES):
            elapsed = time.perf_counter() - start_time

            if (
                not checkpoint_requested
                and (elapsed >= checkpoint_after or completed_count >= limit)
            ):
                checkpoint_requested = True
                checkpoint_request_state = {
                    "elapsed_seconds": round(elapsed, 4),
                    "dispatched_count": next_transaction_index,
                    "decision_count": decision_count,
                    "completed_count": completed_count,
                    "in_flight_count": next_transaction_index - completed_count,
                }
                print(
                    f"Requesting checkpoint at {elapsed:.2f}s: "
                    f"dispatched={next_transaction_index}, "
                    f"completed={completed_count}"
                )

                for site in SITES:
                    control_queues[site].put(
                        make_message(
                            MessageType.CHECKPOINT_REQUEST,
                            receiver=site,
                            payload={"checkpoint_id": checkpoint_id},
                        )
                    )

            try:
                message = output_queue.get(timeout=0.02)
            except queue.Empty:
                continue

            message_type = str(message.get("message_type"))

            if message_type == "ERROR":
                raise RuntimeError(f"Participant error: {message}")

            if message_type == MessageType.CHECKPOINT_RESPONSE.value:
                site = str(message["sender"])
                checkpoint_results[site] = message["payload"]

                if (
                    len(checkpoint_results) == len(SITES)
                    and checkpoint_snapshot is None
                ):
                    local_results = [
                        checkpoint_results[site] for site in SITES
                    ]
                    write_local_checkpoint_summary(
                        checkpoint_id,
                        local_results,
                        checkpoint_request_state,
                    )
                    manager = GlobalCheckpointManager(
                        metrics_dir=DEMO_METRICS_DIR,
                        snapshot_dir=DEMO_SNAPSHOT_DIR,
                    )
                    manager.create_global_checkpoint(checkpoint_id)
                    global_path = (
                        DEMO_SNAPSHOT_DIR
                        / f"global_checkpoint_{checkpoint_id}.json"
                    )

                    with global_path.open("r", encoding="utf-8") as file:
                        checkpoint_snapshot = json.load(file)

                    coordinator_log.append_log(
                        gseq=int(checkpoint_snapshot["global_safe_point"]),
                        tx_id=None,
                        role=NodeRole.COORDINATOR,
                        state=TxState.END,
                        event=LogEvent.CHECKPOINT,
                        details={
                            "checkpoint_id": checkpoint_id,
                            "global_safe_point": checkpoint_snapshot[
                                "global_safe_point"
                            ],
                            "site_safe_points": checkpoint_snapshot[
                                "site_safe_points"
                            ],
                        },
                    )
                continue

            tx_id = str(message["tx_id"])
            context = contexts[tx_id]

            if message_type in {
                MessageType.VOTE_COMMIT.value,
                MessageType.VOTE_ABORT.value,
            }:
                context["votes"][str(message["sender"])] = message

                if len(context["votes"]) == len(SITES):
                    all_vote_commit = all(
                        vote["message_type"]
                        == MessageType.VOTE_COMMIT.value
                        for vote in context["votes"].values()
                    )
                    decision = (
                        TxState.COMMIT if all_vote_commit else TxState.ABORT
                    )
                    decision_type = (
                        MessageType.GLOBAL_COMMIT
                        if decision == TxState.COMMIT
                        else MessageType.GLOBAL_ABORT
                    )
                    decision_event = (
                        LogEvent.GLOBAL_COMMIT
                        if decision == TxState.COMMIT
                        else LogEvent.GLOBAL_ABORT
                    )
                    context["decision"] = decision.value
                    decision_count += 1

                    if decision == TxState.COMMIT:
                        commit_count += 1
                    else:
                        abort_count += 1

                    coordinator_log.append_log(
                        gseq=context["gseq"],
                        tx_id=tx_id,
                        role=NodeRole.COORDINATOR,
                        state=decision,
                        event=decision_event,
                        details={
                            "message": (
                                f"Concurrent Coordinator decision: "
                                f"{decision.value}"
                            ),
                            "votes": context["votes"],
                        },
                    )

                    decision_record = {
                        "tx_id": tx_id,
                        "gseq": context["gseq"],
                        "global_decision": decision.value,
                        "participants": SITES,
                        "votes": context["votes"],
                        "timestamp": utc_now_iso(),
                    }
                    decisions[tx_id] = decision_record
                    append_durable_jsonl(DECISION_LOG_PATH, decision_record)

                    for site in SITES:
                        input_queues[site].put(
                            make_message(
                                decision_type,
                                tx_id=tx_id,
                                gseq=context["gseq"],
                                receiver=site,
                            )
                        )
                continue

            if message_type == MessageType.ACK.value:
                context["acks"][str(message["sender"])] = message

                if (
                    len(context["acks"]) == len(SITES)
                    and not context["completed"]
                ):
                    context["completed"] = True
                    completed_count += 1

                    coordinator_log.append_log(
                        gseq=context["gseq"],
                        tx_id=tx_id,
                        role=NodeRole.COORDINATOR,
                        state=TxState.END,
                        event=LogEvent.END_OF_TRANSACTION,
                        details={
                            "message": (
                                "Concurrent Coordinator completed transaction"
                            ),
                            "global_decision": context["decision"],
                            "acks": context["acks"],
                        },
                    )
                    decisions[tx_id]["acks"] = context["acks"]
                    dispatch_next()

        write_json_atomically(GLOBAL_TX_TABLE_PATH, decisions)

        for site in SITES:
            control_queues[site].put(
                make_message(MessageType.SHUTDOWN, receiver=site)
            )

        for process in processes.values():
            process.join(timeout=10.0)

        atomicity = validate_atomicity(decisions)

        if atomicity["mismatch_count"] > 0:
            raise RuntimeError(
                f"Atomicity validation failed: {atomicity['mismatch_sample']}"
            )

        if checkpoint_snapshot is None:
            raise RuntimeError("Global checkpoint was not created.")

        site_safe_points = checkpoint_snapshot["site_safe_points"]
        fast_safe_points = [
            int(progress)
            for site, progress in site_safe_points.items()
            if site != slow_site
        ]

        if int(site_safe_points[slow_site]) >= max(fast_safe_points):
            raise RuntimeError(
                "The slow site did not limit the safe point. "
                "Increase --slow-delay, --window-size, or --checkpoint-after."
            )

        prune_results: List[dict] = []
        prune_totals: Dict[str, Any] | None = None

        if prune:
            prune_results = prune_checkpoint_logs(checkpoint_snapshot)
            total_before_bytes = sum(
                result["before_bytes"] for result in prune_results
            )
            total_after_bytes = sum(
                result["after_bytes"] for result in prune_results
            )
            total_saved_bytes = total_before_bytes - total_after_bytes
            total_saved_percent = (
                round((total_saved_bytes / total_before_bytes) * 100, 2)
                if total_before_bytes
                else 0.0
            )
            prune_totals = {
                "before_bytes": total_before_bytes,
                "after_bytes": total_after_bytes,
                "saved_bytes": total_saved_bytes,
                "saved_percent": total_saved_percent,
            }

        elapsed_seconds = round(time.perf_counter() - start_time, 4)
        summary = {
            "type": "CONCURRENT_PIPELINED_2PC_DEMO",
            "dataset": str(DATASET_PATH),
            "limit": limit,
            "window_size": window_size,
            "abort_rate": abort_rate,
            "seed": seed,
            "slow_site": slow_site,
            "fast_delay_seconds": fast_delay,
            "slow_delay_seconds": slow_delay,
            "checkpoint_after_seconds": checkpoint_after,
            "checkpoint_id": checkpoint_id,
            "elapsed_seconds": elapsed_seconds,
            "commit_count": commit_count,
            "abort_count": abort_count,
            "completed_count": completed_count,
            "checkpoint_request_state": checkpoint_request_state,
            "site_safe_points": site_safe_points,
            "global_safe_point": checkpoint_snapshot["global_safe_point"],
            "safe_point_limited_by": [
                site
                for site, progress in site_safe_points.items()
                if int(progress)
                == int(checkpoint_snapshot["global_safe_point"])
            ],
            "active_tx_count_at_checkpoint": checkpoint_snapshot[
                "active_tx_count"
            ],
            "in_doubt_tx_count_at_checkpoint": checkpoint_snapshot[
                "in_doubt_tx_count"
            ],
            "protected_tx_count_at_checkpoint": checkpoint_snapshot[
                "protected_tx_count"
            ],
            "atomicity_validation": atomicity,
            "pruning_enabled": prune,
            "prune_results": prune_results,
            "prune_totals": prune_totals,
            "artifacts": {
                "coordinator_log": str(
                    DEMO_LOG_DIR / "Coordinator.log"
                ),
                "participant_log_dir": str(DEMO_LOG_DIR),
                "decision_log": str(DECISION_LOG_PATH),
                "global_tx_table": str(GLOBAL_TX_TABLE_PATH),
                "metrics_dir": str(DEMO_METRICS_DIR),
                "snapshot_dir": str(DEMO_SNAPSHOT_DIR),
            },
        }

        with SUMMARY_PATH.open("w", encoding="utf-8") as file:
            json.dump(summary, file, indent=2, ensure_ascii=False)

        print("\nConcurrent 2PC completed.")
        print(
            f"COMMIT={commit_count}, ABORT={abort_count}, "
            f"atomicity_mismatches={atomicity['mismatch_count']}"
        )
        print(f"Site safe points: {site_safe_points}")
        print(
            f"global_safe_point = {checkpoint_snapshot['global_safe_point']}"
        )
        print(f"Safe point limited by: {summary['safe_point_limited_by']}")

        if prune_totals is not None:
            print(
                f"Pruning saved: {prune_totals['saved_bytes']} bytes "
                f"({prune_totals['saved_percent']}%)"
            )
        else:
            print("Pruning skipped. Re-run with --prune to prune logs.")

        print(f"Summary saved to: {SUMMARY_PATH}")
        return summary
    finally:
        for site in SITES:
            if processes[site].is_alive():
                control_queues[site].put(
                    make_message(MessageType.SHUTDOWN, receiver=site)
                )

        for process in processes.values():
            process.join(timeout=2.0)

            if process.is_alive():
                process.terminate()
                process.join(timeout=2.0)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Run concurrent/pipelined 2PC with a slow participant and "
            "checkpoint the system while transactions are in flight."
        )
    )
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--window-size", type=int, default=100)
    parser.add_argument("--abort-rate", type=float, default=0.1)
    parser.add_argument("--slow-site", choices=SITES, default="NodeB")
    parser.add_argument("--fast-delay", type=float, default=0.0)
    parser.add_argument("--slow-delay", type=float, default=0.005)
    parser.add_argument("--checkpoint-after", type=float, default=1.2)
    parser.add_argument("--checkpoint-id", type=int, default=60)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--prune",
        action="store_true",
        help="Prune safe log records after the workload completes.",
    )
    args = parser.parse_args()

    if args.limit <= 0:
        raise ValueError("--limit must be greater than 0.")

    if args.window_size <= 0:
        raise ValueError("--window-size must be greater than 0.")

    if args.window_size > args.limit:
        raise ValueError("--window-size cannot be greater than --limit.")

    if not 0.0 <= args.abort_rate <= 1.0:
        raise ValueError("--abort-rate must be between 0.0 and 1.0.")

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
        window_size=args.window_size,
        abort_rate=args.abort_rate,
        slow_site=args.slow_site,
        fast_delay=args.fast_delay,
        slow_delay=args.slow_delay,
        checkpoint_after=args.checkpoint_after,
        checkpoint_id=args.checkpoint_id,
        seed=args.seed,
        prune=args.prune,
    )


if __name__ == "__main__":
    freeze_support()
    main()
