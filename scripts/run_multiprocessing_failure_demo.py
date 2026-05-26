from __future__ import annotations

import argparse
import json
import os
import queue
import sys
from multiprocessing import Process, Queue, freeze_support
from pathlib import Path
from typing import Any, Dict, List

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.checkpoint_manager import GlobalCheckpointManager
from src.coordinator import Coordinator
from src.log_manager import LogManager
from src.models import LogEvent, MessageType, NodeRole, ProtocolMessage, TxState, utc_now_iso
from src.node import ParticipantNode
from src.recovery_manager import RecoveryManager


DATA_DIR = PROJECT_ROOT / "data"
METRICS_DIR = PROJECT_ROOT / "metrics"
SNAPSHOT_DIR = PROJECT_ROOT / "snapshots"
GLOBAL_TX_TABLE_PATH = DATA_DIR / "global_tx_table.json"


def participant_process(
    site_name: str,
    input_queue: Queue,
    output_queue: Queue,
) -> None:
    """
    Participant process used by the multiprocessing failure demo.

    For the hard-crash path, the process writes READY and exits immediately.
    This simulates a participant process dying before it receives the global
    decision from the Coordinator.
    """

    node = ParticipantNode(site_name, min_delay=0.01, max_delay=0.03)

    while True:
        message = input_queue.get()
        message_type = message.get("message_type")

        if message_type == MessageType.SHUTDOWN.value:
            return

        if message_type == MessageType.PREPARE.value:
            hard_crash_after_ready = bool(
                message["payload"].get("hard_crash_after_ready", False)
            )

            if hard_crash_after_ready:
                node.handle_prepare(
                    transaction=message["payload"]["transaction"],
                    gseq=int(message["gseq"]),
                    can_commit=bool(message["payload"].get("can_commit", True)),
                    crash_after_ready=False,
                )
                os._exit(2)

            response = node.handle_prepare(
                transaction=message["payload"]["transaction"],
                gseq=int(message["gseq"]),
                can_commit=bool(message["payload"].get("can_commit", True)),
            )
            output_queue.put(response.to_dict())

        elif message_type == MessageType.GLOBAL_COMMIT.value:
            response = node.handle_global_commit(
                tx_id=str(message["tx_id"]),
                gseq=int(message["gseq"]),
            )
            output_queue.put(response.to_dict())

        elif message_type == MessageType.GLOBAL_ABORT.value:
            response = node.handle_global_abort(
                tx_id=str(message["tx_id"]),
                gseq=int(message["gseq"]),
            )
            output_queue.put(response.to_dict())


def make_message(
    message_type: MessageType,
    *,
    tx_id: str,
    gseq: int,
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


def collect_messages(
    output_queue: Queue,
    *,
    expected_count: int,
    timeout_seconds: float,
) -> List[dict]:
    messages: List[dict] = []

    for _ in range(expected_count):
        try:
            messages.append(output_queue.get(timeout=timeout_seconds))
        except queue.Empty:
            break

    return messages


def write_global_tx_table(record: Dict[str, Any]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    with GLOBAL_TX_TABLE_PATH.open("w", encoding="utf-8") as file:
        json.dump({record["tx_id"]: record}, file, indent=2, ensure_ascii=False)


def create_local_checkpoints(checkpoint_id: int) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []

    for site in ["NodeA", "NodeB", "NodeC"]:
        node = ParticipantNode(site, min_delay=0.0, max_delay=0.0)
        metadata = node.create_checkpoint(checkpoint_id)
        result = metadata.to_dict()
        result["active_tx_count"] = len(metadata.active_tx_ids)
        result["in_doubt_tx_count"] = len(metadata.in_doubt_tx_ids)
        results.append(result)

    summary = {
        "checkpoint_id": checkpoint_id,
        "sites": results,
    }

    METRICS_DIR.mkdir(parents=True, exist_ok=True)
    summary_path = METRICS_DIR / f"local_checkpoint_{checkpoint_id}_summary.json"

    with summary_path.open("w", encoding="utf-8") as file:
        json.dump(summary, file, indent=2, ensure_ascii=False)

    return results


def prune_logs(checkpoint_id: int) -> List[Dict[str, Any]]:
    global_checkpoint_path = SNAPSHOT_DIR / f"global_checkpoint_{checkpoint_id}.json"

    with global_checkpoint_path.open("r", encoding="utf-8") as file:
        global_checkpoint = json.load(file)

    global_safe_point = int(global_checkpoint["global_safe_point"])
    protected_tx_ids = global_checkpoint.get("protected_tx_ids", [])
    results: List[Dict[str, Any]] = []

    for site in ["Coordinator", "NodeA", "NodeB", "NodeC"]:
        result = LogManager(site).prune_logs(
            global_safe_point=global_safe_point,
            protected_tx_ids=protected_tx_ids,
        )
        results.append(result)

    return results


def has_ready_log(site: str, tx_id: str) -> bool:
    return any(
        record.get("tx_id") == tx_id and record.get("state") == TxState.READY.value
        for record in LogManager(site).read_logs()
    )


def stop_live_processes(
    *,
    input_queues: Dict[str, Queue],
    processes: Dict[str, Process],
) -> None:
    for site in ["NodeA", "NodeC"]:
        if processes[site].is_alive():
            input_queues[site].put(
                ProtocolMessage(
                    message_type=MessageType.SHUTDOWN,
                    sender="Coordinator",
                    receiver=site,
                ).to_dict()
            )

    for process in processes.values():
        process.join(timeout=2.0)

    for process in processes.values():
        if process.is_alive():
            process.terminate()
            process.join(timeout=2.0)


def run_demo(checkpoint_id: int) -> Dict[str, Any]:
    tx = {
        "tx_id": "TX_MP_FAIL_001",
        "account_id": "ACC7777",
        "symbol": "NVDA",
        "side": "BUY",
        "quantity": 50,
        "price": 920.40,
        "timestamp": "multiprocessing-failure-demo",
    }

    Coordinator().clear_all_logs()

    coordinator_log = LogManager("Coordinator")
    gseq = 1
    sites = ["NodeA", "NodeB", "NodeC"]

    input_queues = {site: Queue() for site in sites}
    output_queue: Queue = Queue()
    processes = {
        site: Process(
            target=participant_process,
            args=(site, input_queues[site], output_queue),
            name=site,
        )
        for site in sites
    }

    for process in processes.values():
        process.start()

    votes: Dict[str, dict] = {}
    prepare_errors: Dict[str, str] = {}
    acks: Dict[str, dict] = {}
    decision_errors: Dict[str, str] = {}

    try:
        print("Step 1: Coordinator writes BEGIN_COMMIT and sends PREPARE.")
        coordinator_log.append_log(
            gseq=gseq,
            tx_id=tx["tx_id"],
            role=NodeRole.COORDINATOR,
            state=TxState.WAIT,
            event=LogEvent.BEGIN_COMMIT,
            details={
                "message": "multiprocessing demo begins 2PC",
                "participants": sites,
                "transaction": tx,
            },
        )

        for site in sites:
            input_queues[site].put(
                make_message(
                    MessageType.PREPARE,
                    tx_id=tx["tx_id"],
                    gseq=gseq,
                    receiver=site,
                    payload={
                        "transaction": tx,
                        "can_commit": True,
                        "hard_crash_after_ready": site == "NodeB",
                    },
                )
            )

        for message in collect_messages(
            output_queue,
            expected_count=3,
            timeout_seconds=1.0,
        ):
            votes[str(message["sender"])] = message

        processes["NodeB"].join(timeout=1.0)

        if "NodeB" not in votes:
            prepare_errors["NodeB"] = (
                f"process exited after READY with exitcode={processes['NodeB'].exitcode}"
            )

        print("Step 2: NodeB process has crashed after READY.")
        print(f"NodeB exitcode: {processes['NodeB'].exitcode}")

        global_decision = TxState.ABORT

        coordinator_log.append_log(
            gseq=gseq,
            tx_id=tx["tx_id"],
            role=NodeRole.COORDINATOR,
            state=global_decision,
            event=LogEvent.GLOBAL_ABORT,
            details={
                "message": "Coordinator decision: ABORT",
                "votes": votes,
                "prepare_errors": prepare_errors,
            },
        )

        print("Step 3: Coordinator sends GLOBAL_ABORT to live participants.")

        for site in ["NodeA", "NodeC"]:
            input_queues[site].put(
                make_message(
                    MessageType.GLOBAL_ABORT,
                    tx_id=tx["tx_id"],
                    gseq=gseq,
                    receiver=site,
                )
            )

        for message in collect_messages(
            output_queue,
            expected_count=2,
            timeout_seconds=1.0,
        ):
            acks[str(message["sender"])] = message

        decision_errors["NodeB"] = "participant process unavailable after READY"

        coordinator_log.append_log(
            gseq=gseq,
            tx_id=tx["tx_id"],
            role=NodeRole.COORDINATOR,
            state=TxState.END,
            event=LogEvent.END_OF_TRANSACTION,
            details={
                "message": "Coordinator completed transaction",
                "global_decision": global_decision.value,
                "acks": acks,
                "decision_errors": decision_errors,
            },
        )

        tx_record = {
            "tx_id": tx["tx_id"],
            "gseq": gseq,
            "global_decision": global_decision.value,
            "participants": sites,
            "votes": votes,
            "acks": acks,
            "prepare_errors": prepare_errors,
            "decision_errors": decision_errors,
            "timestamp": utc_now_iso(),
        }
        write_global_tx_table(tx_record)

        print("Step 4: Create local and global checkpoints.")
        local_checkpoints = create_local_checkpoints(checkpoint_id)
        global_checkpoint = GlobalCheckpointManager().create_global_checkpoint(checkpoint_id)

        print("Step 5: Try safe log pruning.")
        prune_results = prune_logs(checkpoint_id)

        nodeb_ready_preserved = has_ready_log("NodeB", tx["tx_id"])

        if not nodeb_ready_preserved:
            raise RuntimeError("NodeB READY log was pruned. This is unsafe.")

        print("Step 6: Recover NodeB from durable log.")
        recovery_result = RecoveryManager("NodeB").recover()

        applied_decision = recovery_result["decisions_applied"].get(tx["tx_id"])

        if applied_decision != global_decision.value:
            raise RuntimeError("RecoveryManager did not apply the global decision.")

        stop_live_processes(input_queues=input_queues, processes=processes)

        summary = {
            "tx_id": tx["tx_id"],
            "checkpoint_id": checkpoint_id,
            "global_decision": global_decision.value,
            "process_exitcodes": {
                site: process.exitcode for site, process in processes.items()
            },
            "nodeb_ready_log_preserved_after_pruning": nodeb_ready_preserved,
            "local_checkpoints": local_checkpoints,
            "global_checkpoint": global_checkpoint,
            "prune_results": prune_results,
            "recovery_result": recovery_result,
        }

        METRICS_DIR.mkdir(parents=True, exist_ok=True)
        summary_path = METRICS_DIR / "multiprocessing_failure_demo_summary.json"

        with summary_path.open("w", encoding="utf-8") as file:
            json.dump(summary, file, indent=2, ensure_ascii=False)

        print("Multiprocessing failure demo completed successfully.")
        print(f"Summary saved to: {summary_path}")

        return summary

    finally:
        stop_live_processes(input_queues=input_queues, processes=processes)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run a multiprocessing NodeB crash and recovery demo."
    )
    parser.add_argument(
        "--checkpoint-id",
        type=int,
        default=100,
        help="Checkpoint id for the multiprocessing failure demo.",
    )

    args = parser.parse_args()

    if args.checkpoint_id <= 0:
        raise ValueError("--checkpoint-id must be greater than 0.")

    run_demo(args.checkpoint_id)


if __name__ == "__main__":
    freeze_support()
    main()
