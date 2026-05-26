from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.checkpoint_manager import GlobalCheckpointManager
from src.coordinator import Coordinator
from src.log_manager import LogManager
from src.node import ParticipantNode
from src.recovery_manager import RecoveryManager


METRICS_DIR = PROJECT_ROOT / "metrics"
SNAPSHOT_DIR = PROJECT_ROOT / "snapshots"


def write_local_checkpoint_summary(checkpoint_id: int, results: List[Dict[str, Any]]) -> Path:
    METRICS_DIR.mkdir(parents=True, exist_ok=True)

    output_path = METRICS_DIR / f"local_checkpoint_{checkpoint_id}_summary.json"

    summary = {
        "checkpoint_id": checkpoint_id,
        "sites": results,
    }

    with output_path.open("w", encoding="utf-8") as file:
        json.dump(summary, file, indent=2, ensure_ascii=False)

    return output_path


def read_site_log(site: str) -> List[dict]:
    return LogManager(site).read_logs()


def print_site_log(site: str) -> None:
    print(f"\n--- {site}.log ---")
    records = read_site_log(site)

    if not records:
        print("(empty)")
        return

    for record in records:
        print(json.dumps(record, ensure_ascii=False))


def create_local_checkpoints(checkpoint_id: int) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []

    for site in ["NodeA", "NodeB", "NodeC"]:
        node = ParticipantNode(site)
        metadata = node.create_checkpoint(checkpoint_id)

        result = metadata.to_dict()
        result["active_tx_count"] = len(metadata.active_tx_ids)
        result["in_doubt_tx_count"] = len(metadata.in_doubt_tx_ids)

        results.append(result)

        print(
            f"{site}: last_checkpointed_gseq={metadata.last_checkpointed_gseq}, "
            f"active={len(metadata.active_tx_ids)}, "
            f"in_doubt={len(metadata.in_doubt_tx_ids)}"
        )

    return results


def prune_logs_for_failure_demo(checkpoint_id: int) -> List[Dict[str, Any]]:
    global_checkpoint_path = SNAPSHOT_DIR / f"global_checkpoint_{checkpoint_id}.json"

    with global_checkpoint_path.open("r", encoding="utf-8") as file:
        global_checkpoint = json.load(file)

    global_safe_point = int(global_checkpoint["global_safe_point"])
    protected_tx_ids = global_checkpoint.get("protected_tx_ids", [])

    print("\n=== Pruning attempt ===")
    print(f"global_safe_point = {global_safe_point}")
    print(f"protected_tx_ids = {protected_tx_ids}")

    results: List[Dict[str, Any]] = []

    for site in ["Coordinator", "NodeA", "NodeB", "NodeC"]:
        manager = LogManager(site)

        result = manager.prune_logs(
            global_safe_point=global_safe_point,
            protected_tx_ids=protected_tx_ids,
        )

        results.append(result)

        print(
            f"{site}: before={result['before_bytes']} bytes, "
            f"after={result['after_bytes']} bytes, "
            f"saved={result['saved_bytes']} bytes, "
            f"pruned_records={result['pruned_records']}, "
            f"remaining_records={result['remaining_records']}"
        )

    return results


def has_ready_log(site: str, tx_id: str) -> bool:
    for record in read_site_log(site):
        if record.get("tx_id") == tx_id and record.get("state") == "READY":
            return True
    return False


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Demo NodeB crash after READY and recovery from durable log."
    )

    parser.add_argument(
        "--checkpoint-id",
        type=int,
        default=99,
        help="Checkpoint id for failure demo.",
    )

    args = parser.parse_args()

    checkpoint_id = args.checkpoint_id

    tx = {
        "tx_id": "TX_FAIL_001",
        "account_id": "ACC9999",
        "symbol": "AAPL",
        "side": "BUY",
        "quantity": 100,
        "price": 187.25,
        "timestamp": "failure-demo",
    }

    print("=== Failure Demo: NodeB crash after READY ===")

    coordinator = Coordinator()
    coordinator.clear_all_logs()

    print("\nStep 1: Execute transaction and crash NodeB after READY")

    result = coordinator.execute_transaction(
        tx,
        crash_site_after_ready="NodeB",
    )

    print(json.dumps(result, indent=2, ensure_ascii=False))

    print("\nStep 2: Logs after crash")
    print_site_log("Coordinator")
    print_site_log("NodeA")
    print_site_log("NodeB")
    print_site_log("NodeC")

    print("\nStep 3: Create local checkpoints")
    local_results = create_local_checkpoints(checkpoint_id)
    local_summary_path = write_local_checkpoint_summary(checkpoint_id, local_results)
    print(f"Local checkpoint summary saved to: {local_summary_path}")

    print("\nStep 4: Create global checkpoint")
    manager = GlobalCheckpointManager()
    global_result = manager.create_global_checkpoint(checkpoint_id)
    print(json.dumps(global_result, indent=2, ensure_ascii=False))

    print("\nStep 5: Try pruning logs")
    prune_results = prune_logs_for_failure_demo(checkpoint_id)

    print("\nStep 6: Verify NodeB READY log is still protected")
    nodeb_ready_exists = has_ready_log("NodeB", tx["tx_id"])

    print(f"NodeB READY log exists after pruning: {nodeb_ready_exists}")

    if not nodeb_ready_exists:
        raise RuntimeError("ERROR: NodeB READY log was pruned. This is unsafe.")

    print_site_log("NodeB")

    print("\nStep 7: Restart NodeB and recover from durable log")
    recovery_manager = RecoveryManager("NodeB")
    recovery_result = recovery_manager.recover()
    print(json.dumps(recovery_result, indent=2, ensure_ascii=False))

    print("\nStep 8: NodeB checks Coordinator final decision")
    coordinator_decision = coordinator.get_decision(tx["tx_id"])
    applied_decision = recovery_result["decisions_applied"].get(tx["tx_id"])

    print(f"Coordinator decision for {tx['tx_id']}: {coordinator_decision}")
    print(f"Decision applied by RecoveryManager: {applied_decision}")

    if coordinator_decision is None:
        raise RuntimeError(f"No global decision found for {tx['tx_id']}")

    if applied_decision != coordinator_decision:
        raise RuntimeError(
            "RecoveryManager did not apply the Coordinator final decision."
        )

    ack = recovery_result["acks_after_recovery"].get(tx["tx_id"])

    if ack is None:
        raise RuntimeError("RecoveryManager did not produce recovery ACK.")

    print("\nStep 9: NodeB has written final decision after recovery")
    print(json.dumps(ack, indent=2, ensure_ascii=False))

    print_site_log("NodeB")

    summary = {
        "tx_id": tx["tx_id"],
        "checkpoint_id": checkpoint_id,
        "global_decision": coordinator_decision,
        "nodeb_ready_log_preserved_after_pruning": nodeb_ready_exists,
        "recovery_result": recovery_result,
        "ack_after_recovery": ack,
        "global_checkpoint": global_result,
        "prune_results": prune_results,
    }

    summary_path = METRICS_DIR / "failure_demo_summary.json"

    with summary_path.open("w", encoding="utf-8") as file:
        json.dump(summary, file, indent=2, ensure_ascii=False)

    print("\nFailure demo completed successfully.")
    print(f"Summary saved to: {summary_path}")


if __name__ == "__main__":
    main()
