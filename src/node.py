from __future__ import annotations

import json
import random
import time
from pathlib import Path
from typing import Any, Dict, Optional

from src.log_manager import LOG_DIR, LogManager
from src.models import (
    CheckpointMetadata,
    LogEvent,
    MessageType,
    NodeRole,
    ProtocolMessage,
    TxState,
    Vote,
    can_participant_move,
    utc_now_iso,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SNAPSHOT_DIR = PROJECT_ROOT / "snapshots"


class ParticipantNode:
    """
    A participant site in the distributed database simulation.

    NodeA, NodeB, and NodeC are participant nodes.

    Each node has:
    - local transaction state
    - durable JSONL log file
    - checkpoint snapshot
    - recovery from log
    """

    def __init__(
        self,
        site_name: str,
        min_delay: float = 0.01,
        max_delay: float = 0.05,
        log_dir: Optional[Path] = None,
        snapshot_dir: Optional[Path] = None,
    ) -> None:
        self.site_name = site_name
        self.role = NodeRole.PARTICIPANT
        self.log_manager = LogManager(site_name, log_dir=log_dir or LOG_DIR)
        self.snapshot_dir = snapshot_dir or SNAPSHOT_DIR

        self.min_delay = min_delay
        self.max_delay = max_delay

        # In-memory state. This can be rebuilt from durable logs after crash.
        self.state_by_tx: Dict[str, TxState] = {}

        self.snapshot_dir.mkdir(parents=True, exist_ok=True)

    def _load_checkpoint_high_watermark(self) -> int:
        """
        Read the durable high-watermark from existing checkpoint snapshots.
        """
        high_watermark = 0

        for snapshot_path in self.snapshot_dir.glob(
            f"{self.site_name}_checkpoint_*.json"
        ):
            checkpoint_suffix = snapshot_path.stem.removeprefix(
                f"{self.site_name}_checkpoint_"
            )

            if not checkpoint_suffix.isdigit():
                continue

            with snapshot_path.open("r", encoding="utf-8") as file:
                try:
                    snapshot = json.load(file)
                except json.JSONDecodeError:
                    continue

            high_watermark = max(
                high_watermark,
                int(snapshot.get("last_checkpointed_gseq", 0)),
            )

        return high_watermark

    def _simulate_communication_delay(self) -> None:
        """
        Simulate communication delay between distributed sites.
        """
        delay = random.uniform(self.min_delay, self.max_delay)
        time.sleep(delay)

    def clear_logs(self) -> None:
        """
        Clear node log and checkpoint high-watermark for testing/demo reset.
        """
        self.log_manager.clear()
        self.state_by_tx.clear()

        for snapshot_path in self.snapshot_dir.glob(
            f"{self.site_name}_checkpoint_*"
        ):
            if snapshot_path.is_file():
                snapshot_path.unlink()

    def get_state(self, tx_id: str) -> TxState:
        """
        Return current in-memory state of a transaction.
        Default state is INIT.
        """
        return self.state_by_tx.get(tx_id, TxState.INIT)

    def handle_prepare(
        self,
        transaction: Dict[str, Any],
        gseq: int,
        can_commit: bool = True,
    ) -> ProtocolMessage:
        """
        Handle PREPARE from Coordinator.

        If the node can commit:
        - move INIT -> READY
        - write READY log
        - return VOTE_COMMIT

        If the node cannot commit:
        - move INIT -> ABORT
        - write ABORT log
        - return VOTE_ABORT

        """

        self._simulate_communication_delay()

        tx_id = str(transaction["tx_id"])
        current_state = self.get_state(tx_id)

        if not can_commit:
            if can_participant_move(current_state, TxState.ABORT):
                self.state_by_tx[tx_id] = TxState.ABORT

                self.log_manager.append_log(
                    gseq=gseq,
                    tx_id=tx_id,
                    role=self.role,
                    state=TxState.ABORT,
                    event=LogEvent.VOTE_ABORT,
                    details={
                        "reason": "participant cannot commit",
                        "transaction": transaction,
                    },
                )

            return ProtocolMessage(
                message_type=MessageType.VOTE_ABORT,
                tx_id=tx_id,
                gseq=gseq,
                sender=self.site_name,
                receiver="Coordinator",
                payload={
                    "vote": Vote.ABORT.value,
                    "state": TxState.ABORT.value,
                },
            )

        if current_state == TxState.READY:
            # Idempotent behavior: already prepared before.
            return ProtocolMessage(
                message_type=MessageType.VOTE_COMMIT,
                tx_id=tx_id,
                gseq=gseq,
                sender=self.site_name,
                receiver="Coordinator",
                payload={
                    "vote": Vote.COMMIT.value,
                    "state": TxState.READY.value,
                    "note": "already READY",
                },
            )

        if not can_participant_move(current_state, TxState.READY):
            return ProtocolMessage(
                message_type=MessageType.VOTE_ABORT,
                tx_id=tx_id,
                gseq=gseq,
                sender=self.site_name,
                receiver="Coordinator",
                payload={
                    "vote": Vote.ABORT.value,
                    "state": current_state.value,
                    "reason": f"invalid transition {current_state.value} -> READY",
                },
            )

        self.state_by_tx[tx_id] = TxState.READY

        self.log_manager.append_log(
            gseq=gseq,
            tx_id=tx_id,
            role=self.role,
            state=TxState.READY,
            event=LogEvent.READY,
            details={
                "message": "participant prepared and voted commit",
                "transaction": transaction,
            },
        )

        return ProtocolMessage(
            message_type=MessageType.VOTE_COMMIT,
            tx_id=tx_id,
            gseq=gseq,
            sender=self.site_name,
            receiver="Coordinator",
            payload={
                "vote": Vote.COMMIT.value,
                "state": TxState.READY.value,
            },
        )

    def handle_global_commit(self, tx_id: str, gseq: int) -> ProtocolMessage:
        """
        Handle GLOBAL_COMMIT from Coordinator.

        Valid transition:
        READY -> COMMIT
        """

        self._simulate_communication_delay()

        current_state = self.get_state(tx_id)

        if current_state == TxState.COMMIT:
            return self._ack(tx_id, gseq, TxState.COMMIT, "already committed")

        if not can_participant_move(current_state, TxState.COMMIT):
            return self._ack(
                tx_id,
                gseq,
                current_state,
                f"cannot commit from {current_state.value}",
            )

        self.state_by_tx[tx_id] = TxState.COMMIT

        self.log_manager.append_log(
            gseq=gseq,
            tx_id=tx_id,
            role=self.role,
            state=TxState.COMMIT,
            event=LogEvent.COMMIT,
            details={
                "message": "global commit received",
            },
        )

        return self._ack(tx_id, gseq, TxState.COMMIT, "commit acknowledged")

    def handle_global_abort(self, tx_id: str, gseq: int) -> ProtocolMessage:
        """
        Handle GLOBAL_ABORT from Coordinator.

        Valid transitions:
        INIT  -> ABORT
        READY -> ABORT
        """

        self._simulate_communication_delay()

        current_state = self.get_state(tx_id)

        if current_state == TxState.ABORT:
            return self._ack(tx_id, gseq, TxState.ABORT, "already aborted")

        if not can_participant_move(current_state, TxState.ABORT):
            return self._ack(
                tx_id,
                gseq,
                current_state,
                f"cannot abort from {current_state.value}",
            )

        self.state_by_tx[tx_id] = TxState.ABORT

        self.log_manager.append_log(
            gseq=gseq,
            tx_id=tx_id,
            role=self.role,
            state=TxState.ABORT,
            event=LogEvent.ABORT,
            details={
                "message": "global abort received",
            },
        )

        return self._ack(tx_id, gseq, TxState.ABORT, "abort acknowledged")

    def _ack(
        self,
        tx_id: str,
        gseq: int,
        state: TxState,
        message: str,
    ) -> ProtocolMessage:
        """
        Return ACK message to Coordinator.
        """
        return ProtocolMessage(
            message_type=MessageType.ACK,
            tx_id=tx_id,
            gseq=gseq,
            sender=self.site_name,
            receiver="Coordinator",
            payload={
                "state": state.value,
                "message": message,
            },
        )

    def create_checkpoint(self, checkpoint_id: int) -> CheckpointMetadata:
        """
        Create local checkpoint snapshot.

        The checkpoint is built from durable logs, so it also works
        after a workload run or after node restart.
        """

        summary = self.log_manager.build_checkpoint_summary()

        latest_state_by_tx = summary["latest_state_by_tx"]
        observed_max_gseq = summary["last_checkpointed_gseq"]
        previous_high_watermark = self._load_checkpoint_high_watermark()
        terminal_gseqs = summary["terminal_gseqs"]
        contiguous_final_gseq = previous_high_watermark

        while contiguous_final_gseq + 1 in terminal_gseqs:
            contiguous_final_gseq += 1

        last_checkpointed_gseq = contiguous_final_gseq
        active_tx_ids = summary["active_tx_ids"]
        in_doubt_tx_ids = summary["in_doubt_tx_ids"]

        # Rebuild in-memory state from durable log before writing snapshot.
        self.state_by_tx = latest_state_by_tx

        snapshot = {
            "checkpoint_id": checkpoint_id,
            "site": self.site_name,
            "last_checkpointed_gseq": last_checkpointed_gseq,
            "observed_max_gseq": observed_max_gseq,
            "previous_high_watermark": previous_high_watermark,
            "contiguous_final_gseq": contiguous_final_gseq,
            "active_tx_ids": active_tx_ids,
            "in_doubt_tx_ids": in_doubt_tx_ids,
            "state_by_tx_count": len(self.state_by_tx),
            "state_by_tx_sample": {
                tx_id: state.value
                for tx_id, state in list(self.state_by_tx.items())[:10]
            },
            "timestamp": utc_now_iso(),
        }

        snapshot_path = (
            self.snapshot_dir
            / f"{self.site_name}_checkpoint_{checkpoint_id}.json"
        )

        with snapshot_path.open("w", encoding="utf-8") as file:
            json.dump(snapshot, file, indent=2, ensure_ascii=False)

        self.log_manager.append_log(
            gseq=last_checkpointed_gseq,
            tx_id=None,
            role=self.role,
            state=TxState.END,
            event=LogEvent.CHECKPOINT,
            details={
                "checkpoint_id": checkpoint_id,
                "snapshot_path": str(snapshot_path),
                "observed_max_gseq": observed_max_gseq,
                "previous_high_watermark": previous_high_watermark,
                "contiguous_final_gseq": contiguous_final_gseq,
                "active_tx_count": len(active_tx_ids),
                "in_doubt_tx_count": len(in_doubt_tx_ids),
                "state_by_tx_count": len(self.state_by_tx),
            },
        )

        return CheckpointMetadata(
            checkpoint_id=checkpoint_id,
            site=self.site_name,
            last_checkpointed_gseq=last_checkpointed_gseq,
            observed_max_gseq=observed_max_gseq,
            previous_high_watermark=previous_high_watermark,
            contiguous_final_gseq=contiguous_final_gseq,
            active_tx_ids=active_tx_ids,
            in_doubt_tx_ids=in_doubt_tx_ids,
            log_size_before=self.log_manager.get_log_size(),
        )

    def recover_from_log(self) -> Dict[str, Any]:
        """
        Recover in-memory transaction states from durable log.

        This method simulates node restart after crash.
        """

        latest_state_by_tx = self.log_manager.get_latest_state_by_tx()
        self.state_by_tx = latest_state_by_tx

        in_doubt_tx_ids = self.log_manager.get_in_doubt_tx_ids()

        self.log_manager.append_log(
            gseq=None,
            tx_id=None,
            role=self.role,
            state=TxState.END,
            event=LogEvent.RECOVERY,
            details={
                "message": "node recovered from durable log",
                "recovered_tx_count": len(latest_state_by_tx),
                "in_doubt_tx_ids": in_doubt_tx_ids,
            },
        )

        return {
            "site": self.site_name,
            "recovered_tx_count": len(latest_state_by_tx),
            "in_doubt_tx_ids": in_doubt_tx_ids,
            "state_by_tx": {
                tx_id: state.value
                for tx_id, state in self.state_by_tx.items()
            },
        }

    def status(self) -> Dict[str, Any]:
        """
        Return current node status.
        """
        return {
            "site": self.site_name,
            "log_path": str(self.log_manager.log_path),
            "log_size": self.log_manager.get_log_size(),
            "state_by_tx": {
                tx_id: state.value
                for tx_id, state in self.state_by_tx.items()
            },
            "active_tx_ids": self.log_manager.get_active_tx_ids(),
            "in_doubt_tx_ids": self.log_manager.get_in_doubt_tx_ids(),
        }


def run_participant_node(
    site_name: str,
    input_queue,
    output_queue,
    min_delay: float = 0.01,
    max_delay: float = 0.05,
) -> None:
    """
    Process loop for multiprocessing simulation.

    This function will be used later when Coordinator communicates with
    NodeA, NodeB, and NodeC through queues.
    """

    node = ParticipantNode(
        site_name=site_name,
        min_delay=min_delay,
        max_delay=max_delay,
    )

    while True:
        message = input_queue.get()

        message_type = message.get("message_type")

        if message_type == MessageType.SHUTDOWN.value:
            break

        try:
            if message_type == MessageType.PREPARE.value:
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

            elif message_type == MessageType.CHECKPOINT_REQUEST.value:
                checkpoint_id = int(message["payload"]["checkpoint_id"])
                metadata = node.create_checkpoint(checkpoint_id)
                output_queue.put(
                    ProtocolMessage(
                        message_type=MessageType.CHECKPOINT_RESPONSE,
                        sender=site_name,
                        receiver="Coordinator",
                        payload=metadata.to_dict(),
                    ).to_dict()
                )

            elif message_type == MessageType.RECOVERY_REQUEST.value:
                result = node.recover_from_log()
                output_queue.put(
                    ProtocolMessage(
                        message_type=MessageType.RECOVERY_RESPONSE,
                        sender=site_name,
                        receiver="Coordinator",
                        payload=result,
                    ).to_dict()
                )

            else:
                output_queue.put(
                    {
                        "message_type": "ERROR",
                        "sender": site_name,
                        "payload": {
                            "error": f"unknown message type: {message_type}"
                        },
                    }
                )

        except Exception as exc:
            output_queue.put(
                {
                    "message_type": "ERROR",
                    "sender": site_name,
                    "payload": {
                        "error": str(exc),
                    },
                }
            )
