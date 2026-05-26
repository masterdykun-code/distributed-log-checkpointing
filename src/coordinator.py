from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.log_manager import LogManager
from src.models import (
    LogEvent,
    MessageType,
    NodeRole,
    ProtocolMessage,
    TxState,
    Vote,
    can_coordinator_move,
    utc_now_iso,
)
from src.node import ParticipantNode


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
GLOBAL_TX_TABLE_PATH = DATA_DIR / "global_tx_table.json"


class Coordinator:
    """
    Coordinator for the Two-Phase Commit protocol.

    The Coordinator is responsible for:
    - assigning global sequence numbers
    - sending PREPARE messages
    - collecting participant votes
    - deciding GLOBAL_COMMIT or GLOBAL_ABORT
    - writing coordinator logs
    - storing global transaction decisions
    """

    def __init__(
        self,
        participants: Optional[List[ParticipantNode]] = None,
        site_name: str = "Coordinator",
        persist_each_transaction: bool = True,
        store_detailed_history: bool = True,
    ) -> None:
        self.site_name = site_name
        self.role = NodeRole.COORDINATOR
        self.log_manager = LogManager(site_name)
        self.persist_each_transaction = persist_each_transaction
        self.store_detailed_history = store_detailed_history

        self.participants: List[ParticipantNode] = participants or [
            ParticipantNode("NodeA"),
            ParticipantNode("NodeB"),
            ParticipantNode("NodeC"),
        ]

        self.state_by_tx: Dict[str, TxState] = {}
        self.global_tx_table: Dict[str, Dict[str, Any]] = {}

        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self._load_global_tx_table()

        self.current_gseq = self._get_max_gseq_from_table()

    def _load_global_tx_table(self) -> None:
        """
        Load global transaction table from disk if it exists.
        """
        if not GLOBAL_TX_TABLE_PATH.exists():
            self.global_tx_table = {}
            return

        with GLOBAL_TX_TABLE_PATH.open("r", encoding="utf-8") as file:
            try:
                self.global_tx_table = json.load(file)
            except json.JSONDecodeError:
                self.global_tx_table = {}

    def _save_global_tx_table(self) -> None:
        """
        Save global transaction table to disk.
        """
        with GLOBAL_TX_TABLE_PATH.open("w", encoding="utf-8") as file:
            json.dump(self.global_tx_table, file, indent=2, ensure_ascii=False)

    def flush_global_tx_table(self) -> None:
        """
        Force saving the global transaction table to disk.
        Useful when persist_each_transaction is disabled during large workloads.
        """
        self._save_global_tx_table()
        
    def _get_max_gseq_from_table(self) -> int:
        """
        Return maximum gseq already used.
        """
        max_gseq = 0

        for record in self.global_tx_table.values():
            gseq = record.get("gseq")
            if gseq is not None:
                max_gseq = max(max_gseq, int(gseq))

        return max_gseq

    def next_gseq(self) -> int:
        """
        Generate next global sequence number.
        """
        self.current_gseq += 1
        return self.current_gseq

    def clear_all_logs(self) -> None:
        """
        Clear coordinator and participant logs.

        This is used for tests and demo reset.
        """
        self.log_manager.clear()
        self.state_by_tx.clear()
        self.global_tx_table.clear()
        self.current_gseq = 0
        self._save_global_tx_table()

        for participant in self.participants:
            participant.clear_logs()

    def get_participant_names(self) -> List[str]:
        return [participant.site_name for participant in self.participants]

    def execute_transaction(
        self,
        transaction: Dict[str, Any],
        can_commit_by_site: Optional[Dict[str, bool]] = None,
        crash_site_after_ready: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Execute one distributed transaction using Two-Phase Commit.

        can_commit_by_site:
            Example: {"NodeB": False}
            This makes NodeB vote abort.

        crash_site_after_ready:
            Example: "NodeB"
            This simulates NodeB crashing after writing READY.
            Full crash recovery will be handled in a later day.
        """

        can_commit_by_site = can_commit_by_site or {}

        tx_id = str(transaction["tx_id"])
        gseq = self.next_gseq()

        # Coordinator INIT -> WAIT
        current_state = self.state_by_tx.get(tx_id, TxState.INIT)

        if not can_coordinator_move(current_state, TxState.WAIT):
            raise ValueError(
                f"Invalid coordinator transition {current_state.value} -> WAIT"
            )

        self.state_by_tx[tx_id] = TxState.WAIT

        self.log_manager.append_log(
            gseq=gseq,
            tx_id=tx_id,
            role=self.role,
            state=TxState.WAIT,
            event=LogEvent.BEGIN_COMMIT,
            details={
                "message": "Coordinator begins 2PC and sends PREPARE",
                "participants": self.get_participant_names(),
                "transaction": transaction,
            },
        )

        votes: Dict[str, Dict[str, Any]] = {}
        prepare_errors: Dict[str, str] = {}

        # Phase 1: PREPARE and collect votes.
        for participant in self.participants:
            site = participant.site_name
            can_commit = can_commit_by_site.get(site, True)
            should_crash = crash_site_after_ready == site

            try:
                vote_msg = participant.handle_prepare(
                    transaction=transaction,
                    gseq=gseq,
                    can_commit=can_commit,
                    crash_after_ready=should_crash,
                )
                votes[site] = vote_msg.to_dict()

            except Exception as exc:
                prepare_errors[site] = str(exc)
                votes[site] = {
                    "message_type": "ERROR",
                    "tx_id": tx_id,
                    "gseq": gseq,
                    "sender": site,
                    "receiver": self.site_name,
                    "payload": {
                        "error": str(exc),
                    },
                    "timestamp": utc_now_iso(),
                }

        all_vote_commit = (
            len(votes) == len(self.participants)
            and not prepare_errors
            and all(
                vote["message_type"] == MessageType.VOTE_COMMIT.value
                for vote in votes.values()
            )
        )

        if all_vote_commit:
            global_decision = TxState.COMMIT
            decision_event = LogEvent.GLOBAL_COMMIT
            decision_message_type = MessageType.GLOBAL_COMMIT
        else:
            global_decision = TxState.ABORT
            decision_event = LogEvent.GLOBAL_ABORT
            decision_message_type = MessageType.GLOBAL_ABORT

        # Coordinator WAIT -> COMMIT/ABORT
        if not can_coordinator_move(TxState.WAIT, global_decision):
            raise ValueError(
                f"Invalid coordinator transition WAIT -> {global_decision.value}"
            )

        self.state_by_tx[tx_id] = global_decision

        self.log_manager.append_log(
            gseq=gseq,
            tx_id=tx_id,
            role=self.role,
            state=global_decision,
            event=decision_event,
            details={
                "message": f"Coordinator decision: {global_decision.value}",
                "votes": votes,
                "prepare_errors": prepare_errors,
            },
        )

        # Phase 2: send GLOBAL_COMMIT or GLOBAL_ABORT.
        acks: Dict[str, Dict[str, Any]] = {}
        decision_errors: Dict[str, str] = {}

        for participant in self.participants:
            site = participant.site_name

            # If this participant crashed during prepare, we do not send
            # final decision to it in this simple object simulation.
            # Later, failure demo will recover it from log.
            if site in prepare_errors:
                decision_errors[site] = "participant unavailable after prepare"
                continue

            try:
                if decision_message_type == MessageType.GLOBAL_COMMIT:
                    ack_msg = participant.handle_global_commit(tx_id, gseq)
                else:
                    ack_msg = participant.handle_global_abort(tx_id, gseq)

                acks[site] = ack_msg.to_dict()

            except Exception as exc:
                decision_errors[site] = str(exc)

        # Coordinator COMMIT/ABORT -> END
        if can_coordinator_move(global_decision, TxState.END):
            self.state_by_tx[tx_id] = TxState.END

            self.log_manager.append_log(
                gseq=gseq,
                tx_id=tx_id,
                role=self.role,
                state=TxState.END,
                event=LogEvent.END_OF_TRANSACTION,
                details={
                    "message": "Coordinator completed transaction",
                    "global_decision": global_decision.value,
                    "acks": acks,
                    "decision_errors": decision_errors,
                },
            )

        # Persist global transaction decision.
        if self.store_detailed_history:
            self.global_tx_table[tx_id] = {
                "tx_id": tx_id,
                "gseq": gseq,
                "global_decision": global_decision.value,
                "participants": self.get_participant_names(),
                "votes": votes,
                "acks": acks,
                "prepare_errors": prepare_errors,
                "decision_errors": decision_errors,
                "timestamp": utc_now_iso(),
            }
        else:
            self.global_tx_table[tx_id] = {
                "tx_id": tx_id,
                "gseq": gseq,
                "global_decision": global_decision.value,
                "participants": self.get_participant_names(),
                "timestamp": utc_now_iso(),
            }

        if self.persist_each_transaction:
            self._save_global_tx_table()

        return {
            "tx_id": tx_id,
            "gseq": gseq,
            "global_decision": global_decision.value,
            "votes": votes,
            "acks": acks,
            "prepare_errors": prepare_errors,
            "decision_errors": decision_errors,
        }

    def get_decision(self, tx_id: str) -> Optional[str]:
        """
        Return global decision for a transaction.

        This will be used later during recovery.
        """
        record = self.global_tx_table.get(tx_id)
        if not record:
            return None
        return record.get("global_decision")

    def status(self) -> Dict[str, Any]:
        """
        Return Coordinator status.
        """
        return {
            "site": self.site_name,
            "participants": self.get_participant_names(),
            "current_gseq": self.current_gseq,
            "state_by_tx": {
                tx_id: state.value
                for tx_id, state in self.state_by_tx.items()
            },
            "global_tx_count": len(self.global_tx_table),
            "log_path": str(self.log_manager.log_path),
            "log_size": self.log_manager.get_log_size(),
            "global_tx_table_path": str(GLOBAL_TX_TABLE_PATH),
        }