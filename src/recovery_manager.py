from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

from src.log_manager import LogManager
from src.models import (
    LogEvent,
    MessageType,
    NodeRole,
    ProtocolMessage,
    TxState,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
GLOBAL_TX_TABLE_PATH = DATA_DIR / "global_tx_table.json"


class RecoveryManager:
    """
    Recover a participant site from durable logs.

    The important 2PC case is a participant that restarts in READY state.
    READY is an in-doubt state, so the participant must learn the final
    Coordinator decision before it can write COMMIT or ABORT.
    """

    def __init__(
        self,
        site_name: str,
        *,
        log_manager: Optional[LogManager] = None,
        global_tx_table_path: Path = GLOBAL_TX_TABLE_PATH,
    ) -> None:
        self.site_name = site_name
        self.role = NodeRole.PARTICIPANT
        self.log_manager = log_manager or LogManager(site_name)
        self.global_tx_table_path = global_tx_table_path

    def recover(self) -> Dict[str, Any]:
        """
        Rebuild transaction states and resolve READY transactions if possible.

        If the Coordinator decision is not available, the READY transaction is
        left unresolved and must stay protected from log pruning.
        """

        latest_state_by_tx = self.log_manager.get_latest_state_by_tx()
        latest_gseq_by_tx = self.log_manager.get_latest_gseq_by_tx()
        global_tx_table = self._load_global_tx_table()

        in_doubt_tx_ids = sorted(
            tx_id
            for tx_id, state in latest_state_by_tx.items()
            if state == TxState.READY
        )

        decisions_applied: Dict[str, str] = {}
        unresolved_tx_ids = []
        acks_after_recovery: Dict[str, dict] = {}

        for tx_id in in_doubt_tx_ids:
            decision = self._get_global_decision(global_tx_table, tx_id)

            if decision is None:
                unresolved_tx_ids.append(tx_id)
                continue

            gseq = self._get_recovery_gseq(
                tx_id=tx_id,
                latest_gseq_by_tx=latest_gseq_by_tx,
                global_tx_table=global_tx_table,
            )

            self._write_final_decision(
                tx_id=tx_id,
                gseq=gseq,
                decision=decision,
            )

            latest_state_by_tx[tx_id] = decision
            decisions_applied[tx_id] = decision.value
            acks_after_recovery[tx_id] = self._build_recovery_ack(
                tx_id=tx_id,
                gseq=gseq,
                decision=decision,
            )

        remaining_in_doubt_tx_ids = sorted(
            tx_id
            for tx_id, state in latest_state_by_tx.items()
            if state == TxState.READY
        )

        self.log_manager.append_log(
            gseq=None,
            tx_id=None,
            role=self.role,
            state=TxState.END,
            event=LogEvent.RECOVERY,
            details={
                "message": "participant recovered from durable log",
                "global_tx_table_path": str(self.global_tx_table_path),
                "recovered_tx_count": len(latest_state_by_tx),
                "in_doubt_tx_ids": in_doubt_tx_ids,
                "decisions_applied": decisions_applied,
                "unresolved_tx_ids": unresolved_tx_ids,
                "remaining_in_doubt_tx_ids": remaining_in_doubt_tx_ids,
            },
        )

        return {
            "site": self.site_name,
            "recovered_tx_count": len(latest_state_by_tx),
            "in_doubt_tx_ids": in_doubt_tx_ids,
            "decisions_applied": decisions_applied,
            "resolved_tx_ids": sorted(decisions_applied),
            "unresolved_tx_ids": sorted(unresolved_tx_ids),
            "remaining_in_doubt_tx_ids": remaining_in_doubt_tx_ids,
            "acks_after_recovery": acks_after_recovery,
            "state_by_tx": {
                tx_id: state.value
                for tx_id, state in latest_state_by_tx.items()
            },
        }

    def _load_global_tx_table(self) -> Dict[str, Dict[str, Any]]:
        if not self.global_tx_table_path.exists():
            return {}

        with self.global_tx_table_path.open("r", encoding="utf-8") as file:
            try:
                data = json.load(file)
            except json.JSONDecodeError:
                return {}

        if not isinstance(data, dict):
            return {}

        return data

    def _get_global_decision(
        self,
        global_tx_table: Dict[str, Dict[str, Any]],
        tx_id: str,
    ) -> Optional[TxState]:
        record = global_tx_table.get(tx_id)

        if not record:
            return None

        decision = str(record.get("global_decision", "")).upper()

        if decision in {"COMMIT", "GLOBAL_COMMIT"}:
            return TxState.COMMIT

        if decision in {"ABORT", "GLOBAL_ABORT"}:
            return TxState.ABORT

        return None

    def _get_recovery_gseq(
        self,
        *,
        tx_id: str,
        latest_gseq_by_tx: Dict[str, int],
        global_tx_table: Dict[str, Dict[str, Any]],
    ) -> Optional[int]:
        if tx_id in latest_gseq_by_tx:
            return latest_gseq_by_tx[tx_id]

        record = global_tx_table.get(tx_id, {})
        gseq = record.get("gseq")

        if gseq is None:
            return None

        return int(gseq)

    def _write_final_decision(
        self,
        *,
        tx_id: str,
        gseq: Optional[int],
        decision: TxState,
    ) -> None:
        event = LogEvent.COMMIT if decision == TxState.COMMIT else LogEvent.ABORT

        self.log_manager.append_log(
            gseq=gseq,
            tx_id=tx_id,
            role=self.role,
            state=decision,
            event=event,
            details={
                "message": "final decision applied during recovery",
                "previous_state": TxState.READY.value,
                "decision_source": str(self.global_tx_table_path),
            },
        )

    def _build_recovery_ack(
        self,
        *,
        tx_id: str,
        gseq: Optional[int],
        decision: TxState,
    ) -> dict:
        return ProtocolMessage(
            message_type=MessageType.ACK,
            tx_id=tx_id,
            gseq=gseq,
            sender=self.site_name,
            receiver="Coordinator",
            payload={
                "state": decision.value,
                "message": "final decision applied during recovery",
            },
        ).to_dict()
