from __future__ import annotations

import json
import os
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Dict, Iterable, List, Optional, Set

from src.models import LogEvent, LogRecord, NodeRole, TxState


PROJECT_ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = PROJECT_ROOT / "logs"


class LogManager:
    """
    Durable JSONL log manager for one distributed site.

    Each site has one log file:
    - logs/coordinator.log
    - logs/nodeA.log
    - logs/nodeB.log
    - logs/nodeC.log

    Each line in the log file is one JSON object.
    """

    def __init__(self, site_name: str, log_dir: Path = LOG_DIR) -> None:
        self.site_name = site_name
        self.log_dir = log_dir
        self.log_dir.mkdir(parents=True, exist_ok=True)

        self.log_path = self.log_dir / f"{self.site_name}.log"
        self.log_path.touch(exist_ok=True)

        self._next_lsn_value = self._load_next_lsn()


    
    def get_log_size(self) -> int:
        """
        Return log file size in bytes.
        """
        if not self.log_path.exists():
            return 0
        return self.log_path.stat().st_size


    
    def _load_next_lsn(self) -> int:
        """
        Load the next LSN once when the LogManager starts.
        This avoids reading the entire log file before every append.
        """
        records = self.read_logs()

        if not records:
            return 1

        return max(int(record.get("lsn", 0)) for record in records) + 1
    
    def _next_lsn(self) -> int:
        """
        Return next local log sequence number.
        LSN is local to each site.
        """
        lsn = self._next_lsn_value
        self._next_lsn_value += 1
        return lsn

    def append_log(
        self,
        *,
        gseq: Optional[int],
        tx_id: Optional[str],
        role: NodeRole,
        state: TxState,
        event: LogEvent,
        details: Optional[dict] = None,
    ) -> LogRecord:
        """
        Append one durable log record to JSONL file.

        The file is flushed and fsynced so that the log survives process crashes.
        """

        record = LogRecord(
            lsn=self._next_lsn(),
            gseq=gseq,
            tx_id=tx_id,
            site=self.site_name,
            role=role,
            state=state,
            event=event,
            details=details or {},
        )

        with self.log_path.open("a", encoding="utf-8") as file:
            file.write(json.dumps(record.to_dict(), ensure_ascii=False) + "\n")
            file.flush()
            os.fsync(file.fileno())

        return record

    def read_logs(self) -> List[dict]:
        """
        Read all log records from the JSONL log file.
        Invalid empty lines are ignored.
        """
        if not self.log_path.exists():
            return []

        records: List[dict] = []

        with self.log_path.open("r", encoding="utf-8") as file:
            for line_number, line in enumerate(file, start=1):
                line = line.strip()

                if not line:
                    continue

                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError as exc:
                    raise ValueError(
                        f"Invalid JSON in {self.log_path} at line {line_number}: {line}"
                    ) from exc

        return records

    def clear(self) -> None:
        """
        Clear the log file.

        This is mainly useful for tests and demo resets.
        Do not use this during normal recovery.
        """
        self.log_path.write_text("", encoding="utf-8")
        self._next_lsn_value = 1

    def get_latest_state_by_tx(self) -> Dict[str, TxState]:
        """
        Return the latest known state of each transaction in this site's log.
        """
        latest: Dict[str, TxState] = {}

        for record in self.read_logs():
            tx_id = record.get("tx_id")
            state = record.get("state")

            if not tx_id or not state:
                continue

            latest[str(tx_id)] = TxState(state)

        return latest

    def get_latest_gseq_by_tx(self) -> Dict[str, int]:
        """
        Return the latest global sequence number of each transaction.
        """
        latest: Dict[str, int] = {}

        for record in self.read_logs():
            tx_id = record.get("tx_id")
            gseq = record.get("gseq")

            if not tx_id or gseq is None:
                continue

            latest[str(tx_id)] = int(gseq)

        return latest

    def build_checkpoint_summary(self) -> dict:
        """
        Build checkpoint summary by scanning the log once.

        This is useful for large logs because it avoids reading the same
        log file multiple times.
        """
        latest_state_by_tx: Dict[str, TxState] = {}
        latest_gseq_by_tx: Dict[str, int] = {}

        for record in self.read_logs():
            tx_id = record.get("tx_id")
            state = record.get("state")
            gseq = record.get("gseq")

            if not tx_id:
                continue

            tx_id = str(tx_id)

            if state:
                latest_state_by_tx[tx_id] = TxState(state)

            if gseq is not None:
                latest_gseq_by_tx[tx_id] = int(gseq)

        active_states = {
            TxState.INIT,
            TxState.WAIT,
            TxState.READY,
        }

        active_tx_ids = sorted(
            tx_id
            for tx_id, state in latest_state_by_tx.items()
            if state in active_states
        )

        in_doubt_tx_ids = sorted(
            tx_id
            for tx_id, state in latest_state_by_tx.items()
            if state == TxState.READY
        )

        last_checkpointed_gseq = (
            max(latest_gseq_by_tx.values()) if latest_gseq_by_tx else 0
        )
        terminal_gseqs = {
            latest_gseq_by_tx[tx_id]
            for tx_id, state in latest_state_by_tx.items()
            if tx_id in latest_gseq_by_tx
            and state in {TxState.COMMIT, TxState.ABORT, TxState.END}
        }

        return {
            "latest_state_by_tx": latest_state_by_tx,
            "latest_gseq_by_tx": latest_gseq_by_tx,
            "last_checkpointed_gseq": last_checkpointed_gseq,
            "terminal_gseqs": terminal_gseqs,
            "active_tx_ids": active_tx_ids,
            "in_doubt_tx_ids": in_doubt_tx_ids,
            "tx_count": len(latest_state_by_tx),
            "log_size_before": self.get_log_size(),
        }

    def get_in_doubt_tx_ids(self) -> List[str]:
        """
        Return transactions that are currently in READY state.

        READY means the participant has voted commit but does not know
        the global decision yet.
        """
        latest = self.get_latest_state_by_tx()

        return sorted(
            tx_id
            for tx_id, state in latest.items()
            if state == TxState.READY
        )

    def get_active_tx_ids(self) -> List[str]:
        """
        Return transactions that are not finished yet.

        For now, INIT, WAIT, and READY are considered active/not finalized.
        """
        latest = self.get_latest_state_by_tx()

        active_states = {
            TxState.INIT,
            TxState.WAIT,
            TxState.READY,
        }

        return sorted(
            tx_id
            for tx_id, state in latest.items()
            if state in active_states
        )

    def get_final_tx_ids(self) -> Set[str]:
            """
            Return transactions that have reached a final state.

            For participant logs, final states are COMMIT or ABORT.
            For coordinator logs, END is also final because the coordinator
            has completed the transaction and stored the global decision.
            """
            latest = self.get_latest_state_by_tx()

            return {
                tx_id
                for tx_id, state in latest.items()
                if state in {TxState.COMMIT, TxState.ABORT, TxState.END}
            }

    def prune_logs(
        self,
        *,
        global_safe_point: int,
        protected_tx_ids: Iterable[str],
    ) -> dict:
        """
        Prune old log records safely.

        A log record is prunable only if:
        - record.gseq <= global_safe_point
        - transaction is not protected
        - transaction final state is COMMIT or ABORT

        READY / in-doubt transactions are never pruned.
        """

        protected: Set[str] = set(protected_tx_ids)
        final_tx_ids = self.get_final_tx_ids()

        before_bytes = self.get_log_size()
        old_records = self.read_logs()
        kept_records: List[dict] = []
        pruned_count = 0

        for record in old_records:
            tx_id = record.get("tx_id")
            gseq = record.get("gseq")

            # Keep records without tx_id, such as system-level records.
            if not tx_id:
                kept_records.append(record)
                continue

            # Keep records without gseq because we cannot compare them safely.
            if gseq is None:
                kept_records.append(record)
                continue

            tx_id = str(tx_id)
            gseq = int(gseq)

            can_prune = (
                gseq <= global_safe_point
                and tx_id not in protected
                and tx_id in final_tx_ids
            )

            if can_prune:
                pruned_count += 1
            else:
                kept_records.append(record)

        self._rewrite_logs_atomically(kept_records)

        after_bytes = self.get_log_size()

        return {
            "site": self.site_name,
            "global_safe_point": global_safe_point,
            "protected_tx_count": len(protected),
            "before_bytes": before_bytes,
            "after_bytes": after_bytes,
            "saved_bytes": before_bytes - after_bytes,
            "saved_percent": round(
                ((before_bytes - after_bytes) / before_bytes) * 100, 2
            )
            if before_bytes > 0
            else 0.0,
            "pruned_records": pruned_count,
            "remaining_records": len(kept_records),
        }

    def _rewrite_logs_atomically(self, records: List[dict]) -> None:
        """
        Rewrite log file using a temporary file and atomic replace.

        This avoids leaving a half-written log file if the process fails
        during pruning.
        """

        with NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            delete=False,
            dir=self.log_dir,
            suffix=".tmp",
        ) as temp_file:
            temp_path = Path(temp_file.name)

            for record in records:
                temp_file.write(json.dumps(record, ensure_ascii=False) + "\n")

            temp_file.flush()
            os.fsync(temp_file.fileno())

        temp_path.replace(self.log_path)
        self._next_lsn_value = self._load_next_lsn()
