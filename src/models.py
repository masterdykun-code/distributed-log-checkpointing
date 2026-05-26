from __future__ import annotations

from dataclasses import dataclass, field, asdict
from enum import Enum
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


class TxState(str, Enum):
    """
    Transaction states used in the 2PC protocol.

    INIT:
        The transaction has just started.

    WAIT:
        Coordinator has sent PREPARE and is waiting for participant votes.

    READY:
        Participant has voted COMMIT and is waiting for the global decision.

    COMMIT:
        The transaction has been committed.

    ABORT:
        The transaction has been aborted.

    END:
        Coordinator has completed the transaction after receiving ACKs.
    """

    INIT = "INIT"
    WAIT = "WAIT"
    READY = "READY"
    COMMIT = "COMMIT"
    ABORT = "ABORT"
    END = "END"


class NodeRole(str, Enum):
    COORDINATOR = "COORDINATOR"
    PARTICIPANT = "PARTICIPANT"


class Vote(str, Enum):
    COMMIT = "VOTE_COMMIT"
    ABORT = "VOTE_ABORT"


class MessageType(str, Enum):
    """
    Message types exchanged between Coordinator and Nodes.
    """

    PREPARE = "PREPARE"
    VOTE_COMMIT = "VOTE_COMMIT"
    VOTE_ABORT = "VOTE_ABORT"
    GLOBAL_COMMIT = "GLOBAL_COMMIT"
    GLOBAL_ABORT = "GLOBAL_ABORT"
    ACK = "ACK"

    CHECKPOINT_REQUEST = "CHECKPOINT_REQUEST"
    CHECKPOINT_RESPONSE = "CHECKPOINT_RESPONSE"
    PRUNE_REQUEST = "PRUNE_REQUEST"
    PRUNE_RESPONSE = "PRUNE_RESPONSE"

    RECOVERY_REQUEST = "RECOVERY_REQUEST"
    RECOVERY_RESPONSE = "RECOVERY_RESPONSE"

    SHUTDOWN = "SHUTDOWN"


class LogEvent(str, Enum):
    """
    Durable log event names.
    """

    BEGIN_COMMIT = "BEGIN_COMMIT"
    PREPARE = "PREPARE"
    READY = "READY"
    VOTE_COMMIT = "VOTE_COMMIT"
    VOTE_ABORT = "VOTE_ABORT"
    GLOBAL_COMMIT = "GLOBAL_COMMIT"
    GLOBAL_ABORT = "GLOBAL_ABORT"
    COMMIT = "COMMIT"
    ABORT = "ABORT"
    ACK = "ACK"
    END_OF_TRANSACTION = "END_OF_TRANSACTION"

    CHECKPOINT = "CHECKPOINT"
    PRUNE = "PRUNE"
    RECOVERY = "RECOVERY"


def utc_now_iso() -> str:
    """
    Return current UTC time in ISO format.
    """
    return datetime.now(timezone.utc).isoformat()


@dataclass
class TradingTransaction:
    """
    A generated high-frequency trading transaction.
    """

    tx_id: str
    account_id: str
    symbol: str
    side: str
    quantity: int
    price: float
    timestamp: str

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TradingTransaction":
        return cls(
            tx_id=str(data["tx_id"]),
            account_id=str(data["account_id"]),
            symbol=str(data["symbol"]),
            side=str(data["side"]),
            quantity=int(data["quantity"]),
            price=float(data["price"]),
            timestamp=str(data["timestamp"]),
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ProtocolMessage:
    """
    Message exchanged between Coordinator and Participant processes.
    """

    message_type: MessageType
    tx_id: Optional[str] = None
    gseq: Optional[int] = None
    sender: Optional[str] = None
    receiver: Optional[str] = None
    payload: Dict[str, Any] = field(default_factory=dict)
    timestamp: str = field(default_factory=utc_now_iso)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "message_type": self.message_type.value,
            "tx_id": self.tx_id,
            "gseq": self.gseq,
            "sender": self.sender,
            "receiver": self.receiver,
            "payload": self.payload,
            "timestamp": self.timestamp,
        }


@dataclass
class LogRecord:
    """
    One durable log record.

    Each site has its own local LSN.
    gseq is used as a global sequence number assigned by the Coordinator.
    """

    lsn: int
    gseq: Optional[int]
    tx_id: Optional[str]
    site: str
    role: NodeRole
    state: TxState
    event: LogEvent
    timestamp: str = field(default_factory=utc_now_iso)
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "lsn": self.lsn,
            "gseq": self.gseq,
            "tx_id": self.tx_id,
            "site": self.site,
            "role": self.role.value,
            "state": self.state.value,
            "event": self.event.value,
            "timestamp": self.timestamp,
            "details": self.details,
        }


@dataclass
class CheckpointMetadata:
    """
    Metadata returned by each site after creating a local checkpoint.
    """

    checkpoint_id: int
    site: str
    last_checkpointed_gseq: int
    active_tx_ids: List[str] = field(default_factory=list)
    in_doubt_tx_ids: List[str] = field(default_factory=list)
    log_size_before: int = 0
    timestamp: str = field(default_factory=utc_now_iso)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "checkpoint_id": self.checkpoint_id,
            "site": self.site,
            "last_checkpointed_gseq": self.last_checkpointed_gseq,
            "active_tx_ids": self.active_tx_ids,
            "in_doubt_tx_ids": self.in_doubt_tx_ids,
            "log_size_before": self.log_size_before,
            "timestamp": self.timestamp,
        }


@dataclass
class PruneResult:
    """
    Result after a site prunes its log.
    """

    checkpoint_id: int
    site: str
    global_safe_point: int
    protected_tx_count: int
    before_bytes: int
    after_bytes: int

    @property
    def saved_bytes(self) -> int:
        return self.before_bytes - self.after_bytes

    @property
    def saved_percent(self) -> float:
        if self.before_bytes == 0:
            return 0.0
        return round((self.saved_bytes / self.before_bytes) * 100, 2)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "checkpoint_id": self.checkpoint_id,
            "site": self.site,
            "global_safe_point": self.global_safe_point,
            "protected_tx_count": self.protected_tx_count,
            "before_bytes": self.before_bytes,
            "after_bytes": self.after_bytes,
            "saved_bytes": self.saved_bytes,
            "saved_percent": self.saved_percent,
        }


TERMINAL_STATES = {
    TxState.COMMIT,
    TxState.ABORT,
    TxState.END,
}


def is_terminal_state(state: TxState) -> bool:
    """
    Return True if a transaction no longer needs normal processing.
    """
    return state in TERMINAL_STATES


def is_in_doubt_state(state: TxState) -> bool:
    """
    READY means the participant is in-doubt:
    it has voted commit but does not know the final global decision yet.
    """
    return state == TxState.READY


def can_participant_move(current: TxState, target: TxState) -> bool:
    """
    Validate participant state transitions.

    Valid participant transitions:
    INIT  -> READY
    INIT  -> ABORT
    READY -> COMMIT
    READY -> ABORT
    """

    valid_transitions = {
        TxState.INIT: {TxState.READY, TxState.ABORT},
        TxState.READY: {TxState.COMMIT, TxState.ABORT},
        TxState.COMMIT: set(),
        TxState.ABORT: set(),
        TxState.END: set(),
        TxState.WAIT: set(),
    }

    return target in valid_transitions.get(current, set())


def can_coordinator_move(current: TxState, target: TxState) -> bool:
    """
    Validate coordinator state transitions.

    Valid coordinator transitions:
    INIT   -> WAIT
    WAIT   -> COMMIT
    WAIT   -> ABORT
    COMMIT -> END
    ABORT  -> END
    """

    valid_transitions = {
        TxState.INIT: {TxState.WAIT},
        TxState.WAIT: {TxState.COMMIT, TxState.ABORT},
        TxState.COMMIT: {TxState.END},
        TxState.ABORT: {TxState.END},
        TxState.END: set(),
        TxState.READY: set(),
    }

    return target in valid_transitions.get(current, set())