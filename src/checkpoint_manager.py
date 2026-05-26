from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Set

from src.models import utc_now_iso


PROJECT_ROOT = Path(__file__).resolve().parents[1]
METRICS_DIR = PROJECT_ROOT / "metrics"
SNAPSHOT_DIR = PROJECT_ROOT / "snapshots"


class GlobalCheckpointManager:
    """
    Manager for global checkpointing.

    It reads local checkpoint metadata from all participant sites,
    computes the global safe point, and writes a global checkpoint snapshot.
    """

    def __init__(self, sites: List[str] | None = None) -> None:
        self.sites = sites or ["NodeA", "NodeB", "NodeC"]

        METRICS_DIR.mkdir(parents=True, exist_ok=True)
        SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)

    def _load_local_checkpoint_summary(self, checkpoint_id: int) -> Dict[str, Any]:
        """
        Load metrics/local_checkpoint_<id>_summary.json.
        """
        path = METRICS_DIR / f"local_checkpoint_{checkpoint_id}_summary.json"

        if not path.exists():
            raise FileNotFoundError(
                f"Local checkpoint summary not found: {path}. "
                f"Run: python scripts/run_checkpoint_demo.py --checkpoint-id {checkpoint_id}"
            )

        with path.open("r", encoding="utf-8") as file:
            return json.load(file)

    def _validate_sites(self, local_sites: List[Dict[str, Any]]) -> None:
        """
        Ensure all required sites are present.
        """
        found_sites = {site["site"] for site in local_sites}
        required_sites = set(self.sites)

        missing = required_sites - found_sites

        if missing:
            raise ValueError(f"Missing local checkpoint metadata for sites: {sorted(missing)}")

    def _compute_global_safe_point(self, local_sites: List[Dict[str, Any]]) -> int:
        """
        The global safe point is the minimum checkpointed gseq across all sites.
        """
        if not local_sites:
            return 0

        return min(int(site["last_checkpointed_gseq"]) for site in local_sites)

    def _collect_protected_transactions(
        self,
        local_sites: List[Dict[str, Any]],
    ) -> Dict[str, List[str]]:
        """
        Protected transactions are active or in-doubt transactions.

        They must not be pruned even if their gseq is less than or equal
        to the global safe point.
        """
        active_tx_ids: Set[str] = set()
        in_doubt_tx_ids: Set[str] = set()

        for site in local_sites:
            active_tx_ids.update(site.get("active_tx_ids", []))
            in_doubt_tx_ids.update(site.get("in_doubt_tx_ids", []))

        protected_tx_ids = active_tx_ids | in_doubt_tx_ids

        return {
            "active_tx_ids": sorted(active_tx_ids),
            "in_doubt_tx_ids": sorted(in_doubt_tx_ids),
            "protected_tx_ids": sorted(protected_tx_ids),
        }

    def create_global_checkpoint(self, checkpoint_id: int) -> Dict[str, Any]:
        """
        Create global checkpoint from local checkpoint metadata.
        """
        local_summary = self._load_local_checkpoint_summary(checkpoint_id)
        local_sites = local_summary.get("sites", [])

        self._validate_sites(local_sites)

        global_safe_point = self._compute_global_safe_point(local_sites)
        protected = self._collect_protected_transactions(local_sites)

        site_safe_points = {
            site["site"]: int(site["last_checkpointed_gseq"])
            for site in local_sites
        }

        global_checkpoint = {
            "checkpoint_id": checkpoint_id,
            "type": "GLOBAL_CHECKPOINT",
            "timestamp": utc_now_iso(),
            "sites": self.sites,
            "site_safe_points": site_safe_points,
            "global_safe_point": global_safe_point,
            "active_tx_ids": protected["active_tx_ids"],
            "in_doubt_tx_ids": protected["in_doubt_tx_ids"],
            "protected_tx_ids": protected["protected_tx_ids"],
            "active_tx_count": len(protected["active_tx_ids"]),
            "in_doubt_tx_count": len(protected["in_doubt_tx_ids"]),
            "protected_tx_count": len(protected["protected_tx_ids"]),
            "local_checkpoint_summary": local_sites,
            "pruning_rule": (
                "A log record can be pruned only if "
                "gseq <= global_safe_point, "
                "transaction is COMMIT or ABORT, "
                "and transaction is not protected."
            ),
        }

        snapshot_path = SNAPSHOT_DIR / f"global_checkpoint_{checkpoint_id}.json"
        summary_path = METRICS_DIR / f"global_checkpoint_{checkpoint_id}_summary.json"

        with snapshot_path.open("w", encoding="utf-8") as file:
            json.dump(global_checkpoint, file, indent=2, ensure_ascii=False)

        with summary_path.open("w", encoding="utf-8") as file:
            json.dump(global_checkpoint, file, indent=2, ensure_ascii=False)

        return {
            "checkpoint_id": checkpoint_id,
            "global_safe_point": global_safe_point,
            "site_safe_points": site_safe_points,
            "active_tx_count": len(protected["active_tx_ids"]),
            "in_doubt_tx_count": len(protected["in_doubt_tx_ids"]),
            "protected_tx_count": len(protected["protected_tx_ids"]),
            "snapshot_path": str(snapshot_path),
            "summary_path": str(summary_path),
        }