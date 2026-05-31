from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.models import utc_now_iso
from src.recovery_manager import GLOBAL_TX_TABLE_PATH, RecoveryManager


METRICS_DIR = PROJECT_ROOT / "metrics"
DEFAULT_SITES = ["NodeA", "NodeB", "NodeC"]


def summarize_recovery(result: Dict[str, Any]) -> Dict[str, Any]:
    decisions_applied = result["decisions_applied"]
    acks_after_recovery = result["acks_after_recovery"]

    return {
        "site": result["site"],
        "recovered_tx_count": result["recovered_tx_count"],
        "in_doubt_before_count": len(result["in_doubt_tx_ids"]),
        "in_doubt_tx_ids": result["in_doubt_tx_ids"],
        "resolved_count": len(result["resolved_tx_ids"]),
        "resolved_tx_ids": result["resolved_tx_ids"],
        "unresolved_count": len(result["unresolved_tx_ids"]),
        "unresolved_tx_ids": result["unresolved_tx_ids"],
        "remaining_in_doubt_count": len(result["remaining_in_doubt_tx_ids"]),
        "remaining_in_doubt_tx_ids": result["remaining_in_doubt_tx_ids"],
        "decisions_applied": decisions_applied,
        "acks_after_recovery_count": len(acks_after_recovery),
    }


def run_recovery(sites: List[str], fail_on_unresolved: bool) -> Dict[str, Any]:
    if not GLOBAL_TX_TABLE_PATH.exists():
        raise FileNotFoundError(
            f"Global transaction table not found: {GLOBAL_TX_TABLE_PATH}. "
            "Run workload before recovery."
        )

    METRICS_DIR.mkdir(parents=True, exist_ok=True)

    site_results = []

    for site in sites:
        result = RecoveryManager(site).recover()
        site_summary = summarize_recovery(result)
        site_results.append(site_summary)

        print(
            f"{site}: in_doubt_before={site_summary['in_doubt_before_count']}, "
            f"resolved={site_summary['resolved_count']}, "
            f"unresolved={site_summary['unresolved_count']}, "
            f"remaining_in_doubt={site_summary['remaining_in_doubt_count']}"
        )

    total_in_doubt_before = sum(
        site["in_doubt_before_count"] for site in site_results
    )
    total_resolved = sum(site["resolved_count"] for site in site_results)
    total_unresolved = sum(site["unresolved_count"] for site in site_results)
    total_remaining_in_doubt = sum(
        site["remaining_in_doubt_count"] for site in site_results
    )

    summary = {
        "type": "RECOVERY_SUMMARY",
        "timestamp": utc_now_iso(),
        "global_tx_table_path": str(GLOBAL_TX_TABLE_PATH),
        "sites": site_results,
        "total_in_doubt_before": total_in_doubt_before,
        "total_resolved": total_resolved,
        "total_unresolved": total_unresolved,
        "total_remaining_in_doubt": total_remaining_in_doubt,
    }

    summary_path = METRICS_DIR / "recovery_summary.json"

    with summary_path.open("w", encoding="utf-8") as file:
        json.dump(summary, file, indent=2, ensure_ascii=False)

    print("\nRecovery completed.")
    print(f"Total in-doubt before recovery : {total_in_doubt_before}")
    print(f"Total resolved                : {total_resolved}")
    print(f"Total unresolved              : {total_unresolved}")
    print(f"Total remaining in-doubt      : {total_remaining_in_doubt}")
    print(f"Summary saved to              : {summary_path}")

    if fail_on_unresolved and total_remaining_in_doubt > 0:
        raise RuntimeError("Recovery left unresolved in-doubt transactions.")

    return summary


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Recover all participant sites from durable logs."
    )
    parser.add_argument(
        "--sites",
        nargs="+",
        default=DEFAULT_SITES,
        help="Participant sites to recover.",
    )
    parser.add_argument(
        "--fail-on-unresolved",
        action="store_true",
        help="Exit with an error if any in-doubt transaction remains unresolved.",
    )

    args = parser.parse_args()

    run_recovery(
        sites=args.sites,
        fail_on_unresolved=args.fail_on_unresolved,
    )


if __name__ == "__main__":
    main()
