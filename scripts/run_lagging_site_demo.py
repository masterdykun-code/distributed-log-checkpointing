from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Dict

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.checkpoint_manager import GlobalCheckpointManager
from src.models import utc_now_iso


METRICS_DIR = PROJECT_ROOT / "metrics"


def write_lagging_local_checkpoint_summary(
    *,
    checkpoint_id: int,
    site_safe_points: Dict[str, int],
    lagging_reason: str,
) -> Path:
    """
    Write a synthetic local checkpoint summary for safe point analysis.

    This demo intentionally gives each site a different checkpointed gseq
    to show why the global safe point must be the minimum across sites.
    """

    METRICS_DIR.mkdir(parents=True, exist_ok=True)

    sites = []

    for site, last_checkpointed_gseq in site_safe_points.items():
        sites.append(
            {
                "checkpoint_id": checkpoint_id,
                "site": site,
                "last_checkpointed_gseq": int(last_checkpointed_gseq),
                "active_tx_ids": [],
                "in_doubt_tx_ids": [],
                "log_size_before": 0,
                "timestamp": utc_now_iso(),
                "active_tx_count": 0,
                "in_doubt_tx_count": 0,
                "demo_note": lagging_reason,
            }
        )

    summary = {
        "checkpoint_id": checkpoint_id,
        "type": "LAGGING_SITE_LOCAL_CHECKPOINT_DEMO",
        "description": (
            "Synthetic local checkpoint metadata used to demonstrate that "
            "global_safe_point is the minimum checkpointed gseq across sites."
        ),
        "sites": sites,
    }

    output_path = METRICS_DIR / f"local_checkpoint_{checkpoint_id}_summary.json"

    with output_path.open("w", encoding="utf-8") as file:
        json.dump(summary, file, indent=2, ensure_ascii=False)

    return output_path


def write_demo_summary(
    *,
    checkpoint_id: int,
    site_safe_points: Dict[str, int],
    global_result: dict,
    local_summary_path: Path,
    lagging_reason: str,
) -> Path:
    expected_safe_point = min(site_safe_points.values())
    lagging_sites = [
        site
        for site, safe_point in site_safe_points.items()
        if safe_point == expected_safe_point
    ]

    summary = {
        "checkpoint_id": checkpoint_id,
        "scenario": "lagging_site_safe_point",
        "lagging_reason": lagging_reason,
        "site_safe_points": site_safe_points,
        "expected_global_safe_point": expected_safe_point,
        "actual_global_safe_point": global_result["global_safe_point"],
        "lagging_sites": lagging_sites,
        "explanation": (
            "The system can safely prune logs only up to the slowest site's "
            "checkpointed gseq. Logs after this point may still be needed by "
            "that site during recovery."
        ),
        "local_summary_path": str(local_summary_path),
        "global_checkpoint_summary_path": global_result["summary_path"],
        "global_checkpoint_snapshot_path": global_result["snapshot_path"],
    }

    output_path = METRICS_DIR / f"lagging_site_checkpoint_{checkpoint_id}_summary.json"

    with output_path.open("w", encoding="utf-8") as file:
        json.dump(summary, file, indent=2, ensure_ascii=False)

    return output_path


def run_demo(
    *,
    checkpoint_id: int,
    node_a_gseq: int,
    node_b_gseq: int,
    node_c_gseq: int,
    lagging_reason: str,
) -> dict:
    site_safe_points = {
        "NodeA": node_a_gseq,
        "NodeB": node_b_gseq,
        "NodeC": node_c_gseq,
    }

    local_summary_path = write_lagging_local_checkpoint_summary(
        checkpoint_id=checkpoint_id,
        site_safe_points=site_safe_points,
        lagging_reason=lagging_reason,
    )

    global_result = GlobalCheckpointManager().create_global_checkpoint(checkpoint_id)

    demo_summary_path = write_demo_summary(
        checkpoint_id=checkpoint_id,
        site_safe_points=site_safe_points,
        global_result=global_result,
        local_summary_path=local_summary_path,
        lagging_reason=lagging_reason,
    )

    result = {
        "checkpoint_id": checkpoint_id,
        "site_safe_points": site_safe_points,
        "global_safe_point": global_result["global_safe_point"],
        "lagging_site": min(site_safe_points, key=site_safe_points.get),
        "local_summary_path": str(local_summary_path),
        "global_summary_path": global_result["summary_path"],
        "demo_summary_path": str(demo_summary_path),
    }

    print("Lagging site safe point demo completed.")
    print(json.dumps(result, indent=2, ensure_ascii=False))

    return result


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Demonstrate global safe point when one site lags behind."
    )
    parser.add_argument("--checkpoint-id", type=int, default=200)
    parser.add_argument("--node-a-gseq", type=int, default=1200)
    parser.add_argument("--node-b-gseq", type=int, default=1170)
    parser.add_argument("--node-c-gseq", type=int, default=1195)
    parser.add_argument(
        "--reason",
        type=str,
        default="NodeB is slower because of processing delay or message delay.",
    )

    args = parser.parse_args()

    if args.checkpoint_id <= 0:
        raise ValueError("--checkpoint-id must be greater than 0.")

    for name, value in {
        "node-a-gseq": args.node_a_gseq,
        "node-b-gseq": args.node_b_gseq,
        "node-c-gseq": args.node_c_gseq,
    }.items():
        if value < 0:
            raise ValueError(f"--{name} must be non-negative.")

    run_demo(
        checkpoint_id=args.checkpoint_id,
        node_a_gseq=args.node_a_gseq,
        node_b_gseq=args.node_b_gseq,
        node_c_gseq=args.node_c_gseq,
        lagging_reason=args.reason,
    )


if __name__ == "__main__":
    main()
