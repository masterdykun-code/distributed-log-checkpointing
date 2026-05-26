from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import List

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.node import ParticipantNode


SNAPSHOT_DIR = PROJECT_ROOT / "snapshots"
METRICS_DIR = PROJECT_ROOT / "metrics"


def create_local_checkpoints(checkpoint_id: int, sites: List[str]) -> list[dict]:
    """
    Create local checkpoint for each participant node.
    """
    results: list[dict] = []

    for site in sites:
        print(f"Creating local checkpoint for {site}...")

        node = ParticipantNode(site)
        metadata = node.create_checkpoint(checkpoint_id)

        result = metadata.to_dict()
        result["active_tx_count"] = len(metadata.active_tx_ids)
        result["in_doubt_tx_count"] = len(metadata.in_doubt_tx_ids)

        results.append(result)

        print(
            f"{site}: last_checkpointed_gseq={metadata.last_checkpointed_gseq}, "
            f"active={len(metadata.active_tx_ids)}, "
            f"in_doubt={len(metadata.in_doubt_tx_ids)}, "
            f"log_size_before={metadata.log_size_before} bytes"
        )

    return results


def write_checkpoint_summary(checkpoint_id: int, results: list[dict]) -> Path:
    """
    Write local checkpoint summary to metrics folder.
    """
    METRICS_DIR.mkdir(parents=True, exist_ok=True)

    summary = {
        "checkpoint_id": checkpoint_id,
        "sites": results,
    }

    output_path = METRICS_DIR / f"local_checkpoint_{checkpoint_id}_summary.json"

    with output_path.open("w", encoding="utf-8") as file:
        json.dump(summary, file, indent=2, ensure_ascii=False)

    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create local checkpoints for participant nodes."
    )

    parser.add_argument(
        "--checkpoint-id",
        type=int,
        default=1,
        help="Checkpoint id.",
    )

    args = parser.parse_args()

    if args.checkpoint_id <= 0:
        raise ValueError("--checkpoint-id must be greater than 0.")

    sites = ["NodeA", "NodeB", "NodeC"]

    results = create_local_checkpoints(
        checkpoint_id=args.checkpoint_id,
        sites=sites,
    )

    output_path = write_checkpoint_summary(
        checkpoint_id=args.checkpoint_id,
        results=results,
    )

    print("\nLocal checkpoint completed.")
    print(f"Summary saved to: {output_path}")


if __name__ == "__main__":
    main()