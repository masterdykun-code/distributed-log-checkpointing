from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.checkpoint_manager import GlobalCheckpointManager


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create global checkpoint and compute global safe point."
    )

    parser.add_argument(
        "--checkpoint-id",
        type=int,
        default=1,
        help="Checkpoint id to use.",
    )

    args = parser.parse_args()

    if args.checkpoint_id <= 0:
        raise ValueError("--checkpoint-id must be greater than 0.")

    manager = GlobalCheckpointManager()
    result = manager.create_global_checkpoint(args.checkpoint_id)

    print("Global checkpoint completed.")
    print(json.dumps(result, indent=2, ensure_ascii=False))

    print("\nImportant result:")
    print(f"global_safe_point = {result['global_safe_point']}")
    print(f"protected_tx_count = {result['protected_tx_count']}")
    print(f"active_tx_count = {result['active_tx_count']}")
    print(f"in_doubt_tx_count = {result['in_doubt_tx_count']}")


if __name__ == "__main__":
    main()