from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from src.checkpoint_manager import GlobalCheckpointManager
from src.node import ParticipantNode


class CheckpointingTests(unittest.TestCase):
    def test_local_checkpoint_high_watermark_does_not_decrease_after_pruning(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            log_dir = root / "logs"
            snapshot_dir = root / "snapshots"
            node = ParticipantNode(
                "NodeA",
                min_delay=0.0,
                max_delay=0.0,
                log_dir=log_dir,
                snapshot_dir=snapshot_dir,
            )

            for gseq in range(1, 4):
                transaction = {
                    "tx_id": f"TX{gseq:06d}",
                    "account_id": "ACC0001",
                    "symbol": "AAPL",
                    "side": "BUY",
                    "quantity": 10,
                    "price": 100.0,
                    "timestamp": "test",
                }
                node.handle_prepare(transaction, gseq, can_commit=True)
                node.handle_global_commit(transaction["tx_id"], gseq)

            first = node.create_checkpoint(1)
            self.assertEqual(first.last_checkpointed_gseq, 3)

            node.log_manager.prune_logs(
                global_safe_point=3,
                protected_tx_ids=[],
            )
            second = node.create_checkpoint(2)

            self.assertEqual(second.last_checkpointed_gseq, 3)

            node.clear_logs()
            after_reset = node.create_checkpoint(3)
            self.assertEqual(after_reset.last_checkpointed_gseq, 0)

    def test_global_safe_point_uses_minimum_local_high_watermark(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            metrics_dir = root / "metrics"
            snapshot_dir = root / "snapshots"
            metrics_dir.mkdir()

            local_summary = {
                "checkpoint_id": 1,
                "sites": [
                    {
                        "site": "NodeA",
                        "last_checkpointed_gseq": 1000,
                        "active_tx_ids": [],
                        "in_doubt_tx_ids": [],
                    },
                    {
                        "site": "NodeB",
                        "last_checkpointed_gseq": 850,
                        "active_tx_ids": [],
                        "in_doubt_tx_ids": [],
                    },
                    {
                        "site": "NodeC",
                        "last_checkpointed_gseq": 1000,
                        "active_tx_ids": [],
                        "in_doubt_tx_ids": [],
                    },
                ],
            }
            summary_path = metrics_dir / "local_checkpoint_1_summary.json"
            summary_path.write_text(
                json.dumps(local_summary),
                encoding="utf-8",
            )

            result = GlobalCheckpointManager(
                metrics_dir=metrics_dir,
                snapshot_dir=snapshot_dir,
            ).create_global_checkpoint(1)

            self.assertEqual(result["global_safe_point"], 850)

    def test_local_checkpoint_stops_before_non_final_gap(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            node = ParticipantNode(
                "NodeA",
                min_delay=0.0,
                max_delay=0.0,
                log_dir=root / "logs",
                snapshot_dir=root / "snapshots",
            )

            transactions = {
                gseq: {
                    "tx_id": f"TX{gseq:06d}",
                    "account_id": "ACC0001",
                    "symbol": "AAPL",
                    "side": "BUY",
                    "quantity": 10,
                    "price": 100.0,
                    "timestamp": "test",
                }
                for gseq in range(1, 4)
            }

            node.handle_prepare(transactions[1], 1, can_commit=True)
            node.handle_global_commit(transactions[1]["tx_id"], 1)
            node.handle_prepare(transactions[2], 2, can_commit=True)
            node.handle_prepare(transactions[3], 3, can_commit=True)
            node.handle_global_commit(transactions[3]["tx_id"], 3)

            first = node.create_checkpoint(1)

            self.assertEqual(first.last_checkpointed_gseq, 1)
            self.assertEqual(first.in_doubt_tx_ids, ["TX000002"])

            node.handle_global_abort(transactions[2]["tx_id"], 2)
            second = node.create_checkpoint(2)

            self.assertEqual(second.last_checkpointed_gseq, 3)


if __name__ == "__main__":
    unittest.main()
