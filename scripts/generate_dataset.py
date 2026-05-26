from __future__ import annotations

import argparse
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"


SYMBOLS: Dict[str, float] = {
    "AAPL": 187.25,
    "MSFT": 420.50,
    "GOOGL": 175.10,
    "AMZN": 185.70,
    "TSLA": 175.80,
    "NVDA": 920.40,
    "META": 510.25,
    "NFLX": 625.30,
    "AMD": 162.45,
    "INTC": 34.20,
}


def generate_account_id(index: int) -> str:
    """
    Generate account id like ACC0001.
    """
    return f"ACC{index:04d}"


def generate_tx_id(index: int) -> str:
    """
    Generate transaction id like TX000001.
    """
    return f"TX{index:06d}"


def generate_transaction(index: int, base_time: datetime) -> dict:
    """
    Generate one high-frequency trading transaction.
    """

    symbol = random.choice(list(SYMBOLS.keys()))
    base_price = SYMBOLS[symbol]

    # Small price fluctuation around the base price.
    price_change_percent = random.uniform(-0.015, 0.015)
    price = round(base_price * (1 + price_change_percent), 2)

    # Simulate many accounts.
    account_number = random.randint(1, 1000)

    # High-frequency trading often has small to medium order quantities.
    quantity = random.choice([10, 20, 50, 100, 200, 500, 1000])

    # BUY or SELL.
    side = random.choice(["BUY", "SELL"])

    # Very close timestamps to simulate high-frequency trading.
    timestamp = base_time + timedelta(microseconds=index * random.randint(50, 500))

    return {
        "tx_id": generate_tx_id(index),
        "account_id": generate_account_id(account_number),
        "symbol": symbol,
        "side": side,
        "quantity": quantity,
        "price": price,
        "timestamp": timestamp.isoformat(),
    }


def write_jsonl(records: List[dict], output_path: Path) -> None:
    """
    Write records to JSONL file.
    Each line is one JSON object.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8") as file:
        for record in records:
            file.write(json.dumps(record, ensure_ascii=False) + "\n")


def write_summary(records: List[dict], summary_path: Path) -> None:
    """
    Write a small summary file for quick verification.
    """
    symbol_counts: Dict[str, int] = {}
    side_counts: Dict[str, int] = {"BUY": 0, "SELL": 0}

    total_notional = 0.0

    for record in records:
        symbol = record["symbol"]
        side = record["side"]

        symbol_counts[symbol] = symbol_counts.get(symbol, 0) + 1
        side_counts[side] = side_counts.get(side, 0) + 1
        total_notional += record["quantity"] * record["price"]

    summary = {
        "total_records": len(records),
        "symbol_counts": symbol_counts,
        "side_counts": side_counts,
        "total_notional": round(total_notional, 2),
        "first_tx_id": records[0]["tx_id"] if records else None,
        "last_tx_id": records[-1]["tx_id"] if records else None,
    }

    with summary_path.open("w", encoding="utf-8") as file:
        json.dump(summary, file, indent=2, ensure_ascii=False)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate high-frequency trading transaction dataset."
    )

    parser.add_argument(
        "--records",
        type=int,
        default=100000,
        help="Number of transaction records to generate.",
    )

    parser.add_argument(
        "--output",
        type=str,
        default=str(DATA_DIR / "transactions_100k.jsonl"),
        help="Output JSONL file path.",
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducible dataset.",
    )

    args = parser.parse_args()

    if args.records <= 0:
        raise ValueError("Number of records must be greater than 0.")

    random.seed(args.seed)

    output_path = Path(args.output)
    summary_path = DATA_DIR / "dataset_summary.json"

    base_time = datetime(2026, 5, 16, 10, 0, 0, tzinfo=timezone.utc)

    print(f"Generating {args.records:,} transaction records...")

    records = [
        generate_transaction(index=i, base_time=base_time)
        for i in range(1, args.records + 1)
    ]

    write_jsonl(records, output_path)
    write_summary(records, summary_path)

    print("Dataset generated successfully.")
    print(f"Output file : {output_path}")
    print(f"Summary file: {summary_path}")
    print(f"First tx    : {records[0]}")
    print(f"Last tx     : {records[-1]}")


if __name__ == "__main__":
    main()