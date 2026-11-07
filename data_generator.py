import argparse
import csv
import logging
import os
import random
import time
import uuid
from collections import Counter
from datetime import datetime
from pathlib import Path

logger = logging.getLogger("data_generator")

DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parent / "data" / "input"

FIELDNAMES = [
    "event_id", "event_type", "user_id", "product_id", "product_name",
    "category", "price", "quantity", "event_timestamp",
]

EVENT_TYPES = ["view", "add_to_cart", "purchase"]
EVENT_WEIGHTS = [0.70, 0.20, 0.10]  # most visitors browse, few buy

# category -> (product names, price range in USD)
CATALOG_SPEC = {
    "Electronics": (["Wireless Earbuds", "Bluetooth Speaker", "Smartwatch",
                     "USB-C Charger", "Laptop Stand", "Mechanical Keyboard"], (15, 300)),
    "Clothing": (["Denim Jacket", "Running Shoes", "Cotton T-Shirt",
                  "Wool Sweater", "Baseball Cap"], (10, 120)),
    "Home & Kitchen": (["Coffee Maker", "Blender", "Non-stick Pan",
                        "Desk Lamp", "Throw Pillow"], (12, 150)),
    "Books": (["Data Engineering Guide", "Python Cookbook",
               "Mystery Novel", "Travel Journal"], (8, 45)),
    "Sports": (["Yoga Mat", "Dumbbell Set", "Water Bottle",
                "Resistance Bands"], (10, 90)),
}

CORRUPTION_TYPES = [
    "messy_event_type",    # fixable: "  VIEW " -> "view"
    "missing_price",       # reject
    "negative_quantity",   # reject
    "invalid_event_type",  # reject
    "bad_timestamp",       # reject
    "missing_user_id",     # reject
]


def build_catalog(rng: random.Random) -> list[dict]:
    """Create a fixed product catalog so each product_id always has the same name and price."""
    catalog = []
    product_id = 1001
    for category, (names, (low, high)) in CATALOG_SPEC.items():
        for name in names:
            catalog.append({
                "product_id": product_id,
                "product_name": name,
                "category": category,
                "price": round(rng.uniform(low, high), 2),
            })
            product_id += 1
    return catalog


def make_event(rng: random.Random, catalog: list[dict]) -> dict:
    """Create one valid event."""
    product = rng.choice(catalog)
    event_type = rng.choices(EVENT_TYPES, weights=EVENT_WEIGHTS)[0]
    quantity = 1 if event_type == "view" else rng.randint(1, 3)
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "user_id": rng.randint(1, 500),
        **product,
        "quantity": quantity,
        "event_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
    }


def corrupt_event(event: dict, rng: random.Random) -> tuple[dict, str]:
    """Introduce one data quality problem into an event."""
    bad = dict(event)
    kind = rng.choice(CORRUPTION_TYPES)
    if kind == "messy_event_type":
        bad["event_type"] = f"  {bad['event_type'].upper()} "
    elif kind == "missing_price":
        bad["price"] = ""
    elif kind == "negative_quantity":
        bad["quantity"] = -rng.randint(1, 3)
    elif kind == "invalid_event_type":
        bad["event_type"] = "click"
    elif kind == "bad_timestamp":
        bad["event_timestamp"] = "not-a-date"
    elif kind == "missing_user_id":
        bad["user_id"] = ""
    return bad, kind


def generate_batch(
    rng: random.Random, catalog: list[dict], n_events: int, bad_rate: float, dup_rate: float
) -> tuple[list[dict], Counter]:
    """Generate one file's worth of events, plus counts of injected problems."""
    events = []
    injected = Counter()
    for _ in range(n_events):
        event = make_event(rng, catalog)
        if rng.random() < bad_rate:
            event, kind = corrupt_event(event, rng)
            injected[kind] += 1
        events.append(event)

    if events and rng.random() < dup_rate:
        events.append(dict(rng.choice(events)))  # exact duplicate row
        injected["duplicate"] += 1

    return events, injected


def write_csv_atomically(events: list[dict], output_dir: Path, file_index: int) -> Path:
    """Write to a hidden temp file, then rename, so readers never see a partial file."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    final_path = output_dir / f"events_{timestamp}_{file_index:05d}.csv"
    temp_path = output_dir / f".{final_path.name}.tmp"

    with open(temp_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(events)

    os.replace(temp_path, final_path)  # atomic on the same drive
    return final_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate fake e-commerce event CSV files.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--interval", type=float, default=5.0, help="Seconds between files")
    parser.add_argument("--events-per-file", type=int, default=100)
    parser.add_argument("--num-files", type=int, default=0, help="0 = run until stopped")
    parser.add_argument("--bad-rate", type=float, default=0.02, help="Share of corrupted rows")
    parser.add_argument("--dup-rate", type=float, default=0.10, help="Chance a file contains a duplicate")
    parser.add_argument("--seed", type=int, default=None, help="Seed for reproducible catalog and events")
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()

    rng = random.Random(args.seed)
    catalog = build_catalog(rng)
    args.output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Writing to %s every %.1fs (%d events/file)", args.output_dir, args.interval, args.events_per_file)

    files_written = 0
    total_events = 0
    total_injected = Counter()
    try:
        while args.num_files == 0 or files_written < args.num_files:
            events, injected = generate_batch(rng, catalog, args.events_per_file, args.bad_rate, args.dup_rate)
            path = write_csv_atomically(events, args.output_dir, files_written)

            files_written += 1
            total_events += len(events)
            total_injected.update(injected)

            note = f", injected: {dict(injected)}" if injected else ""
            logger.info("Wrote %s (%d rows%s)", path.name, len(events), note)

            if args.num_files == 0 or files_written < args.num_files:
                time.sleep(args.interval)
    except KeyboardInterrupt:
        logger.info("Stopped by user")
    finally:
        logger.info("Summary: %d files, %d rows, injected problems: %s",
                    files_written, total_events, dict(total_injected) or "none")


if __name__ == "__main__":
    main()