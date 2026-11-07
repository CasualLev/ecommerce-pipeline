import argparse
import csv
import shutil
import statistics

import psycopg2

from spark_streaming_to_postgres import METRICS_FILE, PG

RESULTS_FILE = METRICS_FILE.parent / "performance_results.md"

LATENCY_SQL = """
WITH lat AS (
    SELECT
        EXTRACT(EPOCH FROM inserted_at - event_timestamp)::float8 AS e2e,
        EXTRACT(EPOCH FROM processed_at - event_timestamp)::float8 AS wait,
        EXTRACT(EPOCH FROM inserted_at - processed_at)::float8 AS write
    FROM events
)
SELECT
    COUNT(*),
    AVG(e2e),
    percentile_cont(0.50) WITHIN GROUP (ORDER BY e2e),
    percentile_cont(0.95) WITHIN GROUP (ORDER BY e2e),
    MAX(e2e),
    AVG(wait),
    AVG(write)
FROM lat;
"""

SPAN_SQL = "SELECT EXTRACT(EPOCH FROM MAX(inserted_at) - MIN(event_timestamp))::float8 FROM events;"


def load_metrics() -> list[dict]:
    if not METRICS_FILE.exists():
        raise SystemExit(f"No metrics file at {METRICS_FILE}. Run the pipeline first.")
    with open(METRICS_FILE, encoding="utf-8") as f:
        return list(csv.DictReader(f))


def query_database() -> tuple:
    conn = psycopg2.connect(**PG)
    try:
        with conn.cursor() as cur:
            cur.execute(LATENCY_SQL)
            latency = cur.fetchone()
            cur.execute(SPAN_SQL)
            span = cur.fetchone()[0]
        return latency, span
    finally:
        conn.close()


def summarize(scenario: str, trigger_seconds: float) -> str:
    rows = load_metrics()
    seconds = [float(r["processing_seconds"]) for r in rows]
    rates = [float(r["rows_per_second"]) for r in rows]
    total = lambda key: sum(int(r[key]) for r in rows)

    (db_rows, avg_e2e, p50, p95, max_e2e, avg_wait, avg_write), span = query_database()
    slow_batches = sum(1 for s in seconds if s > trigger_seconds)
    steady = seconds[1:] or seconds  # first batch includes Spark warm-up

    lines = [
        f"## Scenario: {scenario}",
        "",
        "**Volume**",
        f"- Batches processed: {len(rows)}",
        f"- Input rows: {total('input_rows')}",
        f"- Inserted rows: {total('inserted_rows')} (rows in table: {db_rows})",
        f"- Rejected rows: {total('rejected_rows')}",
        f"- Duplicates removed: {total('duplicates_removed')}",
        f"- Already existing (replays skipped): {total('already_existing')}",
        "",
        "**Batch processing time (seconds)**",
        f"- First batch (includes warm-up): {seconds[0]:.2f}",
        f"- Mean (excluding first): {statistics.mean(steady):.2f}",
        f"- Median (excluding first): {statistics.median(steady):.2f}",
        f"- Max: {max(seconds):.2f}",
        f"- Batches slower than the {trigger_seconds:g}s trigger: {slow_batches} of {len(rows)}",
        "",
        "**Throughput**",
        f"- Mean per-batch processing rate: {statistics.mean(rates):.1f} rows/s",
        f"- Sustained end-to-end rate: {db_rows / span:.1f} rows/s over {span:.1f}s" if span else "- Sustained rate: n/a",
        "",
        "**End-to-end latency (event created -> stored in PostgreSQL, seconds)**",
        f"- Mean: {avg_e2e:.2f} | Median: {p50:.2f} | p95: {p95:.2f} | Max: {max_e2e:.2f}",
        f"- Mean wait before processing: {avg_wait:.2f}",
        f"- Mean processing + write time: {avg_write:.2f}",
        "",
    ]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Summarize pipeline performance for one scenario.")
    parser.add_argument("--scenario", required=True, help="Name for this run, e.g. baseline")
    parser.add_argument("--trigger-seconds", type=float, default=5)
    args = parser.parse_args()

    summary = summarize(args.scenario, args.trigger_seconds)
    print(summary)

    shutil.copy(METRICS_FILE, METRICS_FILE.parent / f"{args.scenario}_batch_metrics.csv")
    with open(RESULTS_FILE, "a", encoding="utf-8") as f:
        f.write(summary + "\n")
    print(f"Saved to {RESULTS_FILE}")


if __name__ == "__main__":
    main()