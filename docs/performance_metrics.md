# Performance Metrics

This report measures the throughput, latency and reliability of the streaming pipeline under three load levels, from a light trickle of events to a stress test.

## 1. Test Environment and Method

**Environment:** a single Windows laptop running every component locally: Spark 3.5.1 in local mode using all CPU cores (`local[*]`), PostgreSQL 16, and the data generator. The generator and Spark shared the same CPU during every test.

**Pipeline settings (identical in all runs):**

| Setting | Value |
|---|---|
| Spark trigger interval | 5 seconds |
| Max files per trigger | 10 |
| Generator corruption rate (`--bad-rate`) | 2% of rows |
| Generator duplicate rate (`--dup-rate`) | 10% chance per file |

**Method.** Before each run, `scripts/reset_pipeline.ps1` cleared the input folder, checkpoint, rejected rows and metrics, and recreated the database, so each scenario started from an empty table. The Spark job was started first, then the generator. Once the generator finished and Spark had processed every file, `analyze_performance.py` summarized the run from two sources:

- `logs/batch_metrics.csv`, written by the Spark job for every micro-batch (row counts and processing time)
- The `events` table, where every row carries three timestamps: `event_timestamp` (created by the generator), `processed_at` (set by Spark) and `inserted_at` (set by PostgreSQL). Their differences give the latency of every individual event.

All timestamps come from the same machine's clock, so latency measurements are not affected by clock drift between machines.

## 2. Scenarios

| Scenario | Events per file | File interval | Files | Planned rate |
|---|---|---|---|---|
| Baseline | 100 | 5 s | 24 | 20 events/s |
| Medium | 1,000 | 2 s | 60 | 500 events/s |
| High | 10,000 | 1 s | 60 | 10,000 events/s |

The high scenario is 500 times the baseline load and was designed to push the pipeline toward its limits.

## 3. Results

### 3.1 Volume and Data Quality

| Metric | Baseline | Medium | High |
|---|---|---|---|
| Batches processed | 23 | 25 | 16 |
| Input rows | 2,404 | 60,006 | 600,007 |
| Inserted rows | 2,355 | 59,003 | 590,020 |
| Rejected rows | 45 (1.87%) | 997 (1.66%) | 9,980 (1.66%) |
| Duplicates removed | 4 | 6 | 7 |
| Replayed rows skipped | 0 | 0 | 0 |
| Rows in table after run | 2,355 | 59,003 | 590,020 |

### 3.2 Batch Processing Time (seconds)

| Metric | Baseline | Medium | High |
|---|---|---|---|
| Average rows per batch | ~105 | ~2,400 | ~37,500 |
| First batch (includes warm-up) | 7.58 | 6.72 | 9.88 |
| Mean (excluding first) | 1.33 | 1.18 | 3.48 |
| Median (excluding first) | 1.29 | 1.15 | 3.32 |
| Batches slower than the 5 s trigger | 1 of 23 | 1 of 25 | 3 of 16 |

### 3.3 Throughput

| Metric | Baseline | Medium | High |
|---|---|---|---|
| Mean per-batch processing rate | 77.2 rows/s | 2,028.5 rows/s | 10,286.7 rows/s |
| Sustained end-to-end rate | 19.7 rows/s | 474.8 rows/s | 6,430.6 rows/s |
| Run duration (first event to last insert) | 119.4 s | 124.3 s | 91.8 s |

For the high scenario, the Spark UI (Structured Streaming tab) reported an **average input rate of 6,693.61 rows/s** and an **average processing rate of 8,008.49 rows/s** across 16 batches.

### 3.4 End-to-End Latency (seconds)

Latency is measured per event, from creation by the generator to storage in PostgreSQL.

| Metric | Baseline | Medium | High |
|---|---|---|---|
| Mean | 5.42 | 4.54 | 6.98 |
| Median | 4.66 | 4.44 | 6.09 |
| p95 | 11.89 | 9.39 | 15.30 |
| Max | 13.33 | 11.43 | 17.81 |
| Mean wait before processing | 3.12 | 2.84 | 3.55 |
| Mean processing + write time | 2.30 | 1.70 | 3.43 |

## 4. Analysis

### 4.1 The pipeline kept up at every load level

The key health check for a streaming pipeline is whether it processes data at least as fast as data arrives. If it doesn't, a backlog builds and latency grows without limit.

In the baseline and medium runs, the sustained end-to-end rate (19.7 and 474.8 rows/s) closely matches the planned input rate (20 and 500 events/s, minus rejected rows). In the high run, the Spark UI shows Spark processing at 8,008 rows/s against an input rate of 6,694 rows/s, about 20% more capacity than was needed.

The high run also shows that **the generator, not Spark, was the bottleneck**: it was planned to produce 10,000 events/s but achieved about 6,700, because writing a 10,000-row CSV file every second takes time, and it shared the CPU with Spark. Spark's maximum throughput on this machine is therefore at least 8,000 rows/s and was not reached in these tests.

### 4.2 Batch time grows much more slowly than batch size

From baseline to high, the average batch grew about 360 times larger (from ~105 to ~37,500 rows), while mean batch time grew only from 1.33 s to 3.48 s, about 2.6 times.

This is because each micro-batch carries a **fixed overhead of roughly one second** regardless of size: scheduling the Spark job, listing new files, opening the JDBC and psycopg2 connections, truncating the staging table, and committing the checkpoint. At baseline volume, this overhead is nearly all of the batch time, which is why the per-batch rate is only 77 rows/s. That number reflects how little data there was, not a capacity limit. At high volume, the same overhead is spread over tens of thousands of rows, and the per-batch rate rises above 10,000 rows/s.

The medium run's batches were slightly faster than the baseline's despite holding 23 times more rows, which confirms that at these sizes, processing time is dominated by fixed overhead rather than data volume.

### 4.3 Latency is dominated by waiting, by design

Mean end-to-end latency stayed between 4.5 and 7 seconds across a 500-fold range of load. In every scenario, the largest share was **waiting** for the next trigger (51–63% of mean latency), not processing.

This is expected with a 5-second trigger: a file that arrives just after Spark checks the folder waits almost a full interval before being picked up, while one that arrives just before waits almost none, averaging out around half the interval. The measured waits (2.8–3.6 s) are slightly above that 2.5 s midpoint because they also include the time the generator spends writing each file and Spark spends listing new files.

The medium run had the lowest latency, likely because files arrived every 2 seconds and were spread more evenly across each 5-second trigger window. In the baseline, one file arrived every 5 seconds, so the timing of file arrivals relative to Spark's checks had a larger effect.

### 4.4 Startup causes a temporary backlog, which clears on its own

In every run, the first batch took 6.7–9.9 seconds, longer than the trigger interval, because it includes one-time setup: starting the JDBC driver, compiling the query plan and opening the first connections.

Under high load, this created a short backlog. Spark logged that batches 0, 1 and 2 were "falling behind," taking 12.1 s, 8.1 s and 6.4 s against the 5 s trigger. Files piled up during the first batch, so batch 1 picked up 8 files (80,001 rows), the largest batch of the run. From batch 3 onward, every batch completed in 2.0–3.8 seconds, well within the trigger interval. This backlog also explains why the high run has the highest p95 and maximum latency (15.30 s and 17.81 s): events that arrived during startup waited longest.

Spark's own timings (e.g., 12.1 s for batch 0) are longer than those recorded inside `foreachBatch` (9.88 s), because Spark's figure also includes work done outside the batch function, such as file listing, query planning and checkpoint commits. This shows a further 1–2 seconds of framework overhead per batch.

### 4.5 Data quality handling is accurate

**Every row is accounted for.** In all three runs, inserted + rejected + duplicates removed equals input rows exactly:

| Scenario | Inserted + Rejected + Duplicates | Input rows |
|---|---|---|
| Baseline | 2,355 + 45 + 4 = 2,404 | 2,404 |
| Medium | 59,003 + 997 + 6 = 60,006 | 60,006 |
| High | 590,020 + 9,980 + 7 = 600,007 | 600,007 |

In each run, the rows inserted also equal the final row count in the `events` table, so nothing was lost or double-written between Spark and PostgreSQL.

**Rejection rates match the generator's settings.** The generator corrupts 2% of rows using six types of problems, one of which (messy event types such as `"  VIEW "`) is fixed by Spark rather than rejected. The expected rejection rate is therefore 2% × 5/6 ≈ **1.67%**. The medium and high runs rejected **1.66%**, matching this almost exactly. The baseline's 1.87% is within normal random variation for a sample of only 2,404 rows. The Spark logs also show rejections spread evenly across the five reject reasons, matching the generator's random choice of problem type.

**Duplicate removal matches expectations.** With a 10% chance of a duplicate per file, the expected count is about 2.4 for 24 files and 6 for 60 files. The pipeline removed 4, 6 and 7 respectively.

## 5. Evaluation Against Targets

The following targets were set for a single-machine local deployment:

| Target | Result | Status |
|---|---|---|
| Steady-state batch time below the 5 s trigger interval | Mean 1.18–3.48 s; after startup, every batch finished within the interval | Met |
| Processing rate at least equal to input rate | Sustained rate matched input in baseline and medium; 8,008 vs. 6,694 rows/s in high | Met |
| No data loss or double-writing | Every input row accounted for; inserted rows equal table rows in every run | Met |
| Invalid rows rejected, valid rows kept | Rejection rate 1.66% vs. 1.67% expected | Met |
| Mean end-to-end latency under 10 s | 4.54–6.98 s | Met |
| p95 end-to-end latency under 15 s | 9.39–11.89 s for baseline and medium; 15.30 s for high, due to the startup backlog | Met for baseline and medium; slightly exceeded under high load |

## 6. Tuning Options and Trade-offs

- **Shorter trigger interval:** would reduce waiting time, the largest part of latency. The trade-off is more, smaller batches, each paying the fixed ~1 s overhead, which lowers efficiency at low volume.
- **Higher `maxFilesPerTrigger`:** would let Spark clear a startup backlog in fewer batches, at the cost of larger, slower individual batches.
- **Warm-up before load:** in these tests, Spark was started before the generator, but the one-time cost still landed on the first batch, because much of the setup (such as loading the JDBC driver and opening connections) only happens when the first data is processed. Sending a small warm-up file before real traffic would absorb this cost. In production, the job runs continuously, so the startup cost occurs only after a restart.
- **Faster database writes:** for much higher volumes, the JDBC writer's `numPartitions` and `batchsize` options, or PostgreSQL's `COPY` command, could speed up the staging write.
- **Scaling out:** in production, Spark would run on a cluster rather than one laptop, and the generator would be replaced by a real event source (such as Kafka) on separate hardware, removing the CPU contention seen here.

## 7. Limitations

- All components shared one machine, so the generator and Spark competed for CPU. Results on dedicated hardware would likely be better.
- Each test ran for one to two minutes. Longer runs would be needed to observe effects such as gradual memory growth or checkpoint size over time.
- Results reflect one specific laptop and will vary on other hardware.
