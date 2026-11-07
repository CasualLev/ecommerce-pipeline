# Test Cases

| ID | Area | Test | Steps | Expected Result | Actual Result | Status |
|---|---|---|---|---|---|---|
| TC-01 | Generator | CSV files are created | Run `python data_generator.py --num-files 3 --interval 1` | 3 CSV files appear in `data/input/` | | |
| TC-02 | Generator | Files contain correct columns | Open a generated file | Header has 9 columns: event_id, event_type, user_id, product_id, product_name, category, price, quantity, event_timestamp | | |
| TC-03 | Generator | Correct row count | Count rows with `Import-Csv` | 100 rows, or 101 if a duplicate was injected | | |
| TC-04 | Generator | No partial files | List `data/input/` after generation | Only `.csv` files, no `.tmp` files left behind | | |
| TC-05 | Generator | Bad data is injected | Run with `--bad-rate 0.5 --num-files 1` | Summary log reports injected problems of several kinds | | |
| TC-06 | Spark | New files are detected | Start Spark, then the generator | A batch is logged for each new file within one trigger interval | | |
| TC-07 | Transform | Messy event types are fixed | `SELECT DISTINCT event_type FROM events;` | Only `view`, `add_to_cart`, `purchase` | | |
| TC-08 | Transform | Invalid rows are rejected | Compare the generator's injected counts with Spark's rejected counts | Rejected rows = injected problems minus `messy_event_type` (which is fixed, not rejected) | | |
| TC-09 | Transform | Duplicates are removed | Compare the generator's `duplicate` count with total `duplicates_removed` in `logs/batch_metrics.csv` | Counts match | | |
| TC-10 | Transform | total_amount is correct | `SELECT COUNT(*) FROM events WHERE (event_type = 'purchase' AND total_amount <> ROUND(price * quantity, 2)) OR (event_type <> 'purchase' AND total_amount <> 0);` | 0 | | |
| TC-11 | PostgreSQL | Data is written without errors | `SELECT COUNT(*) FROM events;` | Equals total `inserted_rows` in the metrics file | | |
| TC-12 | Recovery | Restart doesn't reprocess old files | Stop Spark, generate 2 more files, restart Spark | Only the 2 new files are processed; no errors | | |
| TC-13 | Recovery | Replayed batches don't create duplicates | Copy an already-processed CSV into `data/input/` under a new name | `already_existing` equals that file's valid rows; table row count unchanged | | |