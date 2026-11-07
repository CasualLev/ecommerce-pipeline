# Test Cases

| ID | Area | Test | Steps | Expected Result | Actual Result | Status |
|---|---|---|---|---|---|---|
| TC-01 | Generator | CSV files are created | Run `python data_generator.py --num-files 3 --interval 1` | 3 CSV files appear in `data/input/` | | |
| TC-02 | Generator | Files contain correct columns | Open a generated file | Header has 9 columns: event_id, event_type, user_id, product_id, product_name, category, price, quantity, event_timestamp | | |
| TC-03 | Generator | Correct row count | Count rows with `Import-Csv` | 100 rows, or 101 if a duplicate was injected | | |
| TC-04 | Generator | No partial files | List `data/input/` after generation | Only `.csv` files, no `.tmp` files left behind | | |
| TC-05 | Generator | Bad data is injected | Run with `--bad-rate 0.5 --num-files 1` | Summary log reports injected problems of several kinds | | |