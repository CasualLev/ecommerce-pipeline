# User Guide

Step-by-step instructions for setting up and running the real-time e-commerce event pipeline on Windows. Commands are for PowerShell, run from the project folder.

## 1. Prerequisites

| Requirement | Notes |
|---|---|
| Python 3.11 | PySpark 3.5 does not support much newer versions such as 3.15 |
| Java 17 (JDK) | Required by Spark. `JAVA_HOME` must be set |
| Hadoop binaries for Windows | `winutils.exe` and `hadoop.dll` in `C:\hadoop\bin`, with `HADOOP_HOME=C:\hadoop` and `C:\hadoop\bin` on `PATH` |
| PostgreSQL 16 | With the command-line tools (`psql`) on `PATH`, e.g. `C:\Program Files\PostgreSQL\16\bin` |
| Internet access (first run only) | Spark downloads the PostgreSQL JDBC driver automatically the first time |

Verify the main tools:

```powershell
py -3.11 --version
java -version
psql --version
```

## 2. Setup

### 2.1 Create the Python environment

```powershell
py -3.11 -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

If activation fails with "running scripts is disabled," run this once, then activate again:

```powershell
Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned
```

Reactivate the environment (`.venv\Scripts\Activate.ps1`) in every new PowerShell window you use for this project.

### 2.2 Create the database

Run the setup script as the PostgreSQL superuser. It creates the `ecommerce_user` user, the `ecommerce` database, and the `events` and `events_staging` tables:

```powershell
psql -U postgres -f postgres_setup.sql
```

Enter the `postgres` superuser password chosen when PostgreSQL was installed. Notices that the database or role "does not exist" on the first run are expected.

The script is safe to re-run: it drops and recreates everything, which **deletes all stored events**.

### 2.3 Configure credentials

```powershell
Copy-Item .env.example .env
```

Open `.env` and set `PG_PASSWORD=ecommerce_local_pw` (the password defined in `postgres_setup.sql`). All connection values are listed in `postgres_connection_details.txt`.

## 3. Running the Pipeline

The pipeline needs two PowerShell windows, both in the project folder with the virtual environment active.

### Step 1: Start the Spark streaming job (Window 1)

```powershell
python spark_streaming_to_postgres.py
```

The job first checks the database connection and reports how many rows the `events` table holds. Wait until it logs **"Streaming started"**. The first run takes longer while the JDBC driver downloads.

### Step 2: Start the data generator (Window 2)

```powershell
python data_generator.py
```

The generator writes a file of 100 events every 5 seconds until stopped. Within a few seconds, Window 1 starts logging one line per batch, for example:

```
Batch 3: 101 in, 98 inserted, 2 rejected {'invalid_price': 1, 'invalid_timestamp': 1}, 1 duplicates removed, 1.31s
```

### Step 3: Stop the pipeline

Press `Ctrl+C` in the generator window first, wait for Spark to log its last batch, then press `Ctrl+C` in the Spark window. Spark may print some errors while shutting down on Windows; this is normal.

When the job is restarted later, it resumes from its checkpoint and processes only files it has not seen before.

## 4. Verifying the Results

Set the database password for the current window, then query the table:

```powershell
$env:PGPASSWORD = "ecommerce_local_pw"

psql -h localhost -U ecommerce_user -d ecommerce -c "SELECT COUNT(*) FROM events;"
psql -h localhost -U ecommerce_user -d ecommerce -c "SELECT event_type, COUNT(*) FROM events GROUP BY event_type;"
psql -h localhost -U ecommerce_user -d ecommerce -c "SELECT * FROM events ORDER BY inserted_at DESC LIMIT 5;"
```

Rows can also be browsed in **pgAdmin 4**, installed with PostgreSQL: connect to `localhost`, then open `ecommerce` → `Schemas` → `public` → `Tables` → `events`.

Other outputs to inspect:

| Location | Contents |
|---|---|
| `data/rejected/` | Invalid rows, each with a `reject_reason` column |
| `logs/batch_metrics.csv` | Row counts and processing time for every batch |
| `http://localhost:4040` | Spark UI while the job runs; the **Structured Streaming** tab shows live input and processing rates |

## 5. Options

### Data generator

| Option | Default | Description |
|---|---|---|
| `--interval` | 5.0 | Seconds between files |
| `--events-per-file` | 100 | Events in each file |
| `--num-files` | 0 | Number of files to write; 0 runs until stopped |
| `--bad-rate` | 0.02 | Share of rows deliberately corrupted |
| `--dup-rate` | 0.10 | Chance that a file contains a duplicate row |
| `--seed` | none | Fixes the random catalog and event mix for repeatable runs |
| `--output-dir` | `data/input` | Folder to write files to |

Example, a fixed run of 10 files with clean data only:

```powershell
python data_generator.py --num-files 10 --interval 2 --bad-rate 0
```

### Spark streaming job

| Option | Default | Description |
|---|---|---|
| `--trigger-seconds` | 5 | How often Spark checks for new files |
| `--max-files-per-trigger` | 10 | Maximum files processed in one micro-batch |
| `--input-dir` | `data/input` | Folder to watch |
| `--checkpoint-dir` | `checkpoints/events_stream` | Where Spark records its progress |

## 6. Resetting the Pipeline

To start again from a completely clean state (empty input folder, checkpoint, rejected rows, metrics and database), stop both scripts and run:

```powershell
.\scripts\reset_pipeline.ps1
```

The script asks for the `postgres` superuser password. Afterwards, confirm the table is empty before starting Spark:

```powershell
$env:PGPASSWORD = "ecommerce_local_pw"
psql -h localhost -U ecommerce_user -d ecommerce -c "SELECT COUNT(*) FROM events;"
```

## 7. Measuring Performance

1. Reset the pipeline (section 6) and confirm the table is empty.
2. Start the Spark job and wait for "Streaming started."
3. Run the generator with a fixed number of files, for example:

   ```powershell
   python data_generator.py --num-files 60 --interval 2 --events-per-file 1000
   ```

4. When no new batches have appeared for about 10 seconds, stop Spark.
5. Summarize the run:

   ```powershell
   python analyze_performance.py --scenario medium
   ```

The summary is printed and appended to `logs/performance_results.md`, and the batch metrics are saved as `logs/<scenario>_batch_metrics.csv`. Repeat from step 1 for each scenario. The scenarios used in `performance_metrics.md` are:

| Scenario | Generator command |
|---|---|
| baseline | `python data_generator.py --num-files 24 --interval 5 --events-per-file 100` |
| medium | `python data_generator.py --num-files 60 --interval 2 --events-per-file 1000` |
| high | `python data_generator.py --num-files 60 --interval 1 --events-per-file 10000` |

## 8. Troubleshooting

| Problem | Likely cause and fix |
|---|---|
| `Cannot connect to PostgreSQL` | PostgreSQL service isn't running, or `.env` has the wrong password. Check with `Get-Service *postgres*` |
| `relation "events" does not exist` | The setup script hasn't been run. Run `psql -U postgres -f postgres_setup.sql` |
| `Java gateway process exited` or `JAVA_HOME is not set` | Java isn't installed or `JAVA_HOME` isn't set. Reopen PowerShell after setting it |
| `HADOOP_HOME and hadoop.home.dir are unset` or `UnsatisfiedLinkError` | Hadoop binaries are missing. Check `C:\hadoop\bin` contains `winutils.exe` and `hadoop.dll` |
| `Python worker failed to connect back` | The virtual environment isn't active. Run `.venv\Scripts\Activate.ps1` |
| `ModuleNotFoundError` | The virtual environment isn't active, or packages aren't installed. Activate it and run `pip install -r requirements.txt` |
| JDBC driver download fails on first run | No internet access. Connect and restart the job; later runs use the cached driver |
| Spark logs "Current batch is falling behind" | A batch took longer than the trigger interval. Normal for the first batch; if it continues, reduce the generator's load or increase `--trigger-seconds` |
| Old files aren't processed after restart | Expected: the checkpoint records processed files. To reprocess everything, run the reset script |
| Reset script fails at the `psql` step | Wrong superuser password, or `PGPASSWORD` was set to the project user's password. The script clears it; re-run and enter the `postgres` password |
