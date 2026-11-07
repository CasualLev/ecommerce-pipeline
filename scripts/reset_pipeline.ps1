# Reset the pipeline to a clean state. Stop Spark and the generator first.
$root = Split-Path $PSScriptRoot -Parent

Write-Host "Clearing input files, checkpoints, rejected rows and current metrics..."
Remove-Item "$root\data\input\*.csv" -ErrorAction SilentlyContinue
Remove-Item "$root\checkpoints\*" -Recurse -Force -ErrorAction SilentlyContinue
Remove-Item "$root\data\rejected" -Recurse -Force -ErrorAction SilentlyContinue
Remove-Item "$root\logs\batch_metrics.csv" -ErrorAction SilentlyContinue

Write-Host "Recreating the database (enter the postgres superuser password)..."
Remove-Item Env:PGPASSWORD -ErrorAction SilentlyContinue
psql -U postgres -f "$root\postgres_setup.sql"

Write-Host "Reset complete."
