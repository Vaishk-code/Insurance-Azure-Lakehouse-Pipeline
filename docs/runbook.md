# Runbook

## Daily flow (simulated)
1. Run that day's `apply_YYYYMMDD.sql` against Azure SQL
2. Upload the day's claims and payments files to the `raw` container
3. Trigger the ADF pipeline for the date
4. Run the Databricks Silver then Gold jobs
5. Check the DQ report and audit table

## Re-running a day
Safe by design (MERGE). Re-trigger the same date and confirm row counts are unchanged.

## When something fails
Record here: symptom, cause, fix. (Day 5 schema drift is the first planned incident.)

| Date | Symptom | Cause | Fix |
|---|---|---|---|
| | | | |
