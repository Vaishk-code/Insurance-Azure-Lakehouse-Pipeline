# Runbook

## Daily flow (simulated)
1. Run that day's `apply_YYYYMMDD.sql` against Azure SQL
2. Upload the day's claims and payments files to the `raw` container
3. Trigger the ADF pipeline for the date
4. Run the Databricks Silver then Gold jobs
5. Check the DQ report and audit table

## Re-running a day
Safe by design (MERGE). Re-trigger the same date and confirm row counts are unchanged.

Check before and after the re-run:
- Silver row counts per table are identical
- Audit table shows a new run entry for the date, with the same rows read and a MERGE that updated 0 new rows
- Quarantine count for that date has not doubled
- Gold DQ checks still show 12 of 12 PASS

## When something fails
Record here: symptom, cause, fix. (Day 5 schema drift is the first planned incident.)

| Date | Symptom | Cause | Fix |
|---|---|---|---|
| 2026-11-02 | ADF pipeline for Day 2 succeeded but claims count in Bronze was 0 | Pipeline was triggered before the claims file finished uploading to `raw`, so the copy step found nothing | Re-uploaded the file, re-triggered the same date. MERGE made the re-run safe. Added a check that fails the pipeline if the expected file is missing |
| 2026-11-03 | 9 customer rows missing from the Day 3 incremental load | `last_modified` was null on those rows, so the watermark filter skipped them | Routed null `last_modified` rows to Quarantine with a rejection reason. Fixed the source rows and reprocessed them. Added a null check on the watermark column to the DQ report |
| 2026-11-05 | Day 5 Silver claims job failed with a column mismatch error | Schema drift: the claims file arrived with a new column (`adjuster_region`) and `claim_amt` renamed to `claim_amount` | Kept Bronze as raw (schema merge enabled) so no data was lost. Updated the Silver mapping to read the new name and carry the new column. Re-ran Silver and Gold for Day 5. All 12 Gold checks passed afterwards |
| 2026-11-05 | Re-run of Day 5 after the fix | Verification step | Silver counts matched the first successful run, confirming the MERGE is idempotent |

## Lessons learned
- Keep Bronze permissive and enforce schema in Silver, so drift never loses data
- Validate that input files exist before triggering ADF
- Log every incident here with the date, even if the fix was small