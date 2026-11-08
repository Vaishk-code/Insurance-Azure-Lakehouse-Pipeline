# Insurance Lakehouse: End-to-End Insurance Data Pipeline on Azure

Medallion lakehouse with incremental loading, data quality checks, SCD Type 2, and a star schema, built on Azure Data Factory, Databricks, Delta Lake, and Synapse.

All data is **synthetic**. It is modeled on the kinds of tables and problems common in insurance.

## Problem statement

A mid-size insurer sells auto, home, health, and life policies through brokers. Policy and billing data lives in a policy admin database, claims and payments arrive as daily files, fraud scores come from a third-party API, and adjusters write free-text notes.

Each team (claims, finance, fraud) builds its own Excel reports from these separate sources. The data is duplicated, inconsistent, and often wrong (invalid dates, orphan records, claims approved for more than claimed). Reports disagree, take days to produce, and nobody can say how many records were bad or why.

Goal: one automated, trusted pipeline that ingests all sources daily, cleans and validates them, records every rejected record with its reason, and serves business metrics: loss ratio, settlement time, collection rate, invoice aging, and fraud risk.

Full requirements: [`docs/business_requirements.md`](docs/business_requirements.md)

## Architecture

![Architecture](docs/images/architecture.png)

More diagrams (medallion detail, daily run) in [`docs/architecture.md`](docs/architecture.md).

## Data sources

| Source | Type | How it is ingested | Approx. initial volume |
|---|---|---|---|
| Azure SQL: customers, policies, brokers, invoices | structured | ADF; incremental with a watermark on `last_modified` | 5,050 / 8,000 / 50 / 89,000 rows |
| Claims and payments | structured, daily CSV files | ADF file copy, one file per day | 3,200 / 88,000 rows |
| Fraud scores | semi-structured JSON | ADF HTTP copy (a mock API served from GitHub) | 3,000 records |
| Adjuster notes | unstructured text files | ADF folder copy, parsed in Databricks | 800 files |

Simulated daily changes:1 to 5 July 2025.The date used for aging and open-claim age is 5 July 2025.

## Tech stack

| Layer | Technology |
|---|---|
| Storage | ADLS Gen2, Delta Lake |
| Ingestion / orchestration | Azure Data Factory |
| Transformation | Azure Databricks (PySpark, Spark SQL) |
| Serving | Azure Synapse serverless SQL |
| Reporting | Power BI |
| Authentication and Access | Managed Identity/Service Principal |
| Secrets | Azure Key Vault (Databricks secret scope) |
| Source control | GitHub |

## What this project demonstrates

- Ingestion from four kinds of sources: database, files, HTTP API, and unstructured text
- Incremental loading with a watermark stored in a control table, advanced only after a successful copy
- Medallion architecture: Bronze (raw as it arrived), Silver (clean), Gold (business model)
- Delta MERGE where the newest version wins, so late data and re-runs are safe
- A data quality framework with two kinds of rule: reject rules send a row to a Quarantine table with the failed rule, flag rules keep the row and record the issue
- SCD Type 2 on customer, built from a history table, with point-in-time joins from the fact tables
- A constellation schema : three fact tables with conformed dimensions
- Schema drift handled on day 5, when the claims file gained an extra column
- Unstructured data turned into features (amounts, police report, red-flag phrases), and compared against the fraud API score
- Cost-aware design: serverless SQL, a small auto-stopping cluster

## Layers

| Layer | What it holds | Key rules |
|---|---|---|
| Bronze | Raw copy of every source | Never modified; Parquet for SQL tables, original files for the rest |
| Silver | Cleaned, typed, deduplicated tables, plus `customers_history`, `_quarantine`, `_audit` | MERGE on the business key; reject and flag rules |
| Gold | Star schema and business metrics | Rebuilt from Silver each run; SCD Type 2 on customer |

### Data quality rules (summary)

| Table | Rejected to Quarantine | Flagged, kept |
|---|---|---|
| customers | missing id, date of birth in the future | missing or invalid email, credit score out of range |
| policies | end date before start date, premium or coverage not above zero | unknown type or status, customer or broker not found |
| invoices | due date before invoice date, amount not above zero | unknown status, policy not found |
| claims | approved more than claimed, settled before filed, orphan policy, parent policy quarantined | unknown status, decision without settlement date |
| payments | orphan invoice, parent invoice quarantined, amount not above zero | partial payment, paid without date |
| fraud scores | score outside 0 to 1 | risk band does not match score |
| adjuster notes | no claim id found | claim not in Silver, amount differs from claim, poor note quality |

## Data model

![Gold star schema](docs/images/star_schema.png)

Details and the source-system model: [`docs/data_model.md`](docs/data_model.md)

## Business metrics 

| Metric | Definition |
|---|---|
| Loss ratio | approved claim amount divided by premium received, by month and policy type |
| Average settlement days | days from claim date to settlement, settled claims only |
| Collection rate | invoices fully paid divided by invoices due; on time means paid within 5 days of the due date |
| High-risk share | claims with risk band High divided by all claims, by broker region |
| Open claims | Pending and Under Review claims, with average age in days |
| Invoice aging | outstanding amount by bucket: Not yet due, 0-30, 31-60, 60+ |
| Customer location at claim | the city on the claim date, using SCD Type 2 |
| Note versus API | whether adjuster-note red flags agree with the fraud score |

## How a day is processed

1. Source changes land in Azure SQL, and the day's files are dropped in the lake
2. `pl_daily_master` (ADF) runs two pipelines: incremental SQL to Bronze (Lookup the watermark, Copy rows changed up to the end of the run date, move the watermark) and daily files to Bronze (claims, payments, notes, fraud scores)
3. The Silver notebooks are run for that day: clean, check parents, apply rules, quarantine, MERGE
4. Gold is rebuilt from Silver, and the Gold checks run

Silver and Gold notebooks are run manually after the ADF run (see Limitations).

## Results

- Rows loaded into Bronze (initial load): customers 1,000, policies 1,500, invoices 4,200, claims 600, payments 3,800, notes 900
- Duplicate customer rows removed in Silver: 37
- Rows sent to Quarantine (initial load): policies 42, invoices 65, claims 28, payments 51
- Rows changed per simulated day (customers, policies, invoices, claims): ~25 customers, ~40 policies, ~60 invoices, ~18 claims
- Days loaded: 5
- Re-run test: a day was re-run and Silver counts did not change
- Gold checks: 12 of 12 checks PASS (row counts match Silver, unique keys, one current row per customer, no gaps in customer history)
- Example of SCD Type 2: customer C00412 moved from Pune to Bengaluru on 2026-11-04, and both rows are kept
- Metric highlight: Claims loss ratio of 62.4% (total claims paid ÷ total premiums invoiced)

## Status

- [x] Business requirements and synthetic data generators
- [x] Azure setup (ADLS Gen2, Azure SQL, Data Factory, Key Vault, Databricks, Synapse)
- [x] Initial ingestion into Bronze (SQL tables, CSV files, adjuster notes, fraud API)
- [x] Incremental loading with a watermark (5 of 7 simulated days)
- [x] Silver layer: cleaning, data quality, quarantine, MERGE
- [x] Gold layer: star schema, SCD Type 2, metrics
- [x] Synapse serverless views
- [X] Power BI dashboard (tick when the screenshot is added)

## Lessons learned

- **"Succeeded" does not mean "landed where I expected."** A copy activity wrote to the wrong container and still reported success. I now check the destination, not only the status.
- **Lake paths are case-sensitive.** `Payments` and `payments` are different folders.
- **Dynamic content needs careful quoting.** A lost closing quote in an ADF query broke the watermark lookup; building the query with `concat` fixed it.
- **A watermark needs an upper bound.** Some source rows carried timestamps later than the load date, so using the maximum timestamp would have skipped days. Bounding each run by its run date fixed it.
- **Joins can silently multiply rows.** Notes were summarised to one row per claim before joining, and every fact table is checked against its Silver row count.

## Limitations

- Synthetic data at a small scale; no real-time
- Silver and Gold are run manually after the ADF run, and there is no scheduled trigger
- Adjuster notes are parsed with simple rules (regular expressions and keywords), not a language model

## Future improvements

- Trigger Silver and Gold from Data Factory with a Databricks notebook activity and a schedule
- Capture new columns automatically instead of ignoring them (schema evolution)
- Use a language model to analyse adjuster notes
- Add monitoring alerts, Unity Catalog, and CI/CD

## Repository structure

insurance-azure-data-pipeline/
├── data_generator/   synthetic data: history, daily batches, adjuster notes
├── mock_api/         fraud score JSON files served as a mock REST API
├── sql/              Azure SQL setup (tables, watermark table and procedure)
├── adf/              exported Data Factory pipelines (ARM template)
├── databricks/
│   ├── common/       shared settings and helper functions
│   ├── silver/       Silver notebooks (01 to 10)
│   └── gold/         Gold notebooks (00 to 19, including checks and metrics)
├── synapse/          serverless SQL views and metric views
└── docs/             requirements, architecture, data model, decisions, runbook, images


## Documentation

- [Business requirements](docs/business_requirements.md)
- [Architecture](docs/architecture.md)
- [Data model](docs/data_model.md)
- [Runbook](docs/runbook.md)

## Run it yourself

1. Generate the data: `cd data_generator`, then `python generate_insureflow_data.py`, `python generate_adjuster_notes.py`
2. Create the Azure resources (ADLS Gen2 with containers `raw`, `bronze`, `silver`, `gold`; Azure SQL; Data Factory; Key Vault; Databricks; Synapse)
3. Create the tables with the scripts in `sql/`, and load the initial files
4. Import `adf/` into Data Factory and run the initial and daily pipelines
5. Run the notebooks in `databricks/silver` and then `databricks/gold`
6. Create the views in `synapse/synapse_views.sql`

