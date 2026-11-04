# Project plan

Azure credits expire on **12 Nov 2026**, so cloud work is front-loaded and everything is torn down before then.

| Dates | Phase | Deliverable |
|---|---|---|
| 3-4 Nov | Setup | Budget alert, `insureflow-rg`, ADLS (bronze/silver/gold/raw), Azure SQL, ADF, Key Vault; initial data loaded |
| 5-6 Nov | Ingestion | ADF pipelines for SQL (incremental), CSV, and API into Bronze |
| 7-9 Nov | Silver + Gold | Databricks cleaning, DQ rules, quarantine, MERGE, SCD2, parse adjuster notes, metrics |
| 10 Nov | Synapse + streaming add-on | Serverless views over Gold; Event Hubs claim events (if time permits) |
| 11 Nov | Proof | Run the 7 simulated days end to end; screenshots of runs, tables, DQ report |
| 11-12 Nov | Teardown | Export ADF JSON and notebooks to this repo; delete `insureflow-rg` |
| After | Free work | Power BI dashboard, README polish, diagrams, LinkedIn posts, applications |

## Per-phase GitHub checklist
- Commit the code and exported pipeline JSON
- Add screenshots to `docs/images/`
- Update `docs/decisions.md` (Outcome column) and the README status
