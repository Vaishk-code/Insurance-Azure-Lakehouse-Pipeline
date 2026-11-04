# Design decisions

Short log of choices and the reasoning. Fill in "Outcome" as you build.

| # | Decision | Why | Trade-off | Outcome |
|---|---|---|---|---|
| 1 | Synthetic data modeled on insurance domain | No confidential data; full control of edge cases | Not real-world volume | |
| 2 | Medallion architecture on Delta Lake | Replayable raw layer, ACID MERGE, time travel | More storage than a single layer | |
| 3 | Watermark incremental load (not full reload) | Cuts daily volume; scales | Needs reliable `last_modified` | |
| 4 | Delta MERGE for claims | Same claim arrives again with new status | Slightly more complex than append | |
| 5 | Quarantine table instead of dropping bad rows | Auditable, fixable, reportable | Extra table to maintain | |
| 6 | SCD2 only on customer | Only attribute where history matters for the questions asked | Other dims are Type 1 | |
| 7 | Synapse serverless, not dedicated pool | Pay per query; avoids idle cost | Less suited to heavy concurrent workloads | |
| 8 | Streaming limited to claim events | Fraud needs fast signal; keeps scope small | Rest of the platform is batch | |
| 9 | Batch Gold refresh | Matches how analytics consumers use Gold | Not real-time dashboards | |
| 10 | Adjuster notes parsed with rules (regex and keywords), not ML | Explainable, cheap, fast to build | Misses nuance a language model could catch | |
