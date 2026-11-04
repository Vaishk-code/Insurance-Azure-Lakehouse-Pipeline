# Architecture

## 1. End-to-end architecture

```mermaid
flowchart LR
    subgraph SRC["Sources"]
        A["Azure SQL<br/>policy admin + billing<br/>customers, policies, brokers, invoices"]
        B["Daily CSV files<br/>claims, payments"]
        C["Fraud score API<br/>JSON over HTTP"]
        D["Event Hubs<br/>claim events (streaming)"]
        N["Adjuster notes<br/>unstructured text files"]
    end
    subgraph ING["Ingestion"]
        E["Azure Data Factory<br/>incremental, file, HTTP"]
    end
    subgraph LAKE["ADLS Gen2 - Medallion lakehouse"]
        F[("Bronze<br/>raw, append-only")]
        G[("Silver<br/>cleaned, deduped, MERGE")]
        H[("Gold<br/>star schema + metrics")]
        Q[("Quarantine<br/>rejected rows + reason")]
    end
    I["Databricks<br/>PySpark + Delta Lake"]
    J["Synapse<br/>serverless SQL"]
    K["Power BI"]

    A --> E
    B --> E
    C --> E
    N --> E
    E --> F
    D --> I
    F --> I
    I --> G
    I --> Q
    I --> H
    H --> J --> K
```

## 2. Medallion layers: what happens inside Databricks

```mermaid
flowchart LR
    B[("Bronze<br/>raw + _ingest_ts + _source_file")] --> V{"Data quality rules"}
    V -->|"pass"| S[("Silver<br/>typed, deduped, Delta MERGE")]
    V -->|"fail"| Q[("Quarantine<br/>row + failed rule")]
    Q --> R["Daily DQ report"]
    S --> D1["dim_customer<br/>SCD Type 2"]
    S --> D2["dim_policy, dim_broker, dim_date"]
    S --> F1["fact_claims<br/>fact_invoices<br/>fact_premium_payments"]
    D1 --> G[("Gold")]
    D2 --> G
    F1 --> G
```

## 3. Streaming add-on: claim events

```mermaid
flowchart LR
    P["Claims portal<br/>claim events (JSON)"] --> EH["Azure Event Hubs"]
    EH --> SS["Databricks<br/>Structured Streaming"]
    SS --> DD["Dedupe on event_id<br/>order by event_ts"]
    DD --> BE[("Bronze / Silver<br/>claim_events")]
    BE --> FA["Fraud alert view<br/>same-day high-risk claims"]
```

## 4. Daily run (simulated)

```mermaid
sequenceDiagram
    participant SQL as Azure SQL
    participant ADF as Data Factory
    participant BR as ADLS Bronze
    participant DBX as Databricks
    participant SYN as Synapse
    SQL->>ADF: rows where last_modified > watermark
    ADF->>BR: land SQL changes, claims, payments, fraud scores
    ADF->>DBX: trigger Silver and Gold jobs
    DBX->>BR: read new raw data
    DBX->>DBX: DQ rules, quarantine bad rows, MERGE into Silver
    DBX->>DBX: rebuild Gold facts, SCD2 on customer
    DBX->>ADF: row counts (read / written / rejected)
    ADF->>SYN: Gold available through serverless views
```

## Layers

| Layer | Purpose | Rules |
|---|---|---|
| Bronze | Exact copy of source data plus load metadata (`_ingest_ts`, `_source_file`) | Append-only, never modified |
| Silver | Cleaned, typed, deduplicated current state | Delta `MERGE` on business key; DQ rules applied; bad rows go to Quarantine |
| Gold | Business-ready star schema and metrics | Built from Silver; SCD Type 2 on customer |
| Quarantine | Rejected records with the rule they failed | Reviewed in the daily DQ report |

## Ingestion patterns

| Source | Pattern | Detail |
|---|---|---|
| Azure SQL (customers, policies, brokers, invoices) | Incremental | Watermark on `last_modified`, stored in a control table |
| Claims / payments CSV | File ingestion | One file per day, parameterized by date |
| Fraud API | HTTP pull | REST linked service, JSON to Bronze |
| Adjuster notes (.txt) | Unstructured file ingestion | Copied as-is to Bronze; Databricks reads the text and extracts amounts and red-flag keywords into Silver |
| Claim events | Streaming (add-on) | Event Hubs to Databricks Structured Streaming |

## Cross-cutting design

- **Idempotency:** every load can be re-run for the same day without creating duplicates (MERGE, not INSERT)
- **Schema drift:** Bronze keeps raw schema; Silver enforces an explicit schema and tolerates new columns
- **Observability:** each run logs rows read / written / rejected to an audit table
- **Secrets:** Key Vault, never in code or the repo
- **Cost control:** smallest cluster, auto-terminate, Synapse serverless only, whole project in one resource group that can be deleted in one step
