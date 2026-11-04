# InsureFlow: Business Requirements

## 1. Background
A mid-size insurer (auto, home, health, life) runs a policy admin system on Azure SQL, receives claims and premium payments as daily file drops, and buys fraud scores from a third-party API. Reporting is manual (Excel), slow, and inconsistent across teams.

**Goal:** a single, trusted analytics platform on Azure with daily refreshed data.

## 2. Stakeholders and what they need
| Team | Need |
|---|---|
| Claims | Settlement time, open claims backlog, rejection rate |
| Finance | Loss ratio, premium collection rate, late/missed payments |
| Fraud | High-risk claims by region, policy type, and broker |
| Brokers/Sales | Policies sold, premium by broker and region |

## 3. Source systems
| Source | Format | Frequency | Load type |
|---|---|---|---|
| Policy admin + billing (Azure SQL): customers, policies, brokers, invoices | Tables | Continuous changes | Incremental (watermark on `last_modified`) |
| Claims | CSV | Daily file, contains new claims **and status updates** | File ingestion + MERGE |
| Premium payments | CSV | Daily file | Append |
| Fraud scores | REST API (JSON) | Daily | HTTP pull |
| Adjuster notes | Free-text `.txt` files, one per claim | Daily | File ingestion + text parsing |

## 4. Business questions (Gold layer must answer)
1. What is the loss ratio (approved claims / premium) by policy type and region, per month?
2. What is the average claim settlement time, and how is it trending?
3. What percentage of invoiced premium is collected on time, late, partially, or not at all?
4. Which regions and brokers have the most high-risk (fraud score >= 0.6) claims?
5. How many claims are still Pending / Under Review, and for how long?
6. What was a customer's city/risk profile **at the time the claim was filed**? (needs SCD Type 2)
7. How much invoiced premium is outstanding, by aging bucket (0-30, 31-60, 60+ days overdue)?
8. Which claims have red-flag language in adjuster notes, and does that agree with the fraud API score?

## 5. Known data problems (found during profiling)
| Issue | Where | Handling |
|---|---|---|
| Duplicate customer rows | customers | Dedupe on `customer_id`, keep latest |
| Missing / invalid email | customers | Flag; quarantine invalid format |
| End date before start date | policies | Quarantine with reason |
| Negative annual premium | policies | Quarantine with reason |
| Approved amount > claimed amount | claims | Quarantine with reason |
| Settlement before claim date | claims | Quarantine with reason |
| Claim references unknown policy | claims | Quarantine (orphan) |
| Note missing the Claim ID header | adjuster notes | Fall back to the file name; quarantine if neither matches a claim |
| Blank or near-empty note ("See attached.") | adjuster notes | Keep; flag note quality as poor |
| Amount written in note differs a lot from claim amount | adjuster notes vs claims | Flag for review |
| Thousands of small files | adjuster notes | Batch reads; discuss the small-files problem |
| Payment references an invoice that does not exist | payments | Quarantine (orphan) |
| Invoice due date earlier than invoice date | invoices | Quarantine with reason |
| Payment amount lower than invoice amount (partial payment) | payments | Keep; mark invoice Partially Paid; report separately |
| Payment arrives after invoice already marked Overdue | payments, invoices | Reconcile on `invoice_id`; latest status wins |
| Same claim arrives multiple times (status updates) | daily claims | Delta MERGE on `claim_id` using `last_modified` |
| Duplicate rows inside a daily file | daily claims | Dedupe before merge |
| Schema change (new column `adjuster_notes`) | claims, day 5 | Handle schema drift without pipeline failure |
| Some claims never receive a fraud score | API | Keep claim, mark score as NULL / "Unscored" |

## 6. Non-functional requirements
- **Incremental:** never reload full history daily
- **Idempotent:** re-running a day must not create duplicates
- **Auditable:** every rejected record is stored with the rule it failed
- **Observable:** failures raise an alert; each run logs rows read / written / rejected
- **Cost-aware:** serverless / auto-terminating compute

## 7. Success criteria
- All 8 business questions answerable from Gold tables
- Daily pipeline runs end-to-end with no manual steps
- Re-run of any day produces identical results
- DQ report shows counts of rejected records per rule
- Documentation: architecture diagram, data model, runbook

## 8. Out of scope
Real-time streaming, ML models, production security hardening.
