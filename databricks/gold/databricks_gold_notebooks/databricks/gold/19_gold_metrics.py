# Databricks notebook source
# MAGIC %md
# MAGIC # 19 Gold metrics: the 8 business questions
# MAGIC Each cell answers one question from `docs/business_requirements.md`, using only Gold tables.
# MAGIC Run it, look at the results, and take screenshots.
# MAGIC
# MAGIC **Definitions (write these in `docs/data_model.md`, interviewers ask):**
# MAGIC | Metric | Definition |
# MAGIC |---|---|
# MAGIC | Loss ratio | approved claim amount divided by premium received (payments with status Paid, Late or Partial), by month and policy type |
# MAGIC | Average settlement days | average days from claim date to settlement, settled claims only |
# MAGIC | Collection rate | invoices fully paid divided by invoices due on or before the as-of date; on-time means paid within 5 days after the due date |
# MAGIC | High-risk share | claims with risk band High divided by all claims, by broker region |
# MAGIC | Open claims | claims still Pending or Under Review, with their average age in days at the as-of date |
# MAGIC | Invoice aging | outstanding amount by aging bucket |

# COMMAND ----------
# MAGIC %run ./00_gold_config

# COMMAND ----------

for t in ["dim_date", "dim_broker", "dim_policy", "dim_customer",
          "fact_claims", "fact_invoices", "fact_premium_payments"]:
    load_gold(t)
print("As-of date:", AS_OF)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Loss ratio by month and policy type

# COMMAND ----------

display(spark.sql("""
    WITH claims_m AS (
        SELECT d.year, d.month, p.policy_type, SUM(fc.approved_amount) AS approved
        FROM fact_claims fc
        JOIN dim_date d   ON fc.claim_date_key = d.date_key
        JOIN dim_policy p ON fc.policy_key = p.policy_key
        GROUP BY d.year, d.month, p.policy_type
    ),
    premium_m AS (
        SELECT d.year, d.month, p.policy_type, SUM(fp.amount) AS premium
        FROM fact_premium_payments fp
        JOIN fact_invoices fi ON fp.invoice_id = fi.invoice_id
        JOIN dim_policy p     ON fi.policy_key = p.policy_key
        JOIN dim_date d       ON fp.paid_date_key = d.date_key
        WHERE fp.status IN ('Paid', 'Late', 'Partial')
        GROUP BY d.year, d.month, p.policy_type
    )
    SELECT c.year, c.month, c.policy_type, c.approved, m.premium,
           ROUND(c.approved / m.premium, 3) AS loss_ratio
    FROM claims_m c
    JOIN premium_m m ON c.year = m.year AND c.month = m.month AND c.policy_type = m.policy_type
    ORDER BY c.year, c.month, c.policy_type
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Average settlement days by claim reason

# COMMAND ----------

display(spark.sql("""
    SELECT claim_reason, COUNT(*) AS settled_claims, ROUND(AVG(settlement_days), 1) AS avg_settlement_days
    FROM fact_claims
    WHERE settlement_days IS NOT NULL
    GROUP BY claim_reason
    ORDER BY avg_settlement_days DESC
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Collection rate by due month

# COMMAND ----------

as_of_key = int(AS_OF.replace("-", ""))
display(spark.sql(f"""
    SELECT d.year, d.month,
           COUNT(*) AS invoices_due,
           SUM(CASE WHEN fi.aging_bucket = 'Paid' THEN 1 ELSE 0 END) AS fully_paid,
           SUM(CASE WHEN fi.aging_bucket = 'Paid' AND fi.days_to_pay <= 5 THEN 1 ELSE 0 END) AS paid_on_time,
           ROUND(SUM(CASE WHEN fi.aging_bucket = 'Paid' THEN 1 ELSE 0 END) / COUNT(*), 3) AS collection_rate,
           ROUND(SUM(CASE WHEN fi.aging_bucket = 'Paid' AND fi.days_to_pay <= 5 THEN 1 ELSE 0 END) / COUNT(*), 3) AS on_time_rate
    FROM fact_invoices fi
    JOIN dim_date d ON fi.due_date_key = d.date_key
    WHERE fi.due_date_key <= {as_of_key}
    GROUP BY d.year, d.month
    ORDER BY d.year, d.month
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. High-risk claims by broker region

# COMMAND ----------

display(spark.sql("""
    SELECT b.region,
           COUNT(*) AS claims,
           SUM(CASE WHEN fc.risk_band = 'High' THEN 1 ELSE 0 END) AS high_risk_claims,
           ROUND(SUM(CASE WHEN fc.risk_band = 'High' THEN 1 ELSE 0 END) / COUNT(*), 3) AS high_risk_share
    FROM fact_claims fc
    JOIN dim_broker b ON fc.broker_key = b.broker_key
    GROUP BY b.region
    ORDER BY high_risk_share DESC
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Open claims: how many, and how old

# COMMAND ----------

display(spark.sql(f"""
    SELECT fc.claim_status,
           COUNT(*) AS open_claims,
           ROUND(AVG(datediff(DATE'{AS_OF}', d.full_date)), 1) AS avg_age_days
    FROM fact_claims fc
    JOIN dim_date d ON fc.claim_date_key = d.date_key
    WHERE fc.claim_status IN ('Pending', 'Under Review')
    GROUP BY fc.claim_status
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Where did the customer live on the claim date?
# MAGIC Only claims where the customer's city at the claim date is different from their current city.
# MAGIC If this returns no rows, nobody in your 5 loaded days moved after filing a claim, which is fine.
# MAGIC The SCD2 proof is also in notebook 13, Step 6.

# COMMAND ----------

display(spark.sql("""
    SELECT fc.claim_id, d.full_date AS claim_date, dc.customer_id,
           dc.city AS city_at_claim_date, cur.city AS current_city
    FROM fact_claims fc
    JOIN dim_customer dc  ON fc.customer_key = dc.customer_key
    JOIN dim_customer cur ON cur.customer_id = dc.customer_id AND cur.is_current
    JOIN dim_date d       ON fc.claim_date_key = d.date_key
    WHERE dc.city <> cur.city
    ORDER BY d.full_date DESC
    LIMIT 20
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Invoice aging: money still owed, by bucket

# COMMAND ----------

display(spark.sql("""
    SELECT aging_bucket, COUNT(*) AS invoices, ROUND(SUM(outstanding_amount), 2) AS outstanding_amount
    FROM fact_invoices
    WHERE aging_bucket <> 'Paid'
    GROUP BY aging_bucket
    ORDER BY CASE aging_bucket WHEN 'Not yet due' THEN 0 WHEN '0-30' THEN 1 WHEN '31-60' THEN 2 ELSE 3 END
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 8. Does the adjuster note agree with the fraud API?
# MAGIC A 2 by 2 table, for claims that have a note. If the text and the API agree, most claims fall in
# MAGIC "no flag / not high" and "flag / high".

# COMMAND ----------

display(spark.sql("""
    SELECT (red_flag_count > 0) AS note_has_red_flag,
           (risk_band = 'High') AS api_says_high_risk,
           COUNT(*) AS claims
    FROM fact_claims
    WHERE has_adjuster_note
    GROUP BY (red_flag_count > 0), (risk_band = 'High')
    ORDER BY note_has_red_flag, api_says_high_risk
"""))
