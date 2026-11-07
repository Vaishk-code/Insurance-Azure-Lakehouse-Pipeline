# Databricks notebook source
# MAGIC %md
# MAGIC # 14 Gold: fact_claims
# MAGIC **What it is:** one row per claim, with numbers (amounts, days) and keys that point to the dimensions.
# MAGIC
# MAGIC **What we add to each Silver claim:**
# MAGIC * `customer_key`: which customer row was valid **on the claim date** (this is the SCD2 join)
# MAGIC * `policy_key`, `broker_key`: found through the policy
# MAGIC * `claim_date_key`, `settlement_date_key`: dates as numbers like 20250701
# MAGIC * `settlement_days`: days from claim date to settlement (empty if not settled yet)
# MAGIC * `fraud_score`, `risk_band`: from the fraud API (claims never scored get "Unscored")
# MAGIC * from the adjuster notes: `has_adjuster_note`, `red_flag_count`, `note_amount_mismatch`
# MAGIC
# MAGIC **Important rule:** the fact table must end with the **same number of rows** as Silver claims.
# MAGIC If it has more, a join created duplicates. That is why notes are summarised to one row per claim first.
# MAGIC
# MAGIC **Needs:** notebooks 11, 12, 13 done first.

# COMMAND ----------
# MAGIC %run ./00_gold_config

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 1: load everything we need

# COMMAND ----------

load_silver("claims")
load_silver("policies")
load_silver("fraud_scores")
load_silver("claim_notes")
load_gold("dim_customer")
load_gold("dim_policy")
load_gold("dim_broker")
print("Silver claims:", spark.table("silver_claims").count())

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 2: build the fact table
# MAGIC Read it from the top:
# MAGIC * `notes`: one summary row per claim (a claim can have 2 notes, so we summarise first to avoid duplicates)
# MAGIC * `LEFT JOIN`: keep every claim even if something is missing. `COALESCE(..., -1)` points missing ones to "Unknown".
# MAGIC * the customer join has a date rule: `claim_date >= effective_from AND claim_date < effective_to`

# COMMAND ----------

fact_claims = spark.sql("""
    WITH notes AS (
        SELECT claim_id,
               MAX(red_flag_count) AS red_flag_count,
               MAX(CASE WHEN dq_flags LIKE '%note_amount_mismatch%' THEN 1 ELSE 0 END) AS mismatch_flag
        FROM silver_claim_notes
        GROUP BY claim_id
    )
    SELECT
        c.claim_id,
        COALESCE(dc.customer_key, -1) AS customer_key,
        COALESCE(dpol.policy_key, -1) AS policy_key,
        COALESCE(db.broker_key, -1)   AS broker_key,
        CAST(date_format(c.claim_date, 'yyyyMMdd') AS INT)      AS claim_date_key,
        CAST(date_format(c.settlement_date, 'yyyyMMdd') AS INT) AS settlement_date_key,
        c.claim_reason,
        c.claim_status,
        c.claim_amount,
        c.approved_amount,
        CASE WHEN c.settlement_date IS NOT NULL
             THEN datediff(c.settlement_date, c.claim_date) END AS settlement_days,
        f.fraud_score,
        COALESCE(f.risk_band, 'Unscored')            AS risk_band,
        (n.claim_id IS NOT NULL)                     AS has_adjuster_note,
        COALESCE(n.red_flag_count, 0)                AS red_flag_count,
        (COALESCE(n.mismatch_flag, 0) = 1)           AS note_amount_mismatch
    FROM silver_claims c
    LEFT JOIN silver_policies p     ON c.policy_id = p.policy_id
    LEFT JOIN dim_policy dpol       ON c.policy_id = dpol.policy_id
    LEFT JOIN dim_broker db         ON p.broker_id = db.broker_id
    LEFT JOIN dim_customer dc       ON p.customer_id = dc.customer_id
                                   AND c.claim_date >= dc.effective_from
                                   AND c.claim_date <  dc.effective_to
    LEFT JOIN silver_fraud_scores f ON c.claim_id = f.claim_id
    LEFT JOIN notes n               ON c.claim_id = n.claim_id
""")
display(fact_claims.limit(10))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 3: the row count check (must match)

# COMMAND ----------

n_silver = spark.table("silver_claims").count()
n_fact = fact_claims.count()
print("Silver claims:", n_silver, "  fact_claims:", n_fact)
print("OK, same count" if n_silver == n_fact else "PROBLEM: counts differ, a join created duplicates or lost rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 4: save

# COMMAND ----------

save_gold(fact_claims, "fact_claims")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 5: how many claims could not find a match? (Unknown = -1)
# MAGIC A small number is fine if you can explain it. A big number means a join problem.

# COMMAND ----------

display(spark.sql("""
    SELECT SUM(CASE WHEN customer_key = -1 THEN 1 ELSE 0 END) AS unknown_customer,
           SUM(CASE WHEN policy_key   = -1 THEN 1 ELSE 0 END) AS unknown_policy,
           SUM(CASE WHEN broker_key   = -1 THEN 1 ELSE 0 END) AS unknown_broker
    FROM fact_claims
"""))
