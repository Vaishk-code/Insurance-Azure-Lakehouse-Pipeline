# Gold: fact_claims
# Step 1: load everything we need

load_silver("claims")
load_silver("policies")
load_silver("fraud_scores")
load_silver("claim_notes")
load_gold("dim_customer")
load_gold("dim_policy")
load_gold("dim_broker")
print("Silver claims:", spark.table("silver_claims").count())

# Step 2: build the fact table

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

# Step 3: the row count check (must match)

n_silver = spark.table("silver_claims").count()
n_fact = fact_claims.count()
print("Silver claims:", n_silver, "  fact_claims:", n_fact)
print("OK, same count" if n_silver == n_fact else "PROBLEM: counts differ, a join created duplicates or lost rows")

# Step 4: save

save_gold(fact_claims, "fact_claims")

# Step 5: how many claims could not find a match? (Unknown = -1)

display(spark.sql("""
    SELECT SUM(CASE WHEN customer_key = -1 THEN 1 ELSE 0 END) AS unknown_customer,
           SUM(CASE WHEN policy_key   = -1 THEN 1 ELSE 0 END) AS unknown_policy,
           SUM(CASE WHEN broker_key   = -1 THEN 1 ELSE 0 END) AS unknown_broker
    FROM fact_claims
"""))
