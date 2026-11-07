# Databricks notebook source
# MAGIC %md
# MAGIC # 12 Gold: dim_broker and dim_policy
# MAGIC **What they are:** one row per broker, one row per policy. Fact tables point to them.
# MAGIC
# MAGIC **Surrogate key:** a simple number (1, 2, 3...) that we create ourselves. Facts join on this number instead of the business id (like `BR0001`).
# MAGIC Why? It is smaller, faster, and it keeps working if business ids ever change.
# MAGIC
# MAGIC **Unknown row (key -1):** if a fact row cannot find its broker or policy, it points to -1 ("Unknown") instead of being empty.
# MAGIC That way reports never lose rows silently.
# MAGIC
# MAGIC **Needs:** Silver `brokers` and `policies`.

# COMMAND ----------
# MAGIC %run ./00_gold_config

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 1: load the Silver tables

# COMMAND ----------

load_silver("brokers")
load_silver("policies")
print("Silver brokers:", spark.table("silver_brokers").count())
print("Silver policies:", spark.table("silver_policies").count())

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 2: build dim_broker
# MAGIC The first SELECT is the "Unknown" row. The second SELECT numbers the real brokers 1, 2, 3...

# COMMAND ----------

dim_broker = spark.sql("""
    SELECT -1 AS broker_key, 'UNKNOWN' AS broker_id, 'Unknown' AS broker_name,
           'Unknown' AS region, CAST(NULL AS DECIMAL(5,3)) AS commission_rate
    UNION ALL
    SELECT ROW_NUMBER() OVER (ORDER BY broker_id) AS broker_key,
           broker_id, broker_name, region, commission_rate
    FROM silver_brokers
""")
display(dim_broker.limit(5))
save_gold(dim_broker, "dim_broker")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 3: build dim_policy
# MAGIC We keep `customer_id` and `broker_id` here too, because the fact tables need them to find the right customer and broker.

# COMMAND ----------

dim_policy = spark.sql("""
    SELECT -1 AS policy_key, 'UNKNOWN' AS policy_id, 'UNKNOWN' AS customer_id, 'UNKNOWN' AS broker_id,
           'Unknown' AS policy_type, CAST(NULL AS DECIMAL(12,2)) AS coverage_amount,
           CAST(NULL AS DECIMAL(10,2)) AS annual_premium, 'Unknown' AS status
    UNION ALL
    SELECT ROW_NUMBER() OVER (ORDER BY policy_id) AS policy_key,
           policy_id, customer_id, broker_id, policy_type, coverage_amount, annual_premium, status
    FROM silver_policies
""")
display(dim_policy.limit(5))
save_gold(dim_policy, "dim_policy")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 4: quick check
# MAGIC Each dimension should have (Silver rows + 1) rows, because of the Unknown row. Keys must be unique.

# COMMAND ----------

b = load_gold("dim_broker")
p = load_gold("dim_policy")
print("dim_broker :", b.count(), " distinct keys:", b.select("broker_key").distinct().count())
print("dim_policy :", p.count(), " distinct keys:", p.select("policy_key").distinct().count())
