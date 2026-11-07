# Databricks notebook source
# MAGIC %md
# MAGIC # 13 Gold: dim_customer (SCD Type 2)
# MAGIC
# MAGIC ## The idea in plain words
# MAGIC When a customer moves from Dallas to Boston, we do **not** overwrite Dallas. We keep both rows,
# MAGIC and write the dates when each was true. This is called **SCD Type 2** (slowly changing dimension).
# MAGIC
# MAGIC Example, customer C000042 moved on 3 July 2025:
# MAGIC
# MAGIC | customer_key | customer_id | city | effective_from | effective_to | is_current |
# MAGIC |---|---|---|---|---|---|
# MAGIC | 101 | C000042 | Dallas | 1900-01-01 | 2025-07-03 | false |
# MAGIC | 5102 | C000042 | Boston | 2025-07-03 | 9999-12-31 | true |
# MAGIC
# MAGIC **Why it matters:** a claim filed in March 2025 should show Dallas (where the customer lived then),
# MAGIC not Boston. A fact table can do that by joining on the date.
# MAGIC
# MAGIC ## How we build it (4 small steps)
# MAGIC 1. Take every saved version of each customer (`customers_history`) in date order
# MAGIC 2. Keep a version only if it is the first one, or the city or state changed
# MAGIC 3. `effective_from` = date of the change. The very first version starts at 1900-01-01
# MAGIC    (some customers signed up after their policy began, so old claims still need a match).
# MAGIC 4. `effective_to` = the date of the next version, or 9999-12-31 for the latest
# MAGIC
# MAGIC **Needs:** Silver `customers_history`.

# COMMAND ----------
# MAGIC %run ./00_gold_config

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 1: load the history and look at one customer who moved

# COMMAND ----------

hist = load_silver("customers_history")
print("version rows in history:", hist.count(),
      " distinct customers:", hist.select("customer_id").distinct().count())

# Customers with more than one version = customers whose data changed
display(spark.sql("""
    SELECT customer_id, COUNT(*) AS versions
    FROM silver_customers_history
    GROUP BY customer_id
    HAVING COUNT(*) > 1
    ORDER BY versions DESC
    LIMIT 10
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 2: number the versions and look at the previous city
# MAGIC `LAG(city)` gives the city from the version before. We compare it with the current one.

# COMMAND ----------

spark.sql("""
    SELECT customer_id, first_name, last_name, city, state, credit_score, last_modified,
           ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY last_modified) AS version_no,
           LAG(city)  OVER (PARTITION BY customer_id ORDER BY last_modified) AS prev_city,
           LAG(state) OVER (PARTITION BY customer_id ORDER BY last_modified) AS prev_state
    FROM silver_customers_history
""").createOrReplaceTempView("step_ordered")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 3: keep only real changes
# MAGIC Keep the first version, or any version where the city or state is different from before.
# MAGIC (`<=>` is an "equals" that treats two empty values as equal, so it is safe with missing data.)

# COMMAND ----------

spark.sql("""
    SELECT *
    FROM step_ordered
    WHERE version_no = 1
       OR NOT (city  <=> prev_city)
       OR NOT (state <=> prev_state)
""").createOrReplaceTempView("step_changes")

print("versions kept:", spark.table("step_changes").count())

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 4: add the start and end dates
# MAGIC `LEAD(effective_from)` gives the start date of the next version. That is the end date of this one.

# COMMAND ----------

spark.sql("""
    SELECT customer_id, first_name, last_name, city, state, credit_score, last_modified,
           CASE WHEN version_no = 1 THEN DATE'1900-01-01'
                ELSE CAST(last_modified AS DATE) END AS effective_from
    FROM step_changes
""").createOrReplaceTempView("step_from")

spark.sql("""
    SELECT *,
           COALESCE(LEAD(effective_from) OVER (PARTITION BY customer_id ORDER BY last_modified),
                    DATE'9999-12-31') AS effective_to
    FROM step_from
""").createOrReplaceTempView("step_ranges")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 5: add the surrogate key and the "current" flag, plus the Unknown row

# COMMAND ----------

real = spark.sql("""
    SELECT ROW_NUMBER() OVER (ORDER BY customer_id, effective_from) AS customer_key,
           customer_id, first_name, last_name, city, state, credit_score,
           effective_from, effective_to,
           (effective_to = DATE'9999-12-31') AS is_current
    FROM step_ranges
""")

unknown = spark.sql("""
    SELECT -1 AS customer_key, 'UNKNOWN' AS customer_id, 'Unknown' AS first_name, 'Unknown' AS last_name,
           'Unknown' AS city, 'Unknown' AS state, CAST(NULL AS INT) AS credit_score,
           DATE'1900-01-01' AS effective_from, DATE'9999-12-31' AS effective_to, true AS is_current
""")

dim_customer = real.unionByName(unknown)
save_gold(dim_customer, "dim_customer")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 6: see it working on a customer who moved
# MAGIC Pick a customer id from the Step 1 list (one with 2 or more versions) and type it below.
# MAGIC You should see one row per place they lived, with dates that join up with no gap.

# COMMAND ----------

dim = load_gold("dim_customer")
movers = spark.sql("""
    SELECT customer_id FROM dim_customer
    WHERE customer_id <> 'UNKNOWN'
    GROUP BY customer_id HAVING COUNT(*) > 1
    LIMIT 5
""")
display(movers)

# COMMAND ----------

# Change the id below to one from the list above
example_id = movers.first()["customer_id"] if movers.count() > 0 else None
print("Showing customer:", example_id)
if example_id:
    display(spark.sql(f"""
        SELECT customer_key, customer_id, city, state, effective_from, effective_to, is_current
        FROM dim_customer WHERE customer_id = '{example_id}' ORDER BY effective_from
    """))
