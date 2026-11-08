# Gold: dim_customer (SCD Type 2)
# Step 1: load the history and look at one customer who moved

hist = load_silver("customers_history")
print("version rows in history:", hist.count(),
      " distinct customers:", hist.select("customer_id").distinct().count())


display(spark.sql("""
    SELECT customer_id, COUNT(*) AS versions
    FROM silver_customers_history
    GROUP BY customer_id
    HAVING COUNT(*) > 1
    ORDER BY versions DESC
    LIMIT 10
"""))

# Step 2: number the versions and look at the previous city

spark.sql("""
    SELECT customer_id, first_name, last_name, city, state, credit_score, last_modified,
           ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY last_modified) AS version_no,
           LAG(city)  OVER (PARTITION BY customer_id ORDER BY last_modified) AS prev_city,
           LAG(state) OVER (PARTITION BY customer_id ORDER BY last_modified) AS prev_state
    FROM silver_customers_history
""").createOrReplaceTempView("step_ordered")

# Step 3: keep only real changes

spark.sql("""
    SELECT *
    FROM step_ordered
    WHERE version_no = 1
       OR NOT (city  <=> prev_city)
       OR NOT (state <=> prev_state)
""").createOrReplaceTempView("step_changes")

print("versions kept:", spark.table("step_changes").count())

# Step 4: add the start and end dates

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

# Step 5: add the surrogate key and the "current" flag, plus the Unknown row

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

# Step 6: see it working on a customer who moved

dim = load_gold("dim_customer")
movers = spark.sql("""
    SELECT customer_id FROM dim_customer
    WHERE customer_id <> 'UNKNOWN'
    GROUP BY customer_id HAVING COUNT(*) > 1
    LIMIT 5
""")
display(movers)

example_id = movers.first()["customer_id"] if movers.count() > 0 else None
print("Showing customer:", example_id)
if example_id:
    display(spark.sql(f"""
        SELECT customer_key, customer_id, city, state, effective_from, effective_to, is_current
        FROM dim_customer WHERE customer_id = '{example_id}' ORDER BY effective_from
    """))
