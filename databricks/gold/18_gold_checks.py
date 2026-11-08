# Gold checks
def check(name, ok, detail=""):
    print(("PASS   " if ok else "FAIL   ") + name + ("   " + str(detail) if detail != "" else ""))

for t in ["claims", "invoices", "payments", "brokers", "policies"]:
    load_silver(t)
for t in ["dim_date", "dim_broker", "dim_policy", "dim_customer",
          "fact_claims", "fact_invoices", "fact_premium_payments"]:
    load_gold(t)

# Check 1: row counts match Silver

def count(view):
    return spark.table(view).count()

check("fact_claims rows = silver claims", count("fact_claims") == count("silver_claims"),
      f"{count('fact_claims')} vs {count('silver_claims')}")
check("fact_invoices rows = silver invoices", count("fact_invoices") == count("silver_invoices"),
      f"{count('fact_invoices')} vs {count('silver_invoices')}")
check("fact_premium_payments rows = silver payments",
      count("fact_premium_payments") == count("silver_payments"),
      f"{count('fact_premium_payments')} vs {count('silver_payments')}")
check("dim_broker rows = silver brokers + 1", count("dim_broker") == count("silver_brokers") + 1)
check("dim_policy rows = silver policies + 1", count("dim_policy") == count("silver_policies") + 1)

# Check 2: amounts match Silver

a = spark.sql("SELECT SUM(claim_amount) AS s FROM fact_claims").first()["s"]
b = spark.sql("SELECT SUM(claim_amount) AS s FROM silver_claims").first()["s"]
check("sum of claim_amount: fact = silver", a == b, f"{a} vs {b}")

a = spark.sql("SELECT SUM(invoiced_amount) AS s FROM fact_invoices").first()["s"]
b = spark.sql("SELECT SUM(amount) AS s FROM silver_invoices").first()["s"]
check("sum of invoice amount: fact = silver", a == b, f"{a} vs {b}")

a = spark.sql("SELECT SUM(amount) AS s FROM fact_premium_payments").first()["s"]
b = spark.sql("SELECT SUM(amount) AS s FROM silver_payments").first()["s"]
check("sum of payment amount: fact = silver", a == b, f"{a} vs {b}")

# Check 3: keys are unique

for table, key in [("dim_date", "date_key"), ("dim_broker", "broker_key"), ("dim_policy", "policy_key"),
                   ("dim_customer", "customer_key"), ("fact_claims", "claim_id"),
                   ("fact_invoices", "invoice_id"), ("fact_premium_payments", "payment_id")]:
    total = count(table)
    distinct = spark.table(table).select(key).distinct().count()
    check(f"{table}.{key} is unique", total == distinct, f"{total} rows, {distinct} distinct")

# Check 4: SCD2, exactly one current row per customer

bad = spark.sql("""
    SELECT customer_id
    FROM dim_customer
    WHERE customer_id <> 'UNKNOWN'
    GROUP BY customer_id
    HAVING SUM(CASE WHEN is_current THEN 1 ELSE 0 END) <> 1
""").count()
check("every customer has exactly one current row", bad == 0, f"customers breaking the rule: {bad}")

# Check 5: SCD2, no gaps or overlaps

bad = spark.sql("""
    SELECT customer_id FROM (
        SELECT customer_id, effective_to,
               LEAD(effective_from) OVER (PARTITION BY customer_id ORDER BY effective_from) AS next_from
        FROM dim_customer
        WHERE customer_id <> 'UNKNOWN'
    )
    WHERE next_from IS NOT NULL AND effective_to <> next_from
""").count()
check("no gaps or overlaps in customer history", bad == 0, f"customers breaking the rule: {bad}")

display(spark.sql("""
    SELECT 'fact_claims' AS fact,
           SUM(CASE WHEN customer_key = -1 THEN 1 ELSE 0 END) AS unknown_customer,
           SUM(CASE WHEN policy_key   = -1 THEN 1 ELSE 0 END) AS unknown_policy,
           SUM(CASE WHEN broker_key   = -1 THEN 1 ELSE 0 END) AS unknown_broker
    FROM fact_claims
    UNION ALL
    SELECT 'fact_invoices',
           SUM(CASE WHEN customer_key = -1 THEN 1 ELSE 0 END),
           SUM(CASE WHEN policy_key   = -1 THEN 1 ELSE 0 END),
           SUM(CASE WHEN broker_key   = -1 THEN 1 ELSE 0 END)
    FROM fact_invoices
"""))
