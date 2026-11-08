#  Gold: fact_premium_payments
# Step 1: load Silver payments

load_silver("payments")
print("Silver payments:", spark.table("silver_payments").count())

# Step 2: build the fact table

fact_pay = spark.sql("""
    SELECT
        payment_id,
        invoice_id,
        CAST(date_format(paid_date, 'yyyyMMdd') AS INT) AS paid_date_key,
        CAST(date_format(due_date,  'yyyyMMdd') AS INT) AS due_date_key,
        amount,
        status,
        CASE WHEN paid_date IS NOT NULL
             THEN GREATEST(datediff(paid_date, due_date), 0) END AS days_late
    FROM silver_payments
""")
display(fact_pay.limit(10))

# Step 3: row count check, then save

n_silver = spark.table("silver_payments").count()
n_fact = fact_pay.count()
print("Silver payments:", n_silver, "  fact_premium_payments:", n_fact)
print("OK, same count" if n_silver == n_fact else "PROBLEM: counts differ")
save_gold(fact_pay, "fact_premium_payments")

# Step 4: look at payments by status

load_gold("fact_premium_payments")
display(spark.sql("""
    SELECT status, COUNT(*) AS payments, ROUND(AVG(days_late), 1) AS avg_days_late
    FROM fact_premium_payments GROUP BY status ORDER BY status
"""))
