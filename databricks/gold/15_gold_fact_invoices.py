# Gold: fact_invoices
# Step 1: load what we need

# COMMAND ----------

load_silver("invoices")
load_silver("payments")
load_silver("policies")
load_gold("dim_customer")
load_gold("dim_policy")
load_gold("dim_broker")
print("As-of date:", AS_OF)
print("Silver invoices:", spark.table("silver_invoices").count())

#Step 2: add up the payments per invoice

spark.sql("""
    SELECT invoice_id,
           SUM(CASE WHEN status IN ('Paid','Late','Partial') THEN amount ELSE 0 END) AS amount_paid,
           MAX(CASE WHEN status IN ('Paid','Late','Partial') THEN paid_date END)     AS last_paid_date
    FROM silver_payments
    GROUP BY invoice_id
""").createOrReplaceTempView("paid_per_invoice")

# Step 3: join invoices with payments and work out the new columns

spark.sql(f"""
    SELECT i.invoice_id, i.policy_id, i.customer_id, i.invoice_date, i.due_date,
           i.amount AS invoiced_amount, i.status,
           COALESCE(p.amount_paid, 0) AS amount_paid,
           p.last_paid_date,
           pol.broker_id
    FROM silver_invoices i
    LEFT JOIN paid_per_invoice p ON i.invoice_id = p.invoice_id
    LEFT JOIN silver_policies pol ON i.policy_id = pol.policy_id
""").createOrReplaceTempView("inv_base")

spark.sql(f"""
    SELECT *,
           (amount_paid >= invoiced_amount) AS is_fully_paid,
           GREATEST(invoiced_amount - amount_paid, 0) AS outstanding_amount,
           CASE WHEN amount_paid >= invoiced_amount
                THEN datediff(last_paid_date, due_date) END AS days_to_pay,
           CASE WHEN amount_paid < invoiced_amount AND due_date < DATE'{AS_OF}'
                THEN datediff(DATE'{AS_OF}', due_date) ELSE 0 END AS days_overdue
    FROM inv_base
""").createOrReplaceTempView("inv_calc")

# Step 4: add the aging bucket and the dimension keys

fact_invoices = spark.sql(f"""
    SELECT
        x.invoice_id,
        COALESCE(dc.customer_key, -1) AS customer_key,
        COALESCE(dpol.policy_key, -1) AS policy_key,
        COALESCE(db.broker_key, -1)   AS broker_key,
        CAST(date_format(x.invoice_date, 'yyyyMMdd') AS INT) AS invoice_date_key,
        CAST(date_format(x.due_date, 'yyyyMMdd') AS INT)     AS due_date_key,
        x.invoiced_amount,
        x.amount_paid,
        x.outstanding_amount,
        x.status,
        x.days_to_pay,
        x.days_overdue,
        CASE WHEN x.is_fully_paid THEN 'Paid'
             WHEN x.due_date >= DATE'{AS_OF}' THEN 'Not yet due'
             WHEN x.days_overdue <= 30 THEN '0-30'
             WHEN x.days_overdue <= 60 THEN '31-60'
             ELSE '60+' END AS aging_bucket
    FROM inv_calc x
    LEFT JOIN dim_policy dpol ON x.policy_id = dpol.policy_id
    LEFT JOIN dim_broker db   ON x.broker_id = db.broker_id
    LEFT JOIN dim_customer dc ON x.customer_id = dc.customer_id
                             AND x.invoice_date >= dc.effective_from
                             AND x.invoice_date <  dc.effective_to
""")
display(fact_invoices.limit(10))

# Step 5: row count check, then save

n_silver = spark.table("silver_invoices").count()
n_fact = fact_invoices.count()
print("Silver invoices:", n_silver, "  fact_invoices:", n_fact)
print("OK, same count" if n_silver == n_fact else "PROBLEM: counts differ")
save_gold(fact_invoices, "fact_invoices")

# Step 6: look at the aging buckets and one partially paid invoice

load_gold("fact_invoices")
display(spark.sql("""
    SELECT aging_bucket, COUNT(*) AS invoices, ROUND(SUM(outstanding_amount), 2) AS outstanding
    FROM fact_invoices GROUP BY aging_bucket ORDER BY aging_bucket
"""))

display(spark.sql("""
    SELECT invoice_id, invoiced_amount, amount_paid, outstanding_amount, aging_bucket
    FROM fact_invoices
    WHERE amount_paid > 0 AND amount_paid < invoiced_amount
    LIMIT 5
"""))
