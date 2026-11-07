# Databricks notebook source
# MAGIC %md
# MAGIC # 11 Gold: dim_date
# MAGIC **What it is:** a calendar table, one row per day. Reports use it to group by month, quarter, weekday.
# MAGIC
# MAGIC **Why a separate table?** So every fact table can join to the same calendar, and "March 2025" means the same everywhere.
# MAGIC
# MAGIC **Date key:** we turn a date like `2025-07-01` into the number `20250701`. It is short, easy to read, and joins fast.
# MAGIC
# MAGIC **Needs:** nothing. Run this first.

# COMMAND ----------
# MAGIC %run ./00_gold_config

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 1: make one row for every day from 2018 to 2026

# COMMAND ----------

dates = spark.sql("""
    SELECT explode(sequence(to_date('2018-01-01'), to_date('2026-12-31'), interval 1 day)) AS full_date
""")
dates.createOrReplaceTempView("all_dates")
print("Days created:", dates.count())

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 2: add the useful columns (year, month, quarter, weekday...)

# COMMAND ----------

dim_date = spark.sql("""
    SELECT
        CAST(date_format(full_date, 'yyyyMMdd') AS INT) AS date_key,
        full_date,
        year(full_date)                AS year,
        month(full_date)               AS month,
        date_format(full_date, 'MMMM') AS month_name,
        quarter(full_date)             AS quarter,
        date_format(full_date, 'EEEE') AS day_name,
        dayofweek(full_date) IN (1, 7) AS is_weekend
    FROM all_dates
""")
display(dim_date.limit(10))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 3: save to Gold

# COMMAND ----------

save_gold(dim_date, "dim_date")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 4: quick check
# MAGIC The number of rows should be 3,287 (days from 1 Jan 2018 to 31 Dec 2026), and every `date_key` should be unique.

# COMMAND ----------

d = load_gold("dim_date")
print("rows:", d.count(), " distinct date_key:", d.select("date_key").distinct().count())
