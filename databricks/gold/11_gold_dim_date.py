# Gold: dim_date
# Step 1: make one row for every day from 2018 to 2026

dates = spark.sql("""
    SELECT explode(sequence(to_date('2018-01-01'), to_date('2026-12-31'), interval 1 day)) AS full_date
""")
dates.createOrReplaceTempView("all_dates")
print("Days created:", dates.count())

# Step 2: add the useful columns (year, month, quarter, weekday...)

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

# Step 3: save to Gold

save_gold(dim_date, "dim_date")

# Step 4: quick check

d = load_gold("dim_date")
print("rows:", d.count(), " distinct date_key:", d.select("date_key").distinct().count())
