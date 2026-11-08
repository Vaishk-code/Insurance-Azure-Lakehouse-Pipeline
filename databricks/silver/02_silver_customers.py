 # 02 Silver: customers

dbutils.widgets.text("batch", "initial", "Bronze load folder")
batch = dbutils.widgets.get("batch")
TABLE = "customers"
src = f"{BRONZE}/sql/{TABLE}/{batch}"

# Step 1: read Bronze

raw = bronze_meta(spark.read.format("parquet").load(src))
print("Bronze rows (includes duplicates):", raw.count())
display(raw.limit(5))

# Step 2: clean and type

typed = raw.select(
    F.upper(F.trim("customer_id")).alias("customer_id"),
    F.initcap(F.trim("first_name")).alias("first_name"),
    F.initcap(F.trim("last_name")).alias("last_name"),
    F.col("dob").cast("date").alias("dob"),
    F.upper(F.trim("gender")).alias("gender"),
    F.when(F.trim("email") == "", None).otherwise(F.lower(F.trim("email"))).alias("email"),
    F.initcap(F.trim("city")).alias("city"),
    F.upper(F.trim("state")).alias("state"),
    F.col("signup_date").cast("date").alias("signup_date"),
    F.col("credit_score").cast("int").alias("credit_score"),
    F.col("last_modified").cast("timestamp").alias("last_modified"),
    "_source_file", "_bronze_ts",
)

# Step 3: rules

reject_rules = {
    "missing_customer_id": "customer_id IS NULL OR customer_id = ''",
    "dob_in_future": "dob > current_date()",
}
flag_rules = {
    "email_missing": "email IS NULL",
    "email_invalid_format": "email IS NOT NULL AND NOT (email RLIKE '^[^@ ]+@[^@ ]+[.][^@ ]+$')",
    "credit_score_out_of_range": "credit_score < 300 OR credit_score > 850",
}
data_cols = ["customer_id", "first_name", "last_name", "dob", "gender", "email",
             "city", "state", "signup_date", "credit_score", "last_modified"]

finish_table(TABLE, batch, typed, data_cols, reject_rules,
             keys=["customer_id"], order_col="last_modified",
             flag_rules=flag_rules, history_keys=["customer_id", "last_modified"])

# Step 4: check the result

out = spark.read.format("delta").load(f"{SILVER}/{TABLE}")
print("Silver customers:", out.count())
print("History rows:", spark.read.format("delta").load(f"{SILVER}/{TABLE}_history").count())
display(out.filter("dq_flags IS NOT NULL").limit(10))
