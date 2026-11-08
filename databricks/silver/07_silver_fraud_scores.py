 # 07 Silver: fraud scores

dbutils.widgets.text("file_pattern", "fraud_scores_initial*.json", "Bronze file name or pattern")
pattern = dbutils.widgets.get("file_pattern")
TABLE = "fraud_scores"
src = f"{BRONZE}/fraud/{pattern}"
print("Reading:", src)

# Step 1: read the JSON

raw = bronze_meta(spark.read.option("multiLine", True).json(src))
raw.printSchema()
print("Bronze rows:", raw.count())
display(raw.limit(5))

# Step 2: clean and type

typed = raw.select(
    F.upper(F.trim("claim_id")).alias("claim_id"),
    F.col("fraud_score").cast("double").alias("fraud_score"),
    F.initcap(F.trim("risk_band")).alias("risk_band"),
    F.to_timestamp("scored_at").alias("scored_at"),
    "_source_file", "_bronze_ts",
)

# Step 3: rules, quarantine, MERGE

reject_rules = {
    "missing_claim_id": "claim_id IS NULL OR claim_id = ''",
    "score_out_of_range": "fraud_score < 0 OR fraud_score > 1",
}
flag_rules = {
    "risk_band_mismatch": ("risk_band <> CASE WHEN fraud_score >= 0.6 THEN 'High' "
                           "WHEN fraud_score >= 0.3 THEN 'Medium' ELSE 'Low' END"),
}
data_cols = ["claim_id", "fraud_score", "risk_band", "scored_at"]

finish_table(TABLE, pattern, typed, data_cols, reject_rules,
             keys=["claim_id"], order_col="scored_at", flag_rules=flag_rules)

# Step 4: check the result

out = spark.read.format("delta").load(f"{SILVER}/{TABLE}")
display(out.groupBy("risk_band").count())
