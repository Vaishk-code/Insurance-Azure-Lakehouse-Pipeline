# 08 Silver: adjuster notes (unstructured text)

dbutils.widgets.text("batch", "initial", "Bronze notes folder")
batch = dbutils.widgets.get("batch")
TABLE = "claim_notes"
src = f"{BRONZE}/notes/{batch}/*.txt"
print("Reading:", src)

# Step 1: read every note file as one text value (`wholetext`)

raw = bronze_meta(spark.read.text(src, wholetext=True))
print("Note files:", raw.count())
display(raw.limit(3))

# Step 2: extract fields with regular expressions

txt = F.col("value")
body = F.regexp_replace(
    F.regexp_extract(txt, r"(?s)Claim Type:[^\n]*\n\s*\n(.*)$", 1), r"^\s+|\s+$", "")

parsed = (raw
    .withColumn("file_name", F.regexp_extract("_source_file", r"([^/]+)$", 1))
    .withColumn("claim_id_header", F.regexp_extract(txt, r"Claim ID:\s*(CL\d+)", 1))
    .withColumn("claim_id_file", F.regexp_extract("file_name", r"^(CL\d+)", 1))
    .withColumn("claim_id",
                F.when(F.col("claim_id_header") != "", F.col("claim_id_header"))
                 .when(F.col("claim_id_file") != "", F.col("claim_id_file")))
    .withColumn("note_type",
                F.when(F.col("file_name").contains("_decision"), "decision").otherwise("assessment"))
    .withColumn("policy_id", F.regexp_extract(txt, r"Policy ID:\s*(P\d+)", 1))
    .withColumn("adjuster", F.regexp_extract(txt, r"Adjuster:\s*([^\r\n]+)", 1))
    .withColumn("note_date", F.to_date(F.regexp_extract(txt, r"Date:\s*(\d{4}-\d{2}-\d{2})", 1)))
    .withColumn("claim_type", F.regexp_extract(txt, r"Claim Type:\s*([^\r\n]+)", 1))
    .withColumn("note_text", body)
)
display(parsed.select("file_name", "claim_id", "note_type", "adjuster", "note_date", "note_text").limit(5))

# Step 3: turn the text into features

RED_FLAG_PHRASES = [
    "differs from the witness", "inconsistent with the reported", "shortly after the policy",
    "multiple prior claims", "reluctant to provide", "unusually high",
]
lower_body = F.lower(F.col("note_text"))
red_flag_count = sum([F.when(lower_body.contains(p), 1).otherwise(0) for p in RED_FLAG_PHRASES])

features = (parsed
    .withColumn("note_amount",
                F.regexp_replace(F.regexp_extract("note_text", r"\$([0-9,]+\.[0-9]{2})", 1), ",", "")
                 .cast("double"))
    .withColumn("police_report_status",
                F.when(F.col("note_text").rlike(r"(?i)report (no\.|number)"), "filed")
                 .when(F.col("note_text").rlike(r"(?i)no police report|not yet provided a police report"),
                       "not_available")
                 .otherwise("unknown"))
    .withColumn("injury_mentioned",
                F.when(F.col("note_text").rlike(r"(?i)minor injury|neck pain"), True)
                 .when(F.col("note_text").rlike(r"(?i)no injuries"), False))
    .withColumn("red_flag_count", red_flag_count)
    .withColumn("note_quality", F.when(F.length("note_text") < 25, "poor").otherwise("ok"))
)

# Step 4: compare with the structured claim

claims = (spark.read.format("delta").load(f"{SILVER}/claims")
          .select("claim_id", F.col("claim_amount").alias("claim_amount_structured"),
                  F.lit(True).alias("_in_silver")))
joined = features.join(claims, "claim_id", "left")

# Step 5: rules, quarantine, MERGE

reject_rules = {"no_claim_id": "claim_id IS NULL"}
flag_rules = {
    "claim_not_in_silver": "_in_silver IS NULL",
    "note_amount_mismatch": ("note_type = 'assessment' AND note_amount IS NOT NULL "
                             "AND claim_amount_structured IS NOT NULL "
                             "AND abs(note_amount - claim_amount_structured) / claim_amount_structured > 0.25"),
    "poor_note_quality": "note_quality = 'poor'",
}
data_cols = ["file_name", "value"]
helper_cols = ["value", "claim_id_header", "claim_id_file", "claim_amount_structured", "_in_silver"]

finish_table(TABLE, batch, joined, data_cols, reject_rules,
             keys=["file_name"], order_col="_bronze_ts",
             flag_rules=flag_rules, drop_cols=helper_cols)

# Step 6: check the result

out = spark.read.format("delta").load(f"{SILVER}/{TABLE}")
print("Silver notes:", out.count())
display(out.groupBy("note_type", "red_flag_count").count().orderBy("note_type", "red_flag_count"))
display(out.filter("dq_flags IS NOT NULL").select("file_name", "claim_id", "dq_flags").limit(10))
