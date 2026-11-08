 Step 1: create the database;
   --------------------------------------------------------------------- 
CREATE DATABASE insurance_gold;
GO
 ---------------------------------------------------------------------
   Step 2: one view per Gold table 
------------------------------------------------------------------------
CREATE VIEW dbo.dim_date AS
SELECT * FROM OPENROWSET(
    BULK 'https://insurancelake.dfs.core.windows.net/gold/dim_date/',
    FORMAT = 'DELTA') AS r;
GO

CREATE VIEW dbo.dim_broker AS
SELECT * FROM OPENROWSET(
    BULK 'https://insurancelake.dfs.core.windows.net/gold/dim_broker/',
    FORMAT = 'DELTA') AS r;
GO

CREATE VIEW dbo.dim_policy AS
SELECT * FROM OPENROWSET(
    BULK 'https://insurancelake.dfs.core.windows.net/gold/dim_policy/',
    FORMAT = 'DELTA') AS r;
GO

CREATE VIEW dbo.dim_customer AS
SELECT * FROM OPENROWSET(
    BULK 'https://insurancelake.dfs.core.windows.net/gold/dim_customer/',
    FORMAT = 'DELTA') AS r;
GO

CREATE VIEW dbo.fact_claims AS
SELECT * FROM OPENROWSET(
    BULK 'https://insurancelake.dfs.core.windows.net/gold/fact_claims/',
    FORMAT = 'DELTA') AS r;
GO

CREATE VIEW dbo.fact_invoices AS
SELECT * FROM OPENROWSET(
    BULK 'https://insurancelake.dfs.core.windows.net/gold/fact_invoices/',
    FORMAT = 'DELTA') AS r;
GO

CREATE VIEW dbo.fact_premium_payments AS
SELECT * FROM OPENROWSET(
    BULK 'https://insurancelake.dfs.core.windows.net/gold/fact_premium_payments/',
    FORMAT = 'DELTA') AS r;
GO


---------------------------------------------------------------------
   Step 3: business metric views 
--------------------------------------------------------------------- 

 1. Loss ratio = approved claims / premium received, by month and policy type
CREATE VIEW dbo.vw_loss_ratio AS
WITH claims_m AS (
    SELECT d.[year] AS yr, d.[month] AS mth, p.policy_type,
           SUM(fc.approved_amount) AS approved_amount
    FROM dbo.fact_claims fc
    JOIN dbo.dim_date d   ON fc.claim_date_key = d.date_key
    JOIN dbo.dim_policy p ON fc.policy_key = p.policy_key
    GROUP BY d.[year], d.[month], p.policy_type
),
premium_m AS (
    SELECT d.[year] AS yr, d.[month] AS mth, p.policy_type,
           SUM(fp.amount) AS premium_received
    FROM dbo.fact_premium_payments fp
    JOIN dbo.fact_invoices fi ON fp.invoice_id = fi.invoice_id
    JOIN dbo.dim_policy p     ON fi.policy_key = p.policy_key
    JOIN dbo.dim_date d       ON fp.paid_date_key = d.date_key
    WHERE fp.status IN ('Paid', 'Late', 'Partial')
    GROUP BY d.[year], d.[month], p.policy_type
)
SELECT c.yr AS [year], c.mth AS [month],
       CONCAT(c.yr, '-', RIGHT('0' + CAST(c.mth AS VARCHAR(2)), 2)) AS year_month,
       c.policy_type, c.approved_amount, m.premium_received,
       ROUND(CAST(c.approved_amount AS FLOAT) / NULLIF(CAST(m.premium_received AS FLOAT), 0), 3) AS loss_ratio
FROM claims_m c
JOIN premium_m m ON c.yr = m.yr AND c.mth = m.mth AND c.policy_type = m.policy_type;
GO
------------------------------------------------------------------------------------------
 2. Average settlement days by claim reason (settled claims only)
CREATE VIEW dbo.vw_settlement_days AS
SELECT claim_reason,
       COUNT(*) AS settled_claims,
       ROUND(AVG(CAST(settlement_days AS FLOAT)), 1) AS avg_settlement_days
FROM dbo.fact_claims
WHERE settlement_days IS NOT NULL
GROUP BY claim_reason;
GO
---------------------------------------------------------------------------------------------
 3. Collection rate by due month (on time = paid within 5 days after the due date)
CREATE VIEW dbo.vw_collection_rate AS
SELECT d.[year], d.[month],
       CONCAT(d.[year], '-', RIGHT('0' + CAST(d.[month] AS VARCHAR(2)), 2)) AS year_month,
       COUNT(*) AS invoices_due,
       SUM(CASE WHEN fi.aging_bucket = 'Paid' THEN 1 ELSE 0 END) AS fully_paid,
       SUM(CASE WHEN fi.aging_bucket = 'Paid' AND fi.days_to_pay <= 5 THEN 1 ELSE 0 END) AS paid_on_time,
       ROUND(CAST(SUM(CASE WHEN fi.aging_bucket = 'Paid' THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*), 3) AS collection_rate,
       ROUND(CAST(SUM(CASE WHEN fi.aging_bucket = 'Paid' AND fi.days_to_pay <= 5 THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*), 3) AS on_time_rate
FROM dbo.fact_invoices fi
JOIN dbo.dim_date d ON fi.due_date_key = d.date_key
WHERE fi.due_date_key <= 20250705
GROUP BY d.[year], d.[month];
GO
-------------------------------------------------------------------------------------------------------------------------------
 4. High-risk claims by broker region
CREATE VIEW dbo.vw_high_risk_by_region AS
SELECT b.region,
       COUNT(*) AS claims,
       SUM(CASE WHEN fc.risk_band = 'High' THEN 1 ELSE 0 END) AS high_risk_claims,
       ROUND(CAST(SUM(CASE WHEN fc.risk_band = 'High' THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*), 3) AS high_risk_share
FROM dbo.fact_claims fc
JOIN dbo.dim_broker b ON fc.broker_key = b.broker_key
GROUP BY b.region;
GO
--------------------------------------------------------------------------------------------------------------------------------
 5. Open claims: how many and how old (days at the as-of date)
CREATE VIEW dbo.vw_open_claims AS
SELECT fc.claim_status,
       COUNT(*) AS open_claims,
       ROUND(AVG(CAST(DATEDIFF(DAY, d.full_date, CAST('2025-07-05' AS DATE)) AS FLOAT)), 1) AS avg_age_days
FROM dbo.fact_claims fc
JOIN dbo.dim_date d ON fc.claim_date_key = d.date_key
WHERE fc.claim_status IN ('Pending', 'Under Review')
GROUP BY fc.claim_status;
GO
-----------------------------------------------------------------------------------------------------------------------------------------
 6. Customer location at claim date (only customers whose city changed since the claim)
CREATE VIEW dbo.vw_customer_location_at_claim AS
SELECT fc.claim_id, d.full_date AS claim_date, dc.customer_id,
       dc.city AS city_at_claim_date, cur.city AS current_city
FROM dbo.fact_claims fc
JOIN dbo.dim_customer dc  ON fc.customer_key = dc.customer_key
JOIN dbo.dim_customer cur ON cur.customer_id = dc.customer_id AND cur.is_current = 1
JOIN dbo.dim_date d       ON fc.claim_date_key = d.date_key
WHERE dc.city <> cur.city;
GO
-------------------------------------------------------------------------------------------------------------------------------------
7. Invoice aging: money still owed, by bucket (sort_order helps charts sort correctly)
CREATE VIEW dbo.vw_invoice_aging AS
SELECT aging_bucket,
       CASE aging_bucket WHEN 'Not yet due' THEN 0 WHEN '0-30' THEN 1
                         WHEN '31-60' THEN 2 ELSE 3 END AS sort_order,
       COUNT(*) AS invoices,
       SUM(outstanding_amount) AS outstanding_amount
FROM dbo.fact_invoices
WHERE aging_bucket <> 'Paid'
GROUP BY aging_bucket;
GO
----------------------------------------------------------------------------------------------------------------------------------------------
8. Adjuster note red flags versus the fraud API (for claims that have a note)
CREATE VIEW dbo.vw_note_vs_fraud AS
SELECT CASE WHEN red_flag_count > 0 THEN 'Note has red flag' ELSE 'No red flag in note' END AS note_signal,
       CASE WHEN risk_band = 'High' THEN 'API says High risk' ELSE 'API says not High' END AS api_signal,
       COUNT(*) AS claims
FROM dbo.fact_claims
WHERE has_adjuster_note = 1
GROUP BY CASE WHEN red_flag_count > 0 THEN 'Note has red flag' ELSE 'No red flag in note' END,
         CASE WHEN risk_band = 'High' THEN 'API says High risk' ELSE 'API says not High' END;
GO
-----------------------------------------------------------------------------------------------------------------------------------------
9. Headline numbers for dashboard cards
CREATE VIEW dbo.vw_kpis AS
SELECT (SELECT COUNT(*) FROM dbo.fact_claims) AS total_claims,
       (SELECT SUM(approved_amount) FROM dbo.fact_claims) AS total_approved_amount,
       (SELECT COUNT(*) FROM dbo.fact_claims WHERE risk_band = 'High') AS high_risk_claims,
       (SELECT SUM(outstanding_amount) FROM dbo.fact_invoices WHERE aging_bucket <> 'Paid') AS outstanding_premium;
GO
