"""
InsureFlow synthetic data generator v2 (standard library only).
Run:  python generate_insureflow_data.py
Output: ./insureflow_data/

Simulates a real insurer:
  HISTORY  : everything up to 2025-06-30  (your one-time INITIAL load)
  DAILY    : 7 days after that (2025-07-01 .. 07-07), one "day" at a time

  sql_source/create_tables.sql        DDL for Azure SQL
  sql_source/initial/*.csv            brokers, customers, policies, invoices (initial load)
  sql_source/daily/apply_YYYYMMDD.sql the day's inserts/updates for Azure SQL
  raw/claims/                         claims_initial.csv + claims_YYYYMMDD.csv
  raw/payments/                       payments_initial.csv + payments_YYYYMMDD.csv
  api/                                fraud_scores_*.json (host on GitHub for mock REST API)

Built-in real-world messiness:
  duplicates, nulls, invalid emails, bad dates, negative premiums,
  approved > claimed, orphan policy_ids, claim status updates (same claim_id
  arriving again), duplicate rows inside daily files, and SCHEMA DRIFT
  (day 5 claims file gets an extra column).
"""
import csv
import json
import os
import random
from datetime import date, timedelta

random.seed(42)

OUT = "insureflow_data"
HIST_END = date(2025, 6, 30)
DAILY_START = date(2025, 7, 1)
N_DAYS = 7
N_BROKERS, N_CUSTOMERS, N_POLICIES = 50, 5000, 8000

FIRST = ["Aarav", "Liam", "Olivia", "Noah", "Emma", "Sophia", "Mason", "Ava",
         "Ethan", "Isabella", "Lucas", "Mia", "Priya", "Rohan", "Ananya", "Kabir"]
LAST = ["Smith", "Johnson", "Brown", "Patel", "Garcia", "Miller", "Davis",
        "Sharma", "Wilson", "Moore", "Taylor", "Reddy", "Khan", "Lee"]
CITIES = [("New York", "NY"), ("Los Angeles", "CA"), ("Chicago", "IL"),
          ("Houston", "TX"), ("Phoenix", "AZ"), ("Dallas", "TX"),
          ("Miami", "FL"), ("Seattle", "WA"), ("Boston", "MA"), ("Denver", "CO")]
REGIONS = {"NY": "Northeast", "MA": "Northeast", "CA": "West", "WA": "West",
           "AZ": "West", "CO": "West", "IL": "Midwest", "TX": "South", "FL": "South"}
POLICY_TYPES = {
    "Auto": ((10000, 100000), (600, 2500)),
    "Home": ((100000, 600000), (800, 3500)),
    "Health": ((50000, 500000), (2000, 8000)),
    "Life": ((100000, 1000000), (500, 4000)),
}
CLAIM_REASONS = {
    "Auto": ["Collision", "Theft", "Windshield Damage", "Vandalism"],
    "Home": ["Fire", "Water Damage", "Burglary", "Storm Damage"],
    "Health": ["Hospitalization", "Surgery", "Emergency Visit", "Prescription"],
    "Life": ["Death Benefit", "Terminal Illness"],
}
NOTES = ["Photos received", "Awaiting police report", "Adjuster assigned",
         "Documents verified", "Customer called twice"]


def rdate(a, b):
    return a + timedelta(days=random.randint(0, (b - a).days))


def write_csv(path, header, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    print(f"{path}  ({len(rows)} rows)")


def write_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=1, default=str)
    print(f"{path}  ({len(data)} records)")


def q(v):
    """SQL literal."""
    if v is None or v == "":
        return "NULL"
    if isinstance(v, (int, float)):
        return str(v)
    return "'" + str(v).replace("'", "''") + "'"


def ts(d, hh=9):
    return f"{d} {hh:02d}:00:00"


def fraud_record(claim_id, d):
    s = round(random.betavariate(6, 2.5) if random.random() < 0.05 else random.betavariate(1.2, 8), 3)
    band = "High" if s >= 0.6 else "Medium" if s >= 0.3 else "Low"
    return {"claim_id": claim_id, "fraud_score": s, "risk_band": band,
            "scored_at": ts(d, 18)}


def make_claim(cid, p, filed):
    claimed = round(random.uniform(0.01, 0.4) * p[4], 2)
    status = random.choices(["Approved", "Rejected", "Pending", "Under Review"],
                            [55, 15, 15, 15])[0]
    approved = round(claimed * random.uniform(0.6, 1.0), 2) if status == "Approved" else 0
    settled = filed + timedelta(days=random.randint(5, 60)) if status in ("Approved", "Rejected") else ""
    if settled and settled > HIST_END:  # cannot be settled in the future -> still open
        status, approved, settled = random.choice(["Pending", "Under Review"]), 0, ""
    last_mod = ts(settled if settled else filed, 16)
    return [f"CL{cid:07d}", p[0], filed, settled, random.choice(CLAIM_REASONS[p[3]]),
            claimed, approved, status, last_mod]


CLAIM_HDR = ["claim_id", "policy_id", "claim_date", "settlement_date", "claim_reason",
             "claim_amount", "approved_amount", "claim_status", "last_modified"]
PAY_HDR = ["payment_id", "invoice_id", "policy_id", "due_date", "paid_date", "amount",
           "payment_method", "status"]
INV_HDR = ["invoice_id", "policy_id", "customer_id", "invoice_date", "due_date",
           "amount", "status", "last_modified"]

# =====================================================================
# 1. SQL DDL
# =====================================================================
os.makedirs(f"{OUT}/sql_source", exist_ok=True)
with open(f"{OUT}/sql_source/create_tables.sql", "w") as f:
    f.write("""-- Azure SQL: the "policy admin system"
CREATE TABLE brokers (
  broker_id VARCHAR(10) PRIMARY KEY, broker_name VARCHAR(100), region VARCHAR(20),
  commission_rate DECIMAL(5,3), hire_date DATE);

CREATE TABLE customers (
  customer_id VARCHAR(10), first_name VARCHAR(50), last_name VARCHAR(50),
  dob DATE, gender CHAR(1), email VARCHAR(150), city VARCHAR(50), state CHAR(2),
  signup_date DATE, credit_score INT, last_modified DATETIME2);
-- (no PK on purpose: initial data contains duplicate customers, like a messy legacy system)

CREATE TABLE policies (
  policy_id VARCHAR(10), customer_id VARCHAR(10), broker_id VARCHAR(10),
  policy_type VARCHAR(20), coverage_amount DECIMAL(12,2), annual_premium DECIMAL(10,2),
  start_date DATE, end_date DATE, status VARCHAR(20), last_modified DATETIME2);

CREATE TABLE invoices (  -- premium invoices issued to customers (billing)
  invoice_id VARCHAR(12), policy_id VARCHAR(10), customer_id VARCHAR(10),
  invoice_date DATE, due_date DATE, amount DECIMAL(10,2),
  status VARCHAR(20), last_modified DATETIME2);
""")

# =====================================================================
# 2. HISTORY (initial load)
# =====================================================================
brokers = []
for i in range(1, N_BROKERS + 1):
    _, st = random.choice(CITIES)
    brokers.append([f"BR{i:04d}", f"{random.choice(FIRST)} {random.choice(LAST)}",
                   REGIONS[st], round(random.uniform(0.03, 0.12), 3),
                   rdate(date(2010, 1, 1), date(2023, 1, 1))])
write_csv(f"{OUT}/sql_source/initial/brokers.csv",
          ["broker_id", "broker_name", "region", "commission_rate", "hire_date"], brokers)

customers = []
for i in range(1, N_CUSTOMERS + 1):
    city, st = random.choice(CITIES)
    fn, ln = random.choice(FIRST), random.choice(LAST)
    signup = rdate(date(2018, 1, 1), HIST_END)
    customers.append([f"C{i:06d}", fn, ln, rdate(date(1950, 1, 1), date(2005, 12, 31)),
                      random.choice(["M", "F"]), f"{fn}.{ln}{i}@example.com".lower(),
                      city, st, signup, random.randint(450, 850), ts(signup, 8)])
for _ in range(60):
    customers[random.randrange(len(customers))][5] = ""
for _ in range(40):
    customers[random.randrange(len(customers))][5] = "not_an_email"
for _ in range(50):
    customers.append(list(random.choice(customers)))
write_csv(f"{OUT}/sql_source/initial/customers.csv",
          ["customer_id", "first_name", "last_name", "dob", "gender", "email", "city",
           "state", "signup_date", "credit_score", "last_modified"], customers)

policies = []
for i in range(1, N_POLICIES + 1):
    ptype = random.choice(list(POLICY_TYPES))
    (cmin, cmax), (pmin, pmax) = POLICY_TYPES[ptype]
    start = rdate(date(2020, 1, 1), HIST_END)
    end = start + timedelta(days=365)
    status = "Active" if end > HIST_END else random.choice(["Expired", "Cancelled", "Renewed"])
    policies.append([f"P{i:07d}", f"C{random.randint(1, N_CUSTOMERS):06d}",
                     f"BR{random.randint(1, N_BROKERS):04d}", ptype,
                     round(random.uniform(cmin, cmax), -2),
                     round(random.uniform(pmin, pmax), 2), start, end, status, ts(start, 9)])
for _ in range(25):
    r = random.choice(policies)
    r[7] = r[6] - timedelta(days=30)
for _ in range(15):
    random.choice(policies)[5] *= -1
write_csv(f"{OUT}/sql_source/initial/policies.csv",
          ["policy_id", "customer_id", "broker_id", "policy_type", "coverage_amount",
           "annual_premium", "start_date", "end_date", "status", "last_modified"], policies)

# --- initial invoices + payments ---
# every monthly premium installment gets an invoice; payments reference the invoice
payments, invoices, pid, iid = [], [], 1, 1
open_inv = []  # invoices still unpaid at the cutoff (paid during the daily simulation)
for p in policies:
    if p[5] <= 0:
        continue
    monthly = round(p[5] / 12, 2)
    for m in range(12):
        due = p[6] + timedelta(days=30 * m)
        if due > HIST_END:
            break
        r = random.random()
        amount, inv_status = monthly, "Paid"
        if r < 0.86:
            paid, status = due + timedelta(days=random.randint(-3, 5)), "Paid"
        elif r < 0.93:
            paid, status = due + timedelta(days=random.randint(10, 40)), "Late"
        elif r < 0.95:
            paid, status = due + timedelta(days=random.randint(0, 10)), "Partial"
            amount, inv_status = round(monthly * 0.5, 2), "Partially Paid"
        else:
            paid, status = "", "Missed"
        if paid and paid > HIST_END:
            paid, status = "", "Missed"
        if status == "Missed":
            inv_status = "Overdue"
        inv = [f"INV{iid:08d}", p[0], p[1], due - timedelta(days=15), due, monthly,
               inv_status, ts(paid if paid else due, 15)]
        invoices.append(inv)
        if status == "Missed" and due >= HIST_END - timedelta(days=45):
            open_inv.append(inv)  # may still be paid late during the daily window
        payments.append([f"PAY{pid:08d}", inv[0], p[0], due, paid, amount,
                         random.choice(["Card", "ACH", "Cheque", "UPI"]), status])
        pid += 1
        iid += 1
    if p[8] == "Active":  # next upcoming invoice (issued, not yet paid)
        due = HIST_END + timedelta(days=random.randint(4, 34))
        inv = [f"INV{iid:08d}", p[0], p[1], due - timedelta(days=15), due, monthly,
               "Issued", ts(due - timedelta(days=15), 15)]
        invoices.append(inv)
        open_inv.append(inv)
        iid += 1
# data quality problems
for _ in range(10):  # payment points to an invoice that does not exist
    random.choice(payments)[1] = "INV99999999"
for _ in range(8):   # due date earlier than invoice date
    r = random.choice(invoices)
    r[4] = r[3] - timedelta(days=5)
write_csv(f"{OUT}/sql_source/initial/invoices.csv", INV_HDR, invoices)
write_csv(f"{OUT}/raw/payments/payments_initial.csv", PAY_HDR, payments)

# --- initial claims + fraud scores ---
claims, cid, fraud = [], 1, []
for p in policies:
    if random.random() > 0.25 or p[6] > HIST_END:
        continue
    for _ in range(random.choice([1, 1, 1, 2, 3])):
        lo, hi = p[6], min(p[7], HIST_END)
        if hi < lo:
            hi = lo
        c = make_claim(cid, p, rdate(lo, hi))
        claims.append(c)
        if random.random() > 0.05:  # ~5% of claims never get scored
            fraud.append(fraud_record(c[0], c[2]))
        cid += 1
for _ in range(20):
    r = random.choice(claims)
    r[6] = round(r[5] * 1.5, 2)
for _ in range(15):
    r = random.choice(claims)
    if r[3] != "":
        r[3] = r[2] - timedelta(days=5)
for _ in range(10):
    random.choice(claims)[1] = "P9999999"
write_csv(f"{OUT}/raw/claims/claims_initial.csv", CLAIM_HDR, claims)
write_json(f"{OUT}/api/fraud_scores_initial.json", fraud)

# =====================================================================
# 3. DAILY SIMULATION
# =====================================================================
next_cust = N_CUSTOMERS + 1
next_pol = N_POLICIES + 1

for d_i in range(N_DAYS):
    day = DAILY_START + timedelta(days=d_i)
    tag = day.strftime("%Y%m%d")
    active = [p for p in policies if p[8] == "Active" and p[5] > 0 and p[6] <= day]

    # ---- SQL changes (apply to Azure SQL, then run ADF incremental) ----
    sql = [f"-- Day {d_i + 1}: {day}  (run in Azure SQL, then trigger ADF)\n"]
    for _ in range(random.randint(8, 14)):  # new customers
        city, st = random.choice(CITIES)
        fn, ln = random.choice(FIRST), random.choice(LAST)
        row = [f"C{next_cust:06d}", fn, ln, rdate(date(1950, 1, 1), date(2005, 12, 31)),
               random.choice(["M", "F"]), f"{fn}.{ln}{next_cust}@example.com".lower(),
               city, st, day, random.randint(450, 850), ts(day, 10)]
        sql.append("INSERT INTO customers VALUES (" + ",".join(q(v) for v in row) + ");")
        next_cust += 1
    for _ in range(random.randint(6, 10)):  # customers move (SCD2 fodder)
        city, st = random.choice(CITIES)
        cu = f"C{random.randint(1, N_CUSTOMERS):06d}"
        sql.append(f"UPDATE customers SET city={q(city)}, state={q(st)}, "
                   f"last_modified={q(ts(day, 11))} WHERE customer_id={q(cu)};")
    for _ in range(random.randint(20, 30)):  # new policies
        ptype = random.choice(list(POLICY_TYPES))
        (cmin, cmax), (pmin, pmax) = POLICY_TYPES[ptype]
        row = [f"P{next_pol:07d}", f"C{random.randint(1, next_cust - 1):06d}",
               f"BR{random.randint(1, N_BROKERS):04d}", ptype,
               round(random.uniform(cmin, cmax), -2), round(random.uniform(pmin, pmax), 2),
               day, day + timedelta(days=365), "Active", ts(day, 12)]
        policies.append(row)
        active.append(row)
        sql.append("INSERT INTO policies VALUES (" + ",".join(q(v) for v in row) + ");")
        next_pol += 1
        due = day + timedelta(days=14)
        inv = [f"INV{iid:08d}", row[0], row[1], day, due, round(row[5] / 12, 2),
               "Issued", ts(day, 12)]
        open_inv.append(inv)
        sql.append("INSERT INTO invoices VALUES (" + ",".join(q(v) for v in inv) + ");")
        iid += 1
    for p in random.sample(active, 4):  # cancellations
        p[8] = "Cancelled"
        sql.append(f"UPDATE policies SET status='Cancelled', last_modified={q(ts(day, 14))} "
                   f"WHERE policy_id={q(p[0])};")
    active = [p for p in active if p[8] == "Active"]

    # ---- daily claims file: new claims + status updates of old ones ----
    todays = []
    day_fraud = []
    for p in random.sample(active, 40):
        c = make_claim(cid, p, day)
        c[3], c[7], c[6] = "", random.choice(["Pending", "Under Review"]), 0
        c[8] = ts(day, 16)
        claims.append(c)
        todays.append(list(c))
        day_fraud.append(fraud_record(c[0], day))
        cid += 1
    open_claims = [c for c in claims if c[7] in ("Pending", "Under Review") and c[2] < day]
    for c in random.sample(open_claims, min(25, len(open_claims))):  # same claim_id again
        c[7] = random.choice(["Approved", "Rejected"])
        c[3] = day
        c[6] = round(c[5] * random.uniform(0.6, 1.0), 2) if c[7] == "Approved" else 0
        c[8] = ts(day, 16)
        todays.append(list(c))
    todays.append(list(random.choice(todays)))  # duplicate row in the file
    hdr = list(CLAIM_HDR)
    if d_i == 4:  # SCHEMA DRIFT on day 5
        hdr.append("adjuster_notes")
        for r in todays:
            r.append(random.choice(NOTES))
    write_csv(f"{OUT}/raw/claims/claims_{tag}.csv", hdr, todays)
    write_json(f"{OUT}/api/fraud_scores_{tag}.json", day_fraud)

    # ---- daily payments file: customers pay open invoices ----
    todays_pay = []
    for inv in [i for i in open_inv if i[4] <= day + timedelta(days=3)]:
        delay = (day - inv[4]).days
        if random.random() > (0.88 if delay <= 3 else 0.12):
            continue  # not paid today; stays open (paid late later, or goes overdue)
        partial = random.random() < 0.02
        amount = round(inv[5] * 0.5, 2) if partial else inv[5]
        todays_pay.append([f"PAY{pid:08d}", inv[0], inv[1], inv[4], day, amount,
                           random.choice(["Card", "ACH", "Cheque", "UPI"]),
                           "Partial" if partial else ("Late" if delay > 5 else "Paid")])
        pid += 1
        inv[6], inv[7] = ("Partially Paid" if partial else "Paid"), ts(day, 15)
        open_inv.remove(inv)
        sql.append(f"UPDATE invoices SET status={q(inv[6])}, last_modified={q(inv[7])} "
                   f"WHERE invoice_id={q(inv[0])};")
    for inv in open_inv:  # unpaid 10+ days past due -> overdue
        if inv[6] == "Issued" and inv[4] < day - timedelta(days=10):
            inv[6], inv[7] = "Overdue", ts(day, 15)
            sql.append(f"UPDATE invoices SET status='Overdue', last_modified={q(inv[7])} "
                       f"WHERE invoice_id={q(inv[0])};")
    write_csv(f"{OUT}/raw/payments/payments_{tag}.csv", PAY_HDR, todays_pay)

    # ---- write the day's SQL change script (apply to Azure SQL, then run ADF) ----
    os.makedirs(f"{OUT}/sql_source/daily", exist_ok=True)
    with open(f"{OUT}/sql_source/daily/apply_{tag}.sql", "w") as f:
        f.write("\n".join(sql) + "\n")

print("\nDone -> ./insureflow_data/")
