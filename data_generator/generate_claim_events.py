"""
Claim events generator (standard library only) - STREAMING add-on data.
Run AFTER generate_insureflow_data.py:
    python generate_claim_events.py

Reads the daily claims files and turns them into a live-style event stream:
    ClaimSubmitted -> DocumentsReceived -> ClaimApproved / ClaimRejected

Output: insureflow_data/streaming/claim_events_YYYYMMDD.jsonl
(one JSON event per line; later replayed into Azure Event Hubs)

Real-world streaming messiness included:
  - ~2% duplicate events (at-least-once delivery)  -> dedupe on event_id
  - ~3% late / out-of-order events                 -> handle with event_ts
"""
import csv
import glob
import json
import os
import random
from datetime import datetime, timedelta

random.seed(7)
BASE = "insureflow_data"
os.makedirs(f"{BASE}/streaming", exist_ok=True)

n = 1  # global event counter


def ev(claim, etype, ts, extra=None):
    global n
    e = {"event_id": f"EV{n:08d}", "claim_id": claim["claim_id"],
         "policy_id": claim["policy_id"], "event_type": etype,
         "event_ts": ts.strftime("%Y-%m-%d %H:%M:%S"), "source": "claims-portal"}
    if extra:
        e.update(extra)
    n += 1
    return e


files = sorted(glob.glob(f"{BASE}/raw/claims/claims_2025*.csv"))
if not files:
    raise SystemExit("Run generate_insureflow_data.py first.")

for path in files:
    tag = os.path.basename(path).split("_")[1].split(".")[0]
    day = datetime.strptime(tag, "%Y%m%d")
    events, seen = [], set()
    for row in csv.DictReader(open(path)):
        if row["claim_id"] in seen:
            continue
        seen.add(row["claim_id"])
        if row["settlement_date"] == "" and row["claim_date"] == day.strftime("%Y-%m-%d"):
            t = day + timedelta(hours=random.randint(8, 17), minutes=random.randint(0, 59))
            events.append(ev(row, "ClaimSubmitted", t,
                             {"claim_amount": float(row["claim_amount"]),
                              "claim_reason": row["claim_reason"]}))
            if random.random() < 0.7:
                events.append(ev(row, "DocumentsReceived",
                                 t + timedelta(hours=random.randint(1, 6))))
        elif row["claim_status"] in ("Approved", "Rejected") and \
                row["settlement_date"] == day.strftime("%Y-%m-%d"):
            t = day + timedelta(hours=random.randint(9, 18), minutes=random.randint(0, 59))
            extra = {"approved_amount": float(row["approved_amount"])} \
                if row["claim_status"] == "Approved" else None
            events.append(ev(row, "Claim" + row["claim_status"], t, extra))

    events.sort(key=lambda e: e["event_ts"])
    # duplicates (at-least-once delivery)
    for e in random.sample(events, max(1, len(events) // 50)):
        events.append(dict(e))
    # late / out-of-order arrival
    for i in random.sample(range(len(events)), max(1, len(events) * 3 // 100)):
        events.append(events.pop(i))

    with open(f"{BASE}/streaming/claim_events_{tag}.jsonl", "w") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")
    print(f"claim_events_{tag}.jsonl  ({len(events)} events)")
