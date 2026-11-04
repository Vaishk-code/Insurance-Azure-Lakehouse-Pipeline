"""
Adjuster notes generator (standard library only) - UNSTRUCTURED data.
Run AFTER generate_insureflow_data.py:
    python generate_adjuster_notes.py

Creates free-text claim notes as individual .txt files:
    insureflow_data/unstructured/adjuster_notes/initial/CL0000123.txt
    insureflow_data/unstructured/adjuster_notes/20250701/CL0003201.txt
    insureflow_data/unstructured/adjuster_notes/20250701/CL0003150_decision.txt

Notes are loosely linked to the fraud score (higher score -> more red-flag
language), so you can later compare what the text says with what the API says.

Real-world messiness included:
  - many small files (the "small files problem")
  - ~4% notes missing the Claim ID header line (fall back to the file name)
  - ~3% nearly empty notes ("See attached.")
  - ~5% notes typed in ALL CAPS
  - ~3% notes where the amount in the text is far from the claim amount
"""
import csv
import glob
import json
import os
import random

random.seed(11)
BASE = "insureflow_data"
OUT = f"{BASE}/unstructured/adjuster_notes"

ADJUSTERS = ["Jordan Lee", "Priya Nair", "Sam Carter", "Maria Lopez",
             "Chen Wei", "Alex Morgan", "Fatima Khan", "Daniel Brooks"]

INCIDENT = {
    "Collision": ["Insured vehicle was struck from behind while stopped at a traffic light.",
                  "Insured reports a collision at an intersection; the other driver ran a red light.",
                  "Vehicle hit a parked car in a parking lot while reversing."],
    "Theft": ["Insured reports the vehicle missing from the driveway overnight.",
              "Vehicle stolen from a shopping center parking lot; insured returned to find it gone."],
    "Windshield Damage": ["Windshield cracked by road debris on the highway.",
                          "A stone chip spread into a long crack across the windshield."],
    "Vandalism": ["Insured found scratches and a broken mirror after leaving the vehicle on the street.",
                  "Vehicle windows were smashed overnight while parked outside the residence."],
    "Fire": ["Fire started in the kitchen and spread to the adjoining room.",
             "Electrical fault suspected as the cause of a fire in the garage."],
    "Water Damage": ["Burst pipe flooded the ground floor while the insured was away.",
                     "Water leaked through the ceiling after heavy rain, damaging flooring."],
    "Burglary": ["Insured returned home to find a rear window broken and items missing.",
                 "Break-in reported; electronics and jewelry taken from the property."],
    "Storm Damage": ["High winds tore part of the roof and damaged the fence.",
                     "A fallen tree during the storm damaged the roof and gutters."],
    "Hospitalization": ["Insured was admitted for a three-day hospital stay.",
                        "Hospital admission following an acute illness; discharge summary received."],
    "Surgery": ["Insured underwent a planned surgical procedure at a network hospital.",
                "Surgery performed after specialist referral; pre-authorization on file."],
    "Emergency Visit": ["Insured visited the emergency department after a sudden injury.",
                        "Emergency room visit for severe pain; treated and discharged same day."],
    "Prescription": ["Claim for a prescribed long-term medication refill.",
                     "Prescription claim submitted for a new course of treatment."],
    "Death Benefit": ["Beneficiary submitted the claim form and death certificate.",
                      "Death benefit claim filed by the named beneficiary; documents under verification."],
    "Terminal Illness": ["Insured submitted a physician statement for an accelerated benefit claim.",
                         "Terminal illness benefit requested with supporting medical certificate."],
}
AMOUNT_LABEL = {
    "Hospitalization": "Hospital bill", "Surgery": "Hospital bill",
    "Emergency Visit": "Medical bill", "Prescription": "Medication cost",
    "Death Benefit": "Benefit claimed", "Terminal Illness": "Benefit claimed",
}
POLICE_REASONS = {"Collision", "Theft", "Vandalism", "Burglary", "Fire"}
INJURY_REASONS = {"Collision", "Fire", "Storm Damage"}

POLICE_YES = ["Police report filed (report no. {n}).",
              "Incident reported to police; report number {n}."]
POLICE_NO = ["No police report available.", "Insured has not yet provided a police report."]
INJURY = ["No injuries reported.", "Minor injury reported, medical bills to follow.",
          "Insured reports neck pain; medical evaluation pending."]
HEALTH_LIFE = {"Hospitalization", "Surgery", "Emergency Visit", "Prescription",
               "Death Benefit", "Terminal Illness"}
EVIDENCE_PROPERTY = ["Photos of the damage received.", "Photos and repair estimate received.",
                     "Awaiting supporting photos from the insured."]
EVIDENCE_MEDICAL = ["Medical records received.", "Medical records requested from the provider.",
                    "Supporting documents received."]
FOLLOW_COMMON = ["Pending review by the claims manager.",
                 "Recommend approval subject to document verification.",
                 "Customer contacted by phone; statement recorded."]
FOLLOW_PROPERTY = FOLLOW_COMMON + ["Adjuster will inspect on site this week."]
RED_FLAGS = ["Insured's account differs from the witness statement.",
             "Damage appears inconsistent with the reported incident.",
             "Claim filed shortly after the policy start date.",
             "Multiple prior claims noted on this policy.",
             "Insured was reluctant to provide supporting documents.",
             "Repair estimate appears unusually high."]
REJECT_REASONS = ["policy exclusion applies", "insufficient documentation",
                  "incident occurred outside the coverage period"]


def header(row, adjuster, date, drop_claim_id=False):
    lines = ["CLAIM NOTE"]
    if not drop_claim_id:
        lines.append(f"Claim ID: {row['claim_id']}")
    lines += [f"Policy ID: {row['policy_id']}", f"Adjuster: {adjuster}",
              f"Date: {date}", f"Claim Type: {row['claim_reason']}", ""]
    return lines


def build_note(row, fscore, date):
    reason = row["claim_reason"]
    amt = float(row["claim_amount"])
    shown = amt * random.uniform(0.92, 1.08)
    if random.random() < 0.03:
        shown = amt * random.uniform(1.5, 3.0)
    label = AMOUNT_LABEL.get(reason, "Estimated damage")
    parts = [random.choice(INCIDENT[reason]), f"{label}: ${shown:,.2f}."]
    if reason in POLICE_REASONS:
        parts.append(random.choice(POLICE_YES).format(n=random.randint(100000, 999999))
                     if random.random() < 0.7 else random.choice(POLICE_NO))
    if reason in INJURY_REASONS:
        parts.append(random.choice(INJURY))
    parts.append(random.choice(EVIDENCE_MEDICAL if reason in HEALTH_LIFE else EVIDENCE_PROPERTY))
    p_red = 0.6 if fscore >= 0.6 else 0.25 if fscore >= 0.3 else 0.04
    if random.random() < p_red:
        parts += random.sample(RED_FLAGS, random.randint(1, 3))
    parts.append(random.choice(FOLLOW_COMMON if reason in HEALTH_LIFE else FOLLOW_PROPERTY))
    text = " ".join(parts)
    if random.random() < 0.03:
        text = "See attached."
    elif random.random() < 0.05:
        text = text.upper()
    lines = header(row, random.choice(ADJUSTERS), date,
                   drop_claim_id=random.random() < 0.04)
    return "\n".join(lines + [text]) + "\n"


def build_decision(row, date):
    if row["claim_status"] == "Approved":
        text = (f"Claim reviewed and approved. Approved amount: "
                f"${float(row['approved_amount']):,.2f}. Payment to be released.")
    else:
        text = f"Claim rejected after review. Reason: {random.choice(REJECT_REASONS)}."
    return "\n".join(header(row, random.choice(ADJUSTERS), date) + [text]) + "\n"


def load_fraud(pattern):
    scores = {}
    for f in glob.glob(pattern):
        for r in json.load(open(f)):
            scores[r["claim_id"]] = r["fraud_score"]
    return scores


def save(folder, name, content):
    os.makedirs(folder, exist_ok=True)
    with open(f"{folder}/{name}", "w") as f:
        f.write(content)


fraud = load_fraud(f"{BASE}/api/fraud_scores_*.json")
if not fraud:
    raise SystemExit("Run generate_insureflow_data.py first.")

# ---- initial: notes for ~25% of historical claims ----
count = 0
for row in csv.DictReader(open(f"{BASE}/raw/claims/claims_initial.csv")):
    if random.random() < 0.25:
        save(f"{OUT}/initial", f"{row['claim_id']}.txt",
             build_note(row, fraud.get(row["claim_id"], 0), row["claim_date"]))
        count += 1
print(f"initial: {count} notes")

# ---- daily: notes on new claims + decision notes on settled ones ----
for path in sorted(glob.glob(f"{BASE}/raw/claims/claims_2025*.csv")):
    tag = os.path.basename(path).split("_")[1].split(".")[0]
    day = f"{tag[:4]}-{tag[4:6]}-{tag[6:]}"
    new_n = dec_n = 0
    seen = set()
    for row in csv.DictReader(open(path)):
        if row["claim_id"] in seen:
            continue
        seen.add(row["claim_id"])
        if row["settlement_date"] == "" and row["claim_date"] == day:
            if random.random() < 0.5:
                save(f"{OUT}/{tag}", f"{row['claim_id']}.txt",
                     build_note(row, fraud.get(row["claim_id"], 0), day))
                new_n += 1
        elif row["claim_status"] in ("Approved", "Rejected") and row["settlement_date"] == day:
            if random.random() < 0.6:
                save(f"{OUT}/{tag}", f"{row['claim_id']}_decision.txt", build_decision(row, day))
                dec_n += 1
    print(f"{tag}: {new_n} new-claim notes, {dec_n} decision notes")
