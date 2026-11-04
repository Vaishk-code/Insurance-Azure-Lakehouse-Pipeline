# Data generator

Creates synthetic insurance data (no real company data is used).

    python generate_insureflow_data.py

Output goes to `insureflow_data/` (git-ignored): an initial history load plus
7 simulated daily batches (new/updated records, claim status updates,
duplicates, and a schema-drift day). See `docs/business_requirements.md`
for the planted data-quality problems.

## Streaming add-on

    python generate_claim_events.py

Run after the main generator. Turns the daily claims files into claim events
(`ClaimSubmitted`, `DocumentsReceived`, `ClaimApproved`/`ClaimRejected`) in
`insureflow_data/streaming/*.jsonl`, with duplicate and out-of-order events
to practice streaming deduplication. Events are later replayed into Event Hubs.

## Unstructured add-on

    python generate_adjuster_notes.py

Run after the main generator. Creates free-text adjuster notes as `.txt` files in
`insureflow_data/unstructured/adjuster_notes/` (a folder `initial/` plus one folder per day).
Notes mention incident details, amounts, police reports, and (more often for high fraud
scores) red-flag language. Includes messy cases: missing Claim ID header, nearly empty
notes, ALL CAPS notes, and amounts that do not match the claim.

Run order: `generate_insureflow_data.py`, then `generate_claim_events.py`, then `generate_adjuster_notes.py`.
