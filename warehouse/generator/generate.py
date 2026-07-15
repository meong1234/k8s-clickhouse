#!/usr/bin/env python3
"""Nimbus synthetic-data generator — tier 1 (the referential backbone).

Deterministic (seeded), stdlib-only. Emits the coherent core of the warehouse as
CSVs (CSVWithNames-ready) that `make wh-generate` streams into the bronze tables:

    customers -> kyc_events -> accounts -> account_events -> cards -> ledger_postings

The high-volume, low-consistency streams (app_events, card_authorizations) are NOT
generated here — they are bulk-synthesized in ClickHouse by warehouse/loaders/*.sql,
reading these tables so referential integrity holds.

Why stdlib-only (no Faker): zero install friction (the generator runs from bare
`python3`, no venv), and every value we need — names, emails, weighted picks — is a
few lines of `random`. Determinism is a hard requirement (P1 acceptance: regenerating
yields an identical raw_customers checksum); a fixed `random.Random(seed)` gives us
that, and pinning nothing keeps it reproducible across machines.

Design notes
------------
* Amounts are integer minor units (cents). Never floats.
* The ledger is balanced double-entry: every transaction_id emits a customer leg and
  an offsetting Nimbus internal/settlement leg, so signed postings sum to zero.
* Balances never go below zero: each account is simulated in strict time order and a
  debit is capped to the available balance (skipped if the balance is <= 0). No
  overdraft is modelled, which makes the "no unexplained negative balance" invariant
  trivially hold.
* The reference window is FIXED (not "now") so runs are reproducible.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import os
import random

# ---------------------------------------------------------------------------
# Fixed reference window — 18 months. Static so regeneration is deterministic.
# ---------------------------------------------------------------------------
WINDOW_START = dt.datetime(2025, 1, 1, 0, 0, 0)
WINDOW_END = dt.datetime(2026, 6, 30, 23, 59, 59)

# Scale presets. `customers` drives everything else; ledger volume falls out of the
# per-account monthly transaction rates below. medium == the laptop-real target
# (~5k customers, ~2M ledger postings); small ~ 1/10 for quick CI-style runs.
SCALES = {
    "small": {"customers": 500},
    "medium": {"customers": 5000},
    "large": {"customers": 50000},
}

# Weighted categoricals — keys MUST match the dbt seeds (seed_countries,
# seed_risk_tiers) so joins in later layers resolve.
COUNTRIES = {"US": 0.55, "GB": 0.15, "DE": 0.10, "FR": 0.08, "CA": 0.07, "AU": 0.05}
RISK_TIERS = {"low": 0.60, "medium": 0.30, "high": 0.10}
REFERRAL_SOURCES = {"organic": 0.40, "referral": 0.25, "paid_social": 0.20, "paid_search": 0.15}
CARD_NETWORKS = {"visa": 0.6, "mastercard": 0.4}

# MCCs that also exist in seed_mcc_codes (card settlements carry one of these).
MCCS = [5411, 5812, 5541, 5732, 5999, 4111, 5921, 7011, 4899, 5691]

# Nimbus internal/clearing accounts — the offsetting leg of every entry. Not in
# raw_accounts (they are house accounts, not customer accounts).
ACCT_PAYROLL = "NIMBUS-PAYROLL-CLEARING"
ACCT_CARD = "NIMBUS-CARD-SETTLEMENT"
ACCT_ATM = "NIMBUS-ATM-NETWORK"
ACCT_FEE = "NIMBUS-FEE-REVENUE"
ACCT_INTEREST = "NIMBUS-INTEREST-EXPENSE"
ACCT_TRANSFER = "NIMBUS-INTERNAL-TRANSFER"

FIRST_NAMES = [
    "James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael", "Linda",
    "David", "Elizabeth", "William", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
    "Thomas", "Sarah", "Chen", "Wei", "Aisha", "Omar", "Priya", "Raj", "Sofia", "Mateo",
    "Emma", "Liam", "Olivia", "Noah", "Ava", "Lucas", "Mia", "Ethan", "Isla", "Yuki",
]
LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Nguyen", "Kim", "Patel", "Singh", "Chen", "Wang", "Muller", "Rossi", "Dubois",
    "Kowalski", "Andersson", "Okafor", "Mensah", "Tanaka", "Suzuki",
]
REJECT_REASONS = ["document_mismatch", "sanctions_hit", "duplicate_identity", "unreadable_document"]


def weighted(rng: random.Random, table: dict[str, float]) -> str:
    return rng.choices(list(table.keys()), weights=list(table.values()), k=1)[0]


def rand_dt(rng: random.Random, start: dt.datetime, end: dt.datetime) -> dt.datetime:
    """Uniform datetime in [start, end]."""
    span = int((end - start).total_seconds())
    return start + dt.timedelta(seconds=rng.randint(0, max(0, span)))


def fmt_ts(ts: dt.datetime) -> str:
    return ts.strftime("%Y-%m-%d %H:%M:%S")


def fmt_date(d: dt.date) -> str:
    return d.strftime("%Y-%m-%d")


class Ids:
    """Monotonic id minter — stable given a fixed generation order (determinism)."""

    def __init__(self) -> None:
        self._n = {}

    def next(self, prefix: str) -> str:
        i = self._n.get(prefix, 0) + 1
        self._n[prefix] = i
        return f"{prefix}-{i:09d}"


def generate(scale: str, seed: int, out_dir: str) -> dict[str, int]:
    rng = random.Random(seed)
    ids = Ids()
    n_customers = SCALES[scale]["customers"]
    os.makedirs(out_dir, exist_ok=True)

    ingested = fmt_ts(WINDOW_END)  # single batch-load timestamp for the Python tier
    counts = {t: 0 for t in (
        "raw_customers", "raw_kyc_events", "raw_accounts",
        "raw_account_events", "raw_cards", "raw_ledger_postings",
    )}

    def opener(name):
        f = open(os.path.join(out_dir, f"{name}.csv"), "w", newline="")
        return f, csv.writer(f)

    f_cust, w_cust = opener("raw_customers")
    f_kyc, w_kyc = opener("raw_kyc_events")
    f_acct, w_acct = opener("raw_accounts")
    f_aevt, w_aevt = opener("raw_account_events")
    f_card, w_card = opener("raw_cards")
    f_ledg, w_ledg = opener("raw_ledger_postings")

    w_cust.writerow(["customer_id", "signup_ts", "email", "full_name", "country", "dob", "risk_tier", "referral_source", "ingested_at"])
    w_kyc.writerow(["kyc_event_id", "customer_id", "event_ts", "old_status", "new_status", "reason", "ingested_at"])
    w_acct.writerow(["account_id", "customer_id", "account_type", "opened_ts", "interest_rate_bps", "ingested_at"])
    w_aevt.writerow(["account_event_id", "account_id", "event_ts", "old_status", "new_status", "ingested_at"])
    w_card.writerow(["card_id", "account_id", "customer_id", "issued_ts", "network", "status", "last4", "ingested_at"])
    w_ledg.writerow(["posting_id", "transaction_id", "account_id", "posting_ts", "direction", "amount_minor", "currency", "counterparty_account_id", "category_code", "mcc", "description", "idempotency_key", "ingested_at"])

    def post(txn_id, acct, ts, direction, amount, counterparty, category, mcc, desc):
        """Emit one ledger posting row (one leg of a double-entry transaction)."""
        w_ledg.writerow([
            ids.next("PST"), txn_id, acct, fmt_ts(ts), direction, amount, "USD",
            counterparty, category, ("" if mcc is None else mcc), desc, txn_id, ingested,
        ])
        counts["raw_ledger_postings"] += 1

    def entry(acct, ts, cust_direction, amount, internal_acct, category, desc, mcc=None):
        """One balanced transaction: customer leg + offsetting internal leg."""
        txn_id = ids.next("TXN")
        internal_direction = "credit" if cust_direction == "debit" else "debit"
        post(txn_id, acct, ts, cust_direction, amount, internal_acct, category, mcc, desc)
        post(txn_id, internal_acct, ts, internal_direction, amount, acct, "internal_settlement", None, desc)

    for _ in range(n_customers):
        cust_id = ids.next("CUST")
        signup = rand_dt(rng, WINDOW_START, WINDOW_END - dt.timedelta(days=30))
        country = weighted(rng, COUNTRIES)
        risk = weighted(rng, RISK_TIERS)
        referral = weighted(rng, REFERRAL_SOURCES)
        first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
        full_name = f"{first} {last}"
        email = f"{first}.{last}.{cust_id.split('-')[1]}@example.com".lower()
        # Age 18-75 at signup.
        dob = (signup - dt.timedelta(days=365 * rng.randint(18, 75) + rng.randint(0, 364))).date()
        w_cust.writerow([cust_id, fmt_ts(signup), email, full_name, country, fmt_date(dob), risk, referral, ingested])
        counts["raw_customers"] += 1

        # --- KYC event chain: submitted -> pending -> verified|rejected ----------
        t_sub = signup + dt.timedelta(minutes=rng.randint(1, 120))
        t_pend = t_sub + dt.timedelta(minutes=rng.randint(5, 240))
        t_final = t_pend + dt.timedelta(hours=rng.randint(1, 72))
        w_kyc.writerow([ids.next("KYC"), cust_id, fmt_ts(t_sub), "none", "submitted", "", ingested]); counts["raw_kyc_events"] += 1
        w_kyc.writerow([ids.next("KYC"), cust_id, fmt_ts(t_pend), "submitted", "pending", "", ingested]); counts["raw_kyc_events"] += 1
        verified = rng.random() < 0.85
        if verified:
            w_kyc.writerow([ids.next("KYC"), cust_id, fmt_ts(t_final), "pending", "verified", "", ingested])
        else:
            w_kyc.writerow([ids.next("KYC"), cust_id, fmt_ts(t_final), "pending", "rejected", rng.choice(REJECT_REASONS), ingested])
        counts["raw_kyc_events"] += 1

        if not verified:
            continue  # rejected customers open no accounts

        # --- Accounts: always a checking; ~30% also a savings (~1.3/customer) ----
        accounts = [("checking", 0)]
        if rng.random() < 0.30:
            accounts.append(("savings", rng.choice([200, 250, 300, 350])))

        for acct_type, rate_bps in accounts:
            acct_id = ids.next("ACCT")
            opened = t_final + dt.timedelta(days=rng.randint(0, 5), hours=rng.randint(0, 23))
            if opened > WINDOW_END:
                opened = t_final
            w_acct.writerow([acct_id, cust_id, acct_type, fmt_ts(opened), rate_bps, ingested]); counts["raw_accounts"] += 1

            # account_events: opens active; small chance of a frozen/close excursion.
            w_aevt.writerow([ids.next("AEVT"), acct_id, fmt_ts(opened), "none", "active", ingested]); counts["raw_account_events"] += 1
            roll = rng.random()
            if roll < 0.05:  # temporary freeze then reactivate
                tf = rand_dt(rng, opened, WINDOW_END)
                w_aevt.writerow([ids.next("AEVT"), acct_id, fmt_ts(tf), "active", "frozen", ingested]); counts["raw_account_events"] += 1
                w_aevt.writerow([ids.next("AEVT"), acct_id, fmt_ts(min(tf + dt.timedelta(days=rng.randint(1, 20)), WINDOW_END)), "frozen", "active", ingested]); counts["raw_account_events"] += 1
            elif roll < 0.08:  # closed
                w_aevt.writerow([ids.next("AEVT"), acct_id, fmt_ts(rand_dt(rng, opened, WINDOW_END)), "active", "closed", ingested]); counts["raw_account_events"] += 1

            # One card per checking account; most active.
            if acct_type == "checking":
                card_status = "active" if rng.random() < 0.92 else rng.choice(["blocked", "expired"])
                w_card.writerow([
                    ids.next("CARD"), acct_id, cust_id,
                    fmt_ts(opened + dt.timedelta(days=rng.randint(0, 3))),
                    weighted(rng, CARD_NETWORKS), card_status,
                    f"{rng.randint(0, 9999):04d}", ingested,
                ]); counts["raw_cards"] += 1

            # --- Ledger simulation (checking only; savings just accrues) ---------
            simulate_ledger(rng, entry, acct_type, acct_id, opened, rate_bps)

    for f in (f_cust, f_kyc, f_acct, f_aevt, f_card, f_ledg):
        f.close()
    return counts


def simulate_ledger(rng, entry, acct_type, acct_id, opened, rate_bps):
    """Emit a chronologically-consistent, non-negative ledger for one account.

    Builds a schedule of (ts, kind, amount, ...) transactions, sorts by time, then
    replays it tracking `balance` so debits never overdraw (capped/skipped).
    """
    start = max(opened, WINDOW_START)
    if start >= WINDOW_END:
        return
    active_days = (WINDOW_END - start).days + 1
    months = max(1, active_days / 30.0)

    schedule = []  # (ts, kind, amount, mcc, category, desc)

    if acct_type == "checking":
        # Biweekly payroll (credit).
        salary = rng.randint(180000, 620000)
        d = start + dt.timedelta(days=rng.randint(0, 6))
        while d <= WINDOW_END:
            schedule.append((d, "credit", salary, None, "payroll", "Payroll deposit"))
            d += dt.timedelta(days=14)

        # Card settlements (~14/mo), linked to an MCC (debit).
        for _ in range(int(round(14 * months))):
            ts = rand_dt(rng, start, WINDOW_END)
            schedule.append((ts, "debit", rng.randint(300, 22000), rng.choice(MCCS), "card_settlement", "Card purchase"))

        # P2P sends (~3/mo, debit to a clearing counterparty).
        for _ in range(int(round(3 * months))):
            ts = rand_dt(rng, start, WINDOW_END)
            schedule.append((ts, "debit", rng.randint(1000, 50000), None, "p2p_transfer", "P2P transfer"))

        # ATM withdrawals + fee (~2/mo). Two transactions, both debit.
        for _ in range(int(round(2 * months))):
            ts = rand_dt(rng, start, WINDOW_END)
            schedule.append((ts, "debit", rng.randint(2000, 40000), None, "atm_withdrawal", "ATM withdrawal"))
            schedule.append((ts + dt.timedelta(seconds=1), "fee_atm", 300, None, "atm_fee", "ATM fee"))

        # Occasional expedited-transfer fee (~1 every 3 months).
        for _ in range(int(round(months / 3.0))):
            ts = rand_dt(rng, start, WINDOW_END)
            schedule.append((ts, "fee_expedite", 1500, None, "expedited_transfer_fee", "Expedited transfer fee"))
    else:
        # Savings: periodic transfers in + monthly interest accrual (both credit).
        for _ in range(int(round(1.5 * months))):
            ts = rand_dt(rng, start, WINDOW_END)
            schedule.append((ts, "credit", rng.randint(20000, 200000), None, "account_transfer", "Transfer to savings"))

    schedule.sort(key=lambda r: r[0])

    balance = 0
    last_interest_month = None
    for ts, kind, amount, mcc, category, desc in schedule:
        # Monthly interest accrual on savings, computed on the running balance.
        if acct_type == "savings" and rate_bps > 0:
            ym = (ts.year, ts.month)
            if last_interest_month is not None and ym != last_interest_month and balance > 0:
                interest = (balance * rate_bps) // (10000 * 12)
                if interest > 0:
                    entry(acct_id, ts, "credit", interest, ACCT_INTEREST, "interest_accrual", "Monthly interest")
                    balance += interest
            last_interest_month = ym

        if kind == "credit":
            internal = ACCT_PAYROLL if category == "payroll" else ACCT_TRANSFER
            entry(acct_id, ts, "credit", amount, internal, category, desc)
            balance += amount
        else:  # any debit-shaped kind
            if balance <= 0:
                continue
            amt = min(amount, balance)  # cap to available: no overdraft
            if kind == "card_settlement" or category == "card_settlement":
                internal = ACCT_CARD
            elif category == "atm_withdrawal":
                internal = ACCT_ATM
            elif category in ("atm_fee", "expedited_transfer_fee"):
                internal = ACCT_FEE
            else:
                internal = ACCT_TRANSFER
            entry(acct_id, ts, "debit", amt, internal, category, desc, mcc=mcc)
            balance -= amt


def main() -> None:
    ap = argparse.ArgumentParser(description="Generate Nimbus bronze CSVs (Python tier).")
    ap.add_argument("--scale", choices=list(SCALES), default="medium")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", default=os.path.join(os.path.dirname(__file__), "out"))
    args = ap.parse_args()

    print(f"==> Generating scale={args.scale} seed={args.seed} -> {args.out}")
    counts = generate(args.scale, args.seed, args.out)
    for table, n in counts.items():
        print(f"    {table:24s} {n:>12,}")
    print("==> Done.")


if __name__ == "__main__":
    main()
