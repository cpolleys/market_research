"""
readouts.py
===========
Enter a ticker symbol and print all upcoming clinical trial readouts
pulled from the local biotech.db and/or live from ClinicalTrials.gov.

Usage:
    python readouts.py          # prompts for ticker interactively
    python readouts.py MRNA     # pass ticker as CLI argument
    python readouts.py MRNA --live  # skip DB, always fetch live from ClinicalTrials.gov

Readouts are sorted by primary_completion_date ascending so the nearest
catalysts appear first.  Trials with no completion date are shown at the end.
"""

import sys
import os
import sqlite3
import argparse
from datetime import date, datetime

# ── Optional: reuse scraper if it's on the path ────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.join(SCRIPT_DIR)   # adjust if readouts.py lives elsewhere
sys.path.insert(0, PROJECT_DIR)

try:
    from scraper import fetch_trials
    HAS_SCRAPER = True
except ImportError:
    HAS_SCRAPER = False

DB_PATH = os.environ.get("DB_PATH", os.path.join(PROJECT_DIR, "biotech.db"))

TODAY = date.today().isoformat()

# Statuses that represent active / upcoming trials
ACTIVE_STATUSES = {
    "NOT_YET_RECRUITING",
    "RECRUITING",
    "ENROLLING_BY_INVITATION",
    "ACTIVE_NOT_RECRUITING",
}

STATUS_LABEL = {
    "NOT_YET_RECRUITING":      "Not yet recruiting",
    "RECRUITING":              "Recruiting",
    "ENROLLING_BY_INVITATION": "Enrolling by invitation",
    "ACTIVE_NOT_RECRUITING":   "Active, not recruiting",
}

PHASE_ORDER = {
    "PHASE1": 1,
    "PHASE1/PHASE2": 2,
    "PHASE2": 3,
    "PHASE2/PHASE3": 4,
    "PHASE3": 5,
    "PHASE4": 6,
}


# ── DB helpers ─────────────────────────────────────────────────────────────────

def db_exists():
    return os.path.exists(DB_PATH)


def lookup_ticker_in_db(ticker: str) -> list[dict]:
    """
    Query the local DB for trials where company == ticker (case-insensitive).
    Returns the latest snapshot per nct_id, filtered to upcoming trials only.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        WITH latest AS (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY nct_id ORDER BY snapshot_date DESC) AS rn
            FROM trials
            WHERE UPPER(company) = UPPER(?)
        )
        SELECT nct_id, company, sponsor, title, phase, status,
               conditions, interventions, enrollment,
               primary_completion_date, start_date, primary_outcomes
        FROM latest
        WHERE rn = 1
          AND status IN (
              'NOT_YET_RECRUITING', 'RECRUITING',
              'ENROLLING_BY_INVITATION', 'ACTIVE_NOT_RECRUITING'
          )
        ORDER BY
            CASE WHEN primary_completion_date IS NULL THEN 1 ELSE 0 END,
            primary_completion_date ASC
    """, (ticker,))

    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def resolve_sponsor_from_db(ticker: str) -> str | None:
    """
    Look up the raw sponsor name that maps to this ticker in company_map.
    Useful for knowing what name to search ClinicalTrials.gov with.
    """
    if not db_exists():
        return None
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "SELECT raw_name FROM company_map WHERE UPPER(ticker) = UPPER(?) LIMIT 1",
        (ticker,)
    )
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None


# ── Live fetch ──────────────────────────────────────────────────────────────────

def fetch_live(search_name: str) -> list[dict]:
    """Fetch trials live from ClinicalTrials.gov via the scraper module."""
    if not HAS_SCRAPER:
        print("  [!] scraper.py not found on path — cannot fetch live data.")
        return []
    print(f"  Fetching live from ClinicalTrials.gov for '{search_name}' …")
    trials = fetch_trials(search_name)
    # Filter to upcoming only
    return [
        t for t in trials
        if t.get("status") in ACTIVE_STATUSES
        and (t.get("primary_completion_date") is None
             or t["primary_completion_date"] >= TODAY)
    ]


# ── Formatting ─────────────────────────────────────────────────────────────────

def phase_str(phase: str | None) -> str:
    if not phase:
        return "Unknown phase"
    return phase.replace("PHASE", "Phase ").replace("/", " / ")


def days_until(date_str: str | None) -> str:
    if not date_str:
        return ""
    try:
        delta = (date.fromisoformat(date_str) - date.today()).days
        if delta < 0:
            return f"  ← {abs(delta)}d ago"
        if delta == 0:
            return "  ← TODAY"
        if delta <= 90:
            return f"  ← {delta}d away  ⚡"
        if delta <= 180:
            return f"  ← {delta}d away"
        return f"  ← ~{delta // 30}mo away"
    except ValueError:
        return ""


def sort_key(trial: dict):
    pcd = trial.get("primary_completion_date")
    return (0 if pcd else 1, pcd or "9999-99-99")


def print_trials(ticker: str, trials: list[dict], source: str):
    RESET  = "\033[0m"
    BOLD   = "\033[1m"
    GREEN  = "\033[32m"
    YELLOW = "\033[33m"
    CYAN   = "\033[36m"
    DIM    = "\033[2m"

    print()
    print(f"{BOLD}{'═' * 70}{RESET}")
    print(f"{BOLD}  Upcoming Readouts: {ticker.upper()}   {DIM}[{source}]{RESET}")
    print(f"{BOLD}{'═' * 70}{RESET}")

    if not trials:
        print(f"\n  {YELLOW}No upcoming trials found for {ticker.upper()}.{RESET}\n")
        return

    trials_sorted = sorted(trials, key=sort_key)

    # Group by phase for a cleaner display
    shown = 0
    for trial in trials_sorted:
        shown += 1
        nct   = trial.get("nct_id", "—")
        title = trial.get("title") or "Untitled"
        phase = phase_str(trial.get("phase"))
        status_raw = trial.get("status", "")
        status = STATUS_LABEL.get(status_raw, status_raw.replace("_", " ").title())
        pcd   = trial.get("primary_completion_date") or "TBD"
        cond  = trial.get("conditions") or "—"
        intr  = trial.get("interventions") or "—"
        enrl  = trial.get("enrollment")
        outcomes = trial.get("primary_outcomes") or "—"

        # Truncate long strings
        if len(title) > 90:
            title = title[:87] + "…"
        if len(cond) > 70:
            cond = cond[:67] + "…"
        if len(intr) > 70:
            intr = intr[:67] + "…"
        if len(outcomes) > 120:
            outcomes = outcomes[:117] + "…"

        suffix = days_until(trial.get("primary_completion_date"))
        pcd_color = YELLOW if suffix and "⚡" in suffix else CYAN

        print(f"\n  {BOLD}[{shown}] {nct}{RESET}  {DIM}({phase}){RESET}")
        print(f"      {title}")
        print(f"      {GREEN}Condition:{RESET}  {cond}")
        print(f"      {GREEN}Treatment:{RESET}  {intr}")
        print(f"      {GREEN}Status:   {RESET}  {status}")
        if enrl:
            print(f"      {GREEN}Enrollment:{RESET} {enrl:,} patients")
        print(f"      {GREEN}Est. Readout:{RESET}{pcd_color} {pcd}{suffix}{RESET}")
        if outcomes and outcomes != "—":
            print(f"      {GREEN}Primary endpoint:{RESET} {outcomes}")
        print(f"      {DIM}https://clinicaltrials.gov/study/{nct}{RESET}")

    print()
    print(f"{DIM}  {shown} upcoming trial(s) shown  ·  Today: {TODAY}{RESET}")
    print(f"{BOLD}{'═' * 70}{RESET}\n")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Print upcoming clinical trial readouts for a ticker."
    )
    parser.add_argument(
        "ticker", nargs="?", help="Stock ticker symbol (e.g. MRNA, VRTX, REGN)"
    )
    parser.add_argument(
        "--live", action="store_true",
        help="Always fetch live from ClinicalTrials.gov (ignores local DB)"
    )
    args = parser.parse_args()

    ticker = args.ticker
    if not ticker:
        ticker = input("Enter ticker symbol: ").strip()
    ticker = ticker.upper()

    trials = []
    source = "unknown"

    # ── 1. Try local DB first (unless --live) ──────────────────────────────────
    if not args.live and db_exists():
        trials = lookup_ticker_in_db(ticker)
        if trials:
            source = f"local DB · {DB_PATH}"
        else:
            print(f"  No DB results for {ticker}. Trying live fetch …")

    # ── 2. Fall back (or --live) to ClinicalTrials.gov ────────────────────────
    if not trials or args.live:
        # Try to find the sponsor name stored in company_map
        sponsor_name = resolve_sponsor_from_db(ticker)
        search_name  = sponsor_name or ticker   # ticker as last resort

        live_trials = fetch_live(search_name)
        if live_trials:
            trials = live_trials
            source = "ClinicalTrials.gov (live)"
        elif not trials:
            # If DB had results but --live was requested and live returned nothing,
            # fall back to the DB results so we always show something.
            trials = lookup_ticker_in_db(ticker) if db_exists() else []
            source = f"local DB · {DB_PATH} (live fetch returned nothing)"

    print_trials(ticker, trials, source)


if __name__ == "__main__":
    main()
