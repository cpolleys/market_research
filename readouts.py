"""
readouts.py
===========
Enter a ticker symbol and print all upcoming clinical trial readouts
pulled from the local biotech.db and/or live from ClinicalTrials.gov.

Usage:
    python readouts.py              # prompts for ticker interactively
    python readouts.py RVMD         # pass ticker as CLI argument
    python readouts.py RVMD --live  # skip DB, always fetch live from ClinicalTrials.gov
    python readouts.py RVMD --sponsor "Revolution Medicines"  # override search name

Readouts are sorted by primary_completion_date ascending so the nearest
catalysts appear first.  Trials with no completion date are shown at the end.
"""

import sys
import os
import sqlite3
import argparse
from datetime import date
from typing import Optional, List, Dict

# ── Optional: reuse scraper if it's on the path ────────────────────────────────
SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

try:
    from scraper import fetch_trials
    HAS_SCRAPER = True
except ImportError:
    HAS_SCRAPER = False

DB_PATH = os.environ.get("DB_PATH", os.path.join(SCRIPT_DIR, "biotech.db"))
TODAY   = date.today().isoformat()

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


# ── DB helpers ─────────────────────────────────────────────────────────────────

def db_exists() -> bool:
    return os.path.exists(DB_PATH)


def lookup_ticker_in_db(ticker: str) -> List[Dict]:
    """
    Query the local DB for trials matching this ticker.
    Searches BOTH the company column (resolved ticker) AND via company_map
    so it works whether the pipeline stored a ticker or a raw name.
    Only returns active/upcoming trials with a future (or missing) completion date.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # First resolve any sponsor names that map to this ticker
    cur.execute(
        "SELECT raw_name FROM company_map WHERE UPPER(ticker) = UPPER(?)",
        (ticker,)
    )
    sponsor_names = [r[0] for r in cur.fetchall()]

    # Build placeholders: match on ticker directly OR any mapped sponsor name
    all_names = [ticker] + sponsor_names
    placeholders = ",".join("?" * len(all_names))

    cur.execute(f"""
        WITH latest AS (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY nct_id ORDER BY snapshot_date DESC) AS rn
            FROM trials
            WHERE UPPER(company) IN ({placeholders})
               OR UPPER(sponsor) IN ({placeholders})
        )
        SELECT nct_id, company, sponsor, title, phase, status,
               conditions, interventions, enrollment,
               primary_completion_date, start_date, primary_outcomes
        FROM latest
        WHERE rn = 1
          AND status IN (
              'NOT_YET_RECRUITING','RECRUITING',
              'ENROLLING_BY_INVITATION','ACTIVE_NOT_RECRUITING'
          )
          AND (primary_completion_date IS NULL OR primary_completion_date >= ?)
        ORDER BY
            CASE WHEN primary_completion_date IS NULL THEN 1 ELSE 0 END,
            primary_completion_date ASC
    """, all_names * 2 + [TODAY])   # * 2 for both IN clauses

    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def resolve_sponsor_name(ticker: str) -> Optional[str]:
    """
    Return the best sponsor name to search ClinicalTrials.gov with.
    Checks company_map first, then falls back to the ticker itself.
    """
    if not db_exists():
        return None
    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()
    # Prefer the raw_name with the highest confidence score
    cur.execute(
        "SELECT raw_name FROM company_map WHERE UPPER(ticker) = UPPER(?) ORDER BY confidence DESC LIMIT 1",
        (ticker,)
    )
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None


def debug_db_company(ticker: str):
    """Print a sample of what's actually stored in the DB for this ticker — helps diagnose mismatches."""
    if not db_exists():
        return
    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()

    print(f"\n  [debug] company_map entries for '{ticker}':")
    cur.execute("SELECT raw_name, ticker, confidence FROM company_map WHERE UPPER(ticker) = UPPER(?)", (ticker,))
    for r in cur.fetchall():
        print(f"    raw_name={r[0]!r}  ticker={r[1]!r}  confidence={r[2]}")

    print(f"\n  [debug] Sample trials.company values (top 10 by recency):")
    cur.execute("""
        SELECT DISTINCT company, sponsor FROM trials
        WHERE UPPER(company) = UPPER(?) OR UPPER(sponsor) LIKE ?
        LIMIT 10
    """, (ticker, f"%revolution%"))
    for r in cur.fetchall():
        print(f"    company={r[0]!r}  sponsor={r[1]!r}")

    conn.close()


# ── Live fetch ──────────────────────────────────────────────────────────────────

def fetch_live(search_name: str) -> List[Dict]:
    """Fetch trials live from ClinicalTrials.gov via the scraper module."""
    if not HAS_SCRAPER:
        print("  [!] scraper.py not found — cannot fetch live data.")
        return []
    print(f"  Fetching live from ClinicalTrials.gov for '{search_name}' …")
    trials = fetch_trials(search_name)
    return [
        t for t in trials
        if t.get("status") in ACTIVE_STATUSES
        and (t.get("primary_completion_date") is None
             or t["primary_completion_date"] >= TODAY)
    ]


# ── Formatting ─────────────────────────────────────────────────────────────────

def phase_str(phase: Optional[str]) -> str:
    if not phase:
        return "Unknown phase"
    return phase.replace("PHASE", "Phase ").replace("/", " / ")


def days_until(date_str: Optional[str]) -> str:
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


def sort_key(trial: Dict):
    pcd = trial.get("primary_completion_date")
    return (0 if pcd else 1, pcd or "9999-99-99")


def print_trials(ticker: str, trials: List[Dict], source: str):
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
        print(f"\n  {YELLOW}No upcoming trials found for {ticker.upper()}.{RESET}")
        print(f"  {DIM}Try: python readouts.py {ticker} --sponsor \"Full Company Name\"{RESET}\n")
        return

    trials_sorted = sorted(trials, key=sort_key)
    shown = 0
    for trial in trials_sorted:
        shown += 1
        nct      = trial.get("nct_id", "—")
        title    = trial.get("title") or "Untitled"
        phase    = phase_str(trial.get("phase"))
        status_raw = trial.get("status", "")
        status   = STATUS_LABEL.get(status_raw, status_raw.replace("_", " ").title())
        pcd      = trial.get("primary_completion_date") or "TBD"
        cond     = trial.get("conditions") or "—"
        intr     = trial.get("interventions") or "—"
        enrl     = trial.get("enrollment")
        outcomes = trial.get("primary_outcomes") or "—"

        if len(title) > 90:    title    = title[:87]    + "…"
        if len(cond)  > 70:    cond     = cond[:67]     + "…"
        if len(intr)  > 70:    intr     = intr[:67]     + "…"
        if len(outcomes) > 120: outcomes = outcomes[:117] + "…"

        suffix    = days_until(trial.get("primary_completion_date"))
        pcd_color = YELLOW if suffix and "⚡" in suffix else CYAN

        print(f"\n  {BOLD}[{shown}] {nct}{RESET}  {DIM}({phase}){RESET}")
        print(f"      {title}")
        print(f"      {GREEN}Condition:  {RESET}{cond}")
        print(f"      {GREEN}Treatment:  {RESET}{intr}")
        print(f"      {GREEN}Status:     {RESET}{status}")
        if enrl:
            print(f"      {GREEN}Enrollment: {RESET}{enrl:,} patients")
        print(f"      {GREEN}Est. Readout:{RESET}{pcd_color} {pcd}{suffix}{RESET}")
        if outcomes and outcomes != "—":
            print(f"      {GREEN}Primary endpoint: {RESET}{outcomes}")
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
        "ticker", nargs="?",
        help="Stock ticker symbol (e.g. MRNA, VRTX, RVMD)"
    )
    parser.add_argument(
        "--live", action="store_true",
        help="Always fetch live from ClinicalTrials.gov (skips local DB)"
    )
    parser.add_argument(
        "--sponsor", type=str, default=None,
        help='Override the sponsor name used for ClinicalTrials.gov search, e.g. --sponsor "Revolution Medicines"'
    )
    parser.add_argument(
        "--debug", action="store_true",
        help="Print DB diagnostic info to help troubleshoot missing results"
    )
    args = parser.parse_args()

    ticker = args.ticker
    if not ticker:
        ticker = input("Enter ticker symbol: ").strip()
    ticker = ticker.upper()

    if args.debug:
        debug_db_company(ticker)

    trials = []
    source = "unknown"

    # ── 1. Try local DB first (unless --live) ─────────────────────────────────
    if not args.live and db_exists():
        trials = lookup_ticker_in_db(ticker)
        if trials:
            source = "local DB"
        else:
            print(f"  No DB results for {ticker}.")

    # ── 2. Fall back (or --live) to ClinicalTrials.gov ────────────────────────
    if not trials or args.live:
        # Use --sponsor override, then company_map lookup, then ticker as last resort
        if args.sponsor:
            search_name = args.sponsor
        else:
            search_name = resolve_sponsor_name(ticker) or ticker
            if search_name == ticker:
                print(f"  No sponsor name found in company_map for {ticker}; searching by ticker.")
                print(f"  Tip: use --sponsor \"Company Name\" if this returns no results.\n")

        live_trials = fetch_live(search_name)
        if live_trials:
            trials = live_trials
            source = f"ClinicalTrials.gov (live, searched: '{search_name}')"
        else:
            print(f"  Live fetch also returned nothing for '{search_name}'.")
            # Last resort: if DB had something before --live was forced, use it
            if args.live and db_exists():
                trials = lookup_ticker_in_db(ticker)
                source = "local DB (live fallback)"

    print_trials(ticker, trials, source)


if __name__ == "__main__":
    main()
