import json
from datetime import date, datetime

import pandas as pd
import streamlit as st
import yfinance as yf

from db_utils import get_conn


# ── schema ─────────────────────────────────────────────────────────────────────
def init_tracker():
    conn = get_conn()

    # One row per entry event — multiple rows allowed per ticker
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ticker_entries (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker      TEXT NOT NULL,
            name        TEXT,
            note        TEXT,
            pmid        TEXT,
            nct_id      TEXT,
            added_at    TEXT NOT NULL,
            added_price REAL
        )
    """)

    # One row per ticker — stores the shared rolling price history
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ticker_prices (
            ticker        TEXT PRIMARY KEY,
            name          TEXT,
            price_history TEXT NOT NULL DEFAULT '[]'
        )
    """)

    # Migration: if the old ticker_tracker table exists, move data across
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='ticker_tracker'"
    )
    if cur.fetchone():
        old_rows = conn.execute(
            "SELECT ticker, name, note, pmid, nct_id, added_at, added_price, price_history "
            "FROM ticker_tracker"
        ).fetchall()
        for r in old_rows:
            ticker, name, note, pmid, nct_id, added_at, added_price, price_history = r
            conn.execute("""
                INSERT OR IGNORE INTO ticker_entries
                    (ticker, name, note, pmid, nct_id, added_at, added_price)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (ticker, name, note, pmid, nct_id, added_at, added_price))
            conn.execute("""
                INSERT OR IGNORE INTO ticker_prices (ticker, name, price_history)
                VALUES (?, ?, ?)
            """, (ticker, name, price_history))
        conn.execute("ALTER TABLE ticker_tracker RENAME TO ticker_tracker_legacy")

    conn.commit()
    conn.close()

init_tracker()


# ── price fetch ────────────────────────────────────────────────────────────────
@st.experimental_memo(ttl=300)
def fetch_price(ticker: str):
    try:
        t     = yf.Ticker(ticker)
        price = t.fast_info.last_price
        prev  = t.fast_info.previous_close
        name  = t.info.get("shortName") or ticker
        if not price:
            return None
        return {"price": round(price, 2), "prev": round(prev, 2) if prev else None, "name": name}
    except Exception:
        return None


# ── DB helpers ─────────────────────────────────────────────────────────────────
def load_tickers():
    """Return distinct tickers that have at least one entry, with their price data."""
    conn = get_conn()
    rows = conn.execute(
        "SELECT ticker, name, price_history FROM ticker_prices ORDER BY ticker"
    ).fetchall()
    conn.close()
    return {r[0]: {"name": r[1], "price_history": json.loads(r[2])} for r in rows}


def load_entries(ticker: str):
    """Return all entries for a ticker, newest first."""
    conn = get_conn()
    rows = conn.execute(
        "SELECT id, ticker, name, note, pmid, nct_id, added_at, added_price "
        "FROM ticker_entries WHERE ticker = ? ORDER BY added_at DESC",
        (ticker,)
    ).fetchall()
    conn.close()
    return [{
        "id":          r[0],
        "ticker":      r[1],
        "name":        r[2],
        "note":        r[3],
        "pmid":        r[4],
        "nct_id":      r[5],
        "added_at":    r[6],
        "added_price": r[7],
    } for r in rows]


def load_all_entries():
    """Return all entries across all tickers, newest first."""
    conn = get_conn()
    rows = conn.execute(
        "SELECT id, ticker, name, note, pmid, nct_id, added_at, added_price "
        "FROM ticker_entries ORDER BY added_at DESC"
    ).fetchall()
    conn.close()
    return [{
        "id":          r[0],
        "ticker":      r[1],
        "name":        r[2],
        "note":        r[3],
        "pmid":        r[4],
        "nct_id":      r[5],
        "added_at":    r[6],
        "added_price": r[7],
    } for r in rows]


def save_entry(entry: dict):
    """Insert a new entry event. Always appends — never replaces."""
    conn = get_conn()
    conn.execute("""
        INSERT INTO ticker_entries
            (ticker, name, note, pmid, nct_id, added_at, added_price)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        entry["ticker"], entry.get("name"), entry.get("note"), entry.get("pmid"),
        entry.get("nct_id"), entry["added_at"], entry.get("added_price"),
    ))
    # Ensure a price row exists for this ticker
    conn.execute("""
        INSERT OR IGNORE INTO ticker_prices (ticker, name, price_history)
        VALUES (?, ?, '[]')
    """, (entry["ticker"], entry.get("name")))
    conn.commit()
    conn.close()


def delete_entry(entry_id: int):
    """Delete a single entry. Removes ticker_prices row if no entries remain."""
    conn = get_conn()
    ticker = conn.execute(
        "SELECT ticker FROM ticker_entries WHERE id = ?", (entry_id,)
    ).fetchone()
    conn.execute("DELETE FROM ticker_entries WHERE id = ?", (entry_id,))
    if ticker:
        remaining = conn.execute(
            "SELECT COUNT(*) FROM ticker_entries WHERE ticker = ?", (ticker[0],)
        ).fetchone()[0]
        if remaining == 0:
            conn.execute("DELETE FROM ticker_prices WHERE ticker = ?", (ticker[0],))
    conn.commit()
    conn.close()


def update_price(ticker: str, data: dict):
    conn = get_conn()
    row = conn.execute(
        "SELECT price_history FROM ticker_prices WHERE ticker = ?", (ticker,)
    ).fetchone()
    if not row:
        conn.execute(
            "INSERT OR IGNORE INTO ticker_prices (ticker, name, price_history) VALUES (?, ?, '[]')",
            (ticker, data.get("name", ticker))
        )
        history = []
    else:
        history = json.loads(row[0])

    today = date.today().isoformat()
    if history and history[-1]["date"] == today:
        history[-1]["price"] = data["price"]
    else:
        history.append({"date": today, "price": data["price"]})
    history.sort(key=lambda x: x["date"])
    history = history[-180:]

    conn.execute(
        "UPDATE ticker_prices SET price_history = ?, name = ? WHERE ticker = ?",
        (json.dumps(history), data.get("name", ticker), ticker)
    )
    conn.commit()
    conn.close()


# ── UI ─────────────────────────────────────────────────────────────────────────
st.title("📈 Ticker Tracker")
st.caption("Track stock prices for companies with notable trial results. Prices via yfinance (~15 min delay).")

# ── Add form ───────────────────────────────────────────────────────────────────
with st.expander("➕ Add a new entry", expanded=True):
    c1, c2, c3, c4, c5 = st.columns([1, 1.4, 1.4, 2.6, 1])
    new_ticker = c1.text_input("Ticker", placeholder="VRTX").strip().upper()
    new_nct    = c2.text_input("NCT ID", placeholder="NCT06625320").strip()
    new_pmid   = c3.text_input("PMID", placeholder="41831073").strip()
    new_note   = c4.text_input("Note", placeholder="Phase 3 PDAC — promising PFS data").strip()
    c5.write("")
    add_clicked = c5.button("Add")

    if add_clicked:
        if not new_ticker:
            st.error("Enter a ticker symbol.")
        else:
            with st.spinner(f"Fetching {new_ticker}…"):
                data = fetch_price(new_ticker)
            if not data:
                st.error(f"Could not fetch price for {new_ticker}. Check the symbol.")
            else:
                save_entry({
                    "ticker":      new_ticker,
                    "name":        data["name"],
                    "note":        new_note,
                    "pmid":        new_pmid,
                    "nct_id":      new_nct,
                    "added_at":    datetime.utcnow().isoformat(),
                    "added_price": data["price"],
                })
                update_price(new_ticker, data)
                st.success(f"Added entry for {new_ticker} @ ${data['price']:.2f}")
                fetch_price.clear()
                st.experimental_rerun()

st.markdown("---")

# ── Load data ──────────────────────────────────────────────────────────────────
ticker_prices = load_tickers()
if not ticker_prices:
    st.info("No tickers tracked yet. Add one above.")
    st.stop()

# ── Refresh all ────────────────────────────────────────────────────────────────
col_hdr, col_btn = st.columns([6, 1])
col_hdr.subheader(f"{len(ticker_prices)} ticker{'s' if len(ticker_prices) != 1 else ''} tracked")
if col_btn.button("↻ Refresh all"):
    fetch_price.clear()
    for ticker in ticker_prices:
        data = fetch_price(ticker)
        if data:
            update_price(ticker, data)
    st.experimental_rerun()

# ── Cards — one per ticker ─────────────────────────────────────────────────────
for ticker, price_data in ticker_prices.items():
    entries  = load_entries(ticker)
    history  = price_data["price_history"]
    current  = history[-1]["price"] if history else None

    with st.container():
        left, right = st.columns([5, 1])

        with left:
            display_name = price_data["name"] or ticker
            st.markdown(f"### {ticker}  {'  ' + display_name if display_name != ticker else ''}")

        with right:
            if st.button("↻", key=f"ref_{ticker}"):
                fetch_price.clear()
                data = fetch_price(ticker)
                if data:
                    update_price(ticker, data)
                st.experimental_rerun()

        # ── Price chart with entry markers ────────────────────────────────────
        if len(history) > 0:
            df_price = pd.DataFrame(history)
            df_price["date"] = pd.to_datetime(df_price["date"])
            df_price = df_price.set_index("date")

            # Build a second series that is NaN everywhere except entry dates
            entry_dates = {}
            for e in entries:
                if e["added_price"] is not None:
                    d = pd.Timestamp(e["added_at"][:10])
                    # If multiple entries on the same day, keep the most recent
                    entry_dates[d] = e["added_price"]

            df_price["entry"] = pd.Series(entry_dates, dtype=float)

            if len(history) > 1:
                st.line_chart(df_price[["price", "entry"]], height=140)
            else:
                st.caption(f"Current price: ${current:.2f}" if current else "")

        # ── Metrics row ───────────────────────────────────────────────────────
        if current and entries:
            first_entry_price = entries[-1]["added_price"]   # oldest entry
            vs_first = (
                round((current - first_entry_price) / first_entry_price * 100, 2)
                if first_entry_price else None
            )
            m1, m2, m3 = st.columns(3)
            m1.metric("Price", f"${current:.2f}")
            m2.metric(
                "vs first entry",
                f"{vs_first:+.2f}%" if vs_first is not None else "—",
                delta=f"{vs_first:.2f}%" if vs_first is not None else None,
            )
            m3.metric("Entries", len(entries))

        # ── Entry list ────────────────────────────────────────────────────────
        for e in entries:
            ec1, ec2 = st.columns([9, 1])
            with ec1:
                note_str = f"**{e['note']}**  " if e.get("note") else ""
                price_str = f"@ ${e['added_price']:.2f}" if e.get("added_price") else ""
                st.markdown(f"&nbsp;&nbsp;&nbsp;{note_str}{e['added_at'][:10]} {price_str}")
                meta = []
                if e.get("nct_id"):
                    meta.append(f"[{e['nct_id']} ↗](https://clinicaltrials.gov/study/{e['nct_id']})")
                if e.get("pmid"):
                    meta.append(f"[PMID {e['pmid']} ↗](https://pubmed.ncbi.nlm.nih.gov/{e['pmid']})")
                if meta:
                    st.caption("  ·  ".join(meta))
            with ec2:
                if st.button("🗑", key=f"del_{e['id']}"):
                    delete_entry(e["id"])
                    st.experimental_rerun()

        st.markdown("---")
