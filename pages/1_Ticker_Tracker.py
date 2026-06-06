import json
import sqlite3
from datetime import date, datetime

import pandas as pd
import streamlit as st
import yfinance as yf

from db_utils import get_conn

# ── ensure tracker table exists ────────────────────────────────────────────────
def init_tracker():
    conn = get_conn()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ticker_tracker (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker        TEXT PRIMARY KEY,
            name          TEXT,
            note          TEXT,
            pmid          TEXT,
            nct_id        TEXT,
            added_at      TEXT NOT NULL,
            added_price   REAL,
            price_history TEXT NOT NULL DEFAULT '[]'
        )
    """)
    
    # Migration: if the old table used ticker as PK, recreate it with surrogate id
    cols = [r[1] for r in conn.execute("PRAGMA table_info(ticker_tracker)").fetchall()]
    if "id" not in cols:
        conn.executescript("""
            ALTER TABLE ticker_tracker RENAME TO ticker_tracker_old;

            CREATE TABLE ticker_tracker (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker        TEXT NOT NULL,
                name          TEXT,
                note          TEXT,
                pmid          TEXT,
                nct_id        TEXT,
                added_at      TEXT NOT NULL,
                added_price   REAL,
                price_history TEXT NOT NULL DEFAULT '[]'
            );

            INSERT INTO ticker_tracker (ticker, name, note, pmid, nct_id, added_at, added_price, price_history)
            SELECT ticker, name, note, pmid, nct_id, added_at, added_price, price_history
            FROM ticker_tracker_old;

            DROP TABLE ticker_tracker_old;
        """)
    
    conn.commit()
    conn.close()

init_tracker()

# ── price fetch ────────────────────────────────────────────────────────────────
@st.experimental_memo(ttl=300)
def fetch_price(ticker: str):
    try:
        t = yf.Ticker(ticker)
        price = t.fast_info.last_price
        prev  = t.fast_info.previous_close
        name  = t.info.get("shortName") or ticker
        if not price:
            return None
        return {"price": round(price, 2), "prev": round(prev, 2) if prev else None, "name": name}
    except Exception:
        return None

# ── DB helpers ─────────────────────────────────────────────────────────────────
def load_all():
    conn = get_conn()
    rows = conn.execute(
        "SELECT id, ticker, name, note, pmid, nct_id, added_at, added_price, price_history "
        "FROM ticker_tracker ORDER BY added_at DESC"
    ).fetchall()
    conn.close()
    return [{
        "id":            r[0],
        "ticker":        r[1],
        "name":          r[2],
        "note":          r[3],
        "pmid":          r[4],
        "nct_id":        r[5],
        "added_at":      r[6],
        "added_price":   r[7],
        "price_history": json.loads(r[8]),
    } for r in rows]

def save_entry(entry: dict):
    """Always inserts a new row — one ticker can have many entries."""
    conn = get_conn()
    conn.execute("""
        INSERT INTO ticker_tracker
            (ticker, name, note, pmid, nct_id, added_at, added_price, price_history)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        entry["ticker"], entry.get("name"), entry.get("note"), entry.get("pmid"),
        entry.get("nct_id"), entry["added_at"], entry.get("added_price"),
        json.dumps(entry.get("price_history", [])),
    ))
    conn.commit()
    conn.close()

def delete_entry(entry_id: int):
    conn = get_conn()
    conn.execute("DELETE FROM ticker_tracker WHERE id = ?", (entry_id,))
    conn.commit()
    conn.close()

def update_price_for_ticker(ticker: str, data: dict):
    """Update price_history on ALL entries for this ticker (they share the same stock)."""
    conn = get_conn()
    rows = conn.execute(
        "SELECT id, price_history FROM ticker_tracker WHERE ticker = ?", (ticker,)
    ).fetchall()
    today = date.today().isoformat()

    for row_id, ph_json in rows:
        history = json.loads(ph_json)
        if history and history[-1]["date"] == today:
            history[-1]["price"] = data["price"]
        else:
            history.append({"date": today, "price": data["price"]})
        history = history[-180:]
        conn.execute(
            "UPDATE ticker_tracker SET price_history = ?, name = ?, added_price = COALESCE(added_price, ?) WHERE id = ?",
            (json.dumps(history), data.get("name", ticker), data["price"], row_id)
        )

    conn.commit()
    conn.close()

# ── UI ─────────────────────────────────────────────────────────────────────────
st.title("📈 Ticker Tracker")
st.caption("Track stock prices for companies with notable trial results. Prices via yfinance (~15 min delay).")

# Add form
with st.expander("➕ Add a new ticker", expanded=True):
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
        elif new_ticker in [r["ticker"] for r in load_all()]:
            st.warning(f"{new_ticker} is already tracked.")
        else:
            conn = get_conn()
            existing_ph = conn.execute(
                "SELECT price_history FROM ticker_tracker WHERE ticker = ? ORDER BY added_at DESC LIMIT 1",
                (new_ticker,)
            ).fetchone()
            conn.close()

            price_history = json.loads(existing_ph[0]) if existing_ph else [
                {"date": date.today().isoformat(), "price": data["price"]}
            ]

            save_entry({
                "ticker":        new_ticker,
                "name":          data["name"],
                "note":          new_note,
                "pmid":          new_pmid,
                "nct_id":        new_nct,
                "added_at":      datetime.utcnow().isoformat(),
                "added_price":   data["price"],
                "price_history": price_history,
            })
            st.success(f"Added entry for {new_ticker} @ ${data['price']:.2f}")
            st.experimental_rerun()

st.markdown("---")

all_entries = load_all()
if not all_entries:
    st.info("No entries tracked yet. Add one above.")
    st.stop()

# Group by ticker
entries_by_ticker = {}
for e in all_entries:
    entries_by_ticker.setdefault(e["ticker"], []).append(e)

col_hdr, col_btn = st.columns([6, 1])
num_tickers = len(entries_by_ticker)
num_entries = len(all_entries)
col_hdr.subheader(f"{num_entries} entr{'ies' if num_entries != 1 else 'y'} across {num_tickers} ticker{'s' if num_tickers != 1 else ''}")

if col_btn.button("↻ Refresh all"):
    fetch_price.clear()
    for ticker in entries_by_ticker:
        data = fetch_price(ticker)
        if data:
            update_price_for_ticker(ticker, data)
    st.experimental_rerun()

# ── Render grouped by ticker ───────────────────────────────────────────────────
for ticker, entries in entries_by_ticker.items():
    latest  = entries[0]
    history = latest["price_history"]
    current = history[-1]["price"] if history else None
    added   = latest["added_price"]
    vs_added = round((current - added) / added * 100, 2) if current and added else None

    hdr_left, hdr_right = st.columns([5, 1])
    with hdr_left:
        name_label = latest["name"] if latest["name"] != ticker else ""
        st.markdown(f"### {ticker}  {name_label}")
    with hdr_right:
        if st.button("↻", key=f"ref_{ticker}"):
            fetch_price.clear()
            data = fetch_price(ticker)
            if data:
                update_price_for_ticker(ticker, data)
            st.experimental_rerun()
            
    if current:
        m1, m2, m3 = st.columns(3)
        m1.metric("Price", f"${current:.2f}")
        m2.metric("Since first added", f"{vs_added:+.2f}%" if vs_added is not None else "—",
                  delta=f"{vs_added:.2f}%" if vs_added is not None else None)
        m3.metric("Data points", len(history))

    if len(history) > 1:
        df = pd.DataFrame(history).set_index("date")
        df.index = pd.to_datetime(df.index)
        st.line_chart(df["price"], height=120)
    
    # One row per study entry
    for entry in entries:
        col_note, col_links, col_del = st.columns([3, 3, 0.5])
        with col_note:
            st.markdown(f"📝 {entry['note']}" if entry.get("note") else "*(no note)*")
        with col_links:
            parts = [f"Added {entry['added_at'][:10]}" + (f" @ ${entry['added_price']:.2f}" if entry.get('added_price') else "")]
            if entry.get("nct_id"):
                parts.append(f"[{entry['nct_id']} ↗](https://clinicaltrials.gov/study/{entry['nct_id']})")
            if entry.get("pmid"):
                parts.append(f"[PMID {entry['pmid']} ↗](https://pubmed.ncbi.nlm.nih.gov/{entry['pmid']})")
            st.caption("  ·  ".join(parts))
        with col_del:
            if st.button("🗑", key=f"del_{entry['id']}"):
                delete_entry(entry["id"])
                st.experimental_rerun()

    st.markdown("---")