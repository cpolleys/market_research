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
    # add nct_id column if it doesn't exist yet (for existing databases)
    try:
        conn.execute("ALTER TABLE ticker_tracker ADD COLUMN nct_id TEXT")
    except Exception:
        pass
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
        "SELECT ticker, name, note, pmid, nct_id, added_at, added_price, price_history "
        "FROM ticker_tracker ORDER BY added_at DESC"
    ).fetchall()
    conn.close()
    return [{
        "ticker":        r[0],
        "name":          r[1],
        "note":          r[2],
        "pmid":          r[3],
        "nct_id":        r[4],
        "added_at":      r[5],
        "added_price":   r[6],
        "price_history": json.loads(r[7]),
    } for r in rows]

def save_ticker(entry: dict):
    conn = get_conn()
    conn.execute("""
        INSERT OR REPLACE INTO ticker_tracker
            (ticker, name, note, pmid, nct_id, added_at, added_price, price_history)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        entry["ticker"], entry.get("name"), entry.get("note"), entry.get("pmid"),
        entry.get("nct_id"), entry["added_at"], entry.get("added_price"),
        json.dumps(entry.get("price_history", [])),
    ))
    conn.commit()
    conn.close()

def delete_ticker(ticker: str):
    conn = get_conn()
    conn.execute("DELETE FROM ticker_tracker WHERE ticker = ?", (ticker,))
    conn.commit()
    conn.close()

def update_price(ticker: str, data: dict):
    conn = get_conn()
    row = conn.execute(
        "SELECT price_history, added_price FROM ticker_tracker WHERE ticker = ?", (ticker,)
    ).fetchone()
    if not row:
        conn.close()
        return
    history = json.loads(row[0])
    today = date.today().isoformat()
    if history and history[-1]["date"] == today:
        history[-1]["price"] = data["price"]
    else:
        history.append({"date": today, "price": data["price"]})
    history = history[-180:]
    conn.execute(
        "UPDATE ticker_tracker SET price_history = ?, name = ?, added_price = COALESCE(added_price, ?) WHERE ticker = ?",
        (json.dumps(history), data.get("name", ticker), data["price"], ticker)
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
            with st.spinner(f"Fetching {new_ticker}…"):
                data = fetch_price(new_ticker)
            if not data:
                st.error(f"Could not fetch price for {new_ticker}. Check the symbol.")
            else:
                save_ticker({
                    "ticker":        new_ticker,
                    "name":          data["name"],
                    "note":          new_note,
                    "pmid":          new_pmid,
                    "nct_id":        new_nct,
                    "added_at":      datetime.utcnow().isoformat(),
                    "added_price":   data["price"],
                    "price_history": [{"date": date.today().isoformat(), "price": data["price"]}],
                })
                st.success(f"Added {new_ticker} @ ${data['price']:.2f}")
                st.experimental_rerun()

st.markdown("---")

tickers = load_all()
if not tickers:
    st.info("No tickers tracked yet. Add one above.")
    st.stop()

# Refresh all
col_hdr, col_btn = st.columns([6, 1])
col_hdr.subheader(f"{len(tickers)} ticker{'s' if len(tickers) != 1 else ''} tracked")
if col_btn.button("↻ Refresh all"):
    fetch_price.clear()
    for t in tickers:
        data = fetch_price(t["ticker"])
        if data:
            update_price(t["ticker"], data)
    st.experimental_rerun()

# Cards
for entry in tickers:
    history = entry["price_history"]
    current = history[-1]["price"] if history else None
    added   = entry["added_price"]
    vs_added = round((current - added) / added * 100, 2) if current and added else None

    with st.container():
        left, right = st.columns([5, 1])

        with left:
            st.markdown(f"### {entry['ticker']}  {entry['name'] if entry['name'] != entry['ticker'] else ''}")
            if entry.get("note"):
                st.caption(entry["note"])
            meta_parts = [f"Added {entry['added_at'][:10]} @ ${added:.2f}"]

            if entry.get("nct_id"):
                meta_parts.append(f"[{entry['nct_id']} ↗](https://clinicaltrials.gov/study/{entry['nct_id']})")

            if entry.get("pmid"):
                meta_parts.append(f"[PMID {entry['pmid']} ↗](https://pubmed.ncbi.nlm.nih.gov/{entry['pmid']})")

            st.caption("  ·  ".join(meta_parts))

        with right:
            if st.button("↻", key=f"ref_{entry['ticker']}"):
                fetch_price.clear()
                data = fetch_price(entry["ticker"])
                if data:
                    update_price(entry["ticker"], data)
                st.experimental_rerun()
            if st.button("🗑", key=f"del_{entry['ticker']}"):
                delete_ticker(entry["ticker"])
                st.experimental_rerun()

        if current:
            m1, m2, m3 = st.columns(3)
            m1.metric("Price", f"${current:.2f}")
            m2.metric("Since added", f"{vs_added:+.2f}%" if vs_added is not None else "—", delta=f"{vs_added:.2f}%" if vs_added is not None else None)
            m3.metric("Data points", len(history))

        if len(history) > 1:
            df = pd.DataFrame(history).set_index("date")
            df.index = pd.to_datetime(df.index)
            st.line_chart(df["price"], height=120)