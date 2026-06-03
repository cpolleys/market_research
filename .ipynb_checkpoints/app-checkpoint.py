import streamlit as st

st.set_page_config(
    page_title="Biotech Research Dashboard",
    page_icon="🧬",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("🧬 Biotech Research Dashboard")
st.markdown(
    """
    Use the sidebar to navigate between sections.

    | Page | Description |
    |---|---|
    | **Ticker Tracker** | Monitor stock prices for companies with notable trial results |
    | **Trial Signals** | Recent status changes and completion date slippage |
    | **Upcoming Completions** | Phase 2–4 trials completing in the next 12 months |
    | **Disease Landscape** | Competitive landscape by condition |
    | **Publications** | New publications linked to tracked trials |
    """
)

st.info(
    "Make sure `biotech.db` is in the same directory as `app.py` before launching, "
    "or set the `DB_PATH` environment variable to point to it.",
    icon="ℹ️",
)
