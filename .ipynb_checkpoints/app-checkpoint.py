import streamlit as st
from db_utils import get_conn

st.set_page_config(
    page_title="Biotech Research Dashboard",
    page_icon="🧬",
    layout="wide",
)

st.title("🧬 Biotech Research Dashboard")
try:
    conn = get_conn()
    conn.close()
    st.success("Connected to biotech.db ✓")
except Exception as e:
    st.error(f"Could not connect to database: {e}")

