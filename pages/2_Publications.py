import pandas as pd
import streamlit as st

from db_utils import read_sql, table_exists

st.title("📄 Publications")
st.caption("New publications linked to tracked trials, detected by the pipeline.")

if not table_exists("publications"):
    st.warning("No publications table found. Run pipeline.py first.")
    st.stop()

# ── Filters ────────────────────────────────────────────────────────────────────
days_back = st.slider("First seen in last N days", 7, 365, 30)
company_filter = st.text_input("Company contains", "")

st.markdown("---")

query = """
SELECT
    p.nct_id,
    t.company,
    p.pmid,
    p.title,
    p.journal,
    p.pub_date,
    p.first_seen
FROM publications p
JOIN (
    SELECT nct_id, company
    FROM trials
    GROUP BY nct_id
) t ON p.nct_id = t.nct_id
WHERE p.first_seen >= date('now', ?)
ORDER BY p.first_seen DESC, p.pub_date DESC
"""

df = read_sql(query, params=(f"-{days_back} days",))

if company_filter:
    df = df[df["company"].str.contains(company_filter, case=False, na=False)]

if df.empty:
    st.info(f"No publications found in the last {days_back} days.")
    st.stop()

# ── Metrics ────────────────────────────────────────────────────────────────────
m1, m2, m3 = st.columns(3)
m1.metric("Publications", len(df))
m2.metric("Trials covered", df["nct_id"].nunique())
m3.metric("Companies", df["company"].nunique())

st.markdown("---")

# ── Cards ──────────────────────────────────────────────────────────────────────
for _, row in df.iterrows():
    with st.container():
        st.markdown(f"**{row['title'] or 'Untitled'}**")

        meta = []
        if row.get("journal"):
            meta.append(row["journal"])
        if row.get("pub_date"):
            meta.append(row["pub_date"])
        if meta:
            st.caption(" · ".join(meta))

        c1, c2, c3 = st.columns(3)
        c1.markdown(f"**Company:** {row['company'] or '—'}")
        c2.markdown(f"**Trial:** [{row['nct_id']}](https://clinicaltrials.gov/study/{row['nct_id']})")
        c3.markdown(f"**PubMed:** [PMID {row['pmid']}](https://pubmed.ncbi.nlm.nih.gov/{row['pmid']})")
        st.caption(f"First detected: {row['first_seen']}")
        st.markdown("---")