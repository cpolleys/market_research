"""
epidemiology.py
===============
Pulls prevalence and incidence data for conditions tracked in the
disease_landscape table.  Two-tier lookup:

  Tier 1 — WHO Global Health Observatory (GHO) OData API
            Free, no auth, ~2 300 indicators, good for common NCDs.
            Base URL: https://ghoapi.azureedge.net/api/

  Tier 2 — GBD CSV cache (local file)
            One-time manual download from vizhub.healthdata.org/gbd-results/
            Much finer disease granularity; required for rare / oncology indications.

Usage
-----
  from epidemiology import get_prevalence, refresh_all_conditions

  # Single condition
  result = get_prevalence("Type 2 Diabetes")

  # Refresh the whole landscape
  refresh_all_conditions()

GBD CSV Setup (one-time)
------------------------
1. Go to https://vizhub.healthdata.org/gbd-results/ (free account required)
2. Select:
     Measure   : Prevalence, Incidence
     Age       : All ages  (add Age-standardized if you want rates)
     Metric    : Number, Rate
     Year      : most recent (2021 or 2019 for older study)
     Location  : Global  +  United States of America
     Sex       : Both
   Download as CSV and save to the path set in GBD_CSV_PATH below.
3. The loader handles the standard IHME column layout automatically.
"""

from __future__ import annotations

import os
import re
import time
import sqlite3
import logging
import requests
import pandas as pd
from datetime import datetime
from difflib import get_close_matches
from typing import Optional
from db import get_conn

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

GHO_BASE       = "https://ghoapi.azureedge.net/api"
GBD_CSV_PATH   = os.environ.get("C:\\Users\\chris\\Documents\\personal_projects", "gbd_data.csv")   # set env var or edit here
US_POPULATION  = 335_000_000   # used to convert global rates → US estimates when needed
REQUEST_DELAY  = 0.3           # seconds between GHO API calls

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s %(message)s",
    handlers=[logging.StreamHandler(__import__("sys").stdout)]  # stdout = black in Jupyter, not red
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# GHO indicator catalogue
# Maps our free-text condition names → GHO indicator codes.
# Values are (prevalence_code, incidence_code); None = not available in GHO.
#
# To find new codes:
#   GET https://ghoapi.azureedge.net/api/Indicator?$filter=contains(IndicatorName,'diabetes')
# ---------------------------------------------------------------------------

GHO_CONDITION_MAP = {
    # ── Metabolic / Cardiovascular ──────────────────────────────────────────
    "type 2 diabetes": {
        "prevalence": "NCD_GLUC_04",        # Age-standardised prevalence of raised fasting blood glucose
        "incidence":  None,
    },
    "diabetes": {
        "prevalence": "NCD_GLUC_04",
        "incidence":  None,
    },
    "hypertension": {
        "prevalence": "NCD_HYP_PREVALENCE_A",
        "incidence":  None,
    },
    "raised blood pressure": {
        "prevalence": "NCD_HYP_PREVALENCE_A",
        "incidence":  None,
    },
    "obesity": {
        "prevalence": "NCD_BMI_30C",        # Prevalence of obesity among adults (BMI ≥ 30)
        "incidence":  None,
    },
    "cardiovascular disease": {
        "prevalence": None,
        "incidence":  None,                 # Use GBD for CVD — GHO lacks a direct prevalence code
    },
    "st elevation myocardial infarction": {
        "prevalence": None,
        "incidence":  None,                 # GBD only
    },
    "heart failure": {
        "prevalence": None,
        "incidence":  None,
    },

    # ── Infectious disease ───────────────────────────────────────────────────
    "hiv": {
        "prevalence": "HIV_0000000026",     # People living with HIV (number)
        "incidence":  "HIV_0000000001",     # New HIV infections (number)
    },
    "tuberculosis": {
        "prevalence": "MDG_0000000020",     # TB prevalence per 100k
        "incidence":  "MDG_0000000020",
    },
    "malaria": {
        "prevalence": None,
        "incidence":  "MALARIA_EST_INCIDENCE",
    },
    "hepatitis b": {
        "prevalence": "HBsAg_Prevalence",
        "incidence":  None,
    },
    "hepatitis c": {
        "prevalence": "HEPATITIS_C_PREVALENCE",
        "incidence":  None,
    },

    # ── Oncology ─────────────────────────────────────────────────────────────
    # GHO has very limited cancer prevalence data; GBD is strongly preferred.
    "breast cancer": {
        "prevalence": None,
        "incidence":  None,
    },
    "lung cancer": {
        "prevalence": None,
        "incidence":  None,
    },
    "colorectal cancer": {
        "prevalence": None,
        "incidence":  None,
    },
    "pancreatic cancer": {
        "prevalence": None,
        "incidence":  None,
    },
    "non-small cell lung cancer": {
        "prevalence": None,
        "incidence":  None,
    },

    # ── Rare / Genetic ───────────────────────────────────────────────────────
    "sickle cell disease": {
        "prevalence": None,
        "incidence":  None,
    },
    "beta-thalassemia": {
        "prevalence": None,
        "incidence":  None,
    },
    "hereditary angioedema": {
        "prevalence": None,
        "incidence":  None,
    },
    "phenylketonuria": {
        "prevalence": None,
        "incidence":  None,
    },

    # ── Neurology / CNS ──────────────────────────────────────────────────────
    "depression": {
        "prevalence": "MENTAL_DEPRESSION_PREVALENCE",
        "incidence":  None,
    },
    "alzheimer": {
        "prevalence": None,
        "incidence":  None,
    },
    "parkinson": {
        "prevalence": None,
        "incidence":  None,
    },
    "amyotrophic lateral sclerosis": {
        "prevalence": None,
        "incidence":  None,
    },

    # ── Respiratory ──────────────────────────────────────────────────────────
    "asthma": {
        "prevalence": None,
        "incidence":  None,
    },
    "chronic obstructive pulmonary disease": {
        "prevalence": None,
        "incidence":  None,
    },
    "copd": {
        "prevalence": None,
        "incidence":  None,
    },

    # ── Immunology / Autoimmune ──────────────────────────────────────────────
    "atopic dermatitis": {
        "prevalence": None,
        "incidence":  None,
    },
    "crohn disease": {
        "prevalence": None,
        "incidence":  None,
    },
    "ulcerative colitis": {
        "prevalence": None,
        "incidence":  None,
    },
    "rheumatoid arthritis": {
        "prevalence": None,
        "incidence":  None,
    },
}

# ---------------------------------------------------------------------------
# GBD cause-name lookup (partial; extend as needed)
# Maps our condition strings → GBD cause names in the CSV export.
# GBD uses its own cause hierarchy so the names differ from ClinicalTrials.
# ---------------------------------------------------------------------------

GBD_CONDITION_MAP = {
    "cardiovascular disease":               "Cardiovascular diseases",
    "st elevation myocardial infarction":   "Ischemic heart disease",
    "heart failure":                        "Ischemic heart disease",
    "breast cancer":                        "Breast cancer",
    "lung cancer":                          "Tracheal, bronchus, and lung cancer",
    "non-small cell lung cancer":           "Tracheal, bronchus, and lung cancer",
    "colorectal cancer":                    "Colon and rectum cancer",
    "pancreatic cancer":                    "Pancreatic cancer",
    "sickle cell disease":                  "Sickle cell disorders",
    "beta-thalassemia":                     "Thalassemias",
    "amyotrophic lateral sclerosis":        "Motor neuron disease",
    "alzheimer":                            "Alzheimer's disease and other dementias",
    "parkinson":                            "Parkinson's disease",
    "asthma":                               "Asthma",
    "copd":                                 "Chronic obstructive pulmonary disease",
    "chronic obstructive pulmonary disease":"Chronic obstructive pulmonary disease",
    "crohn disease":                        "Inflammatory bowel disease",
    "ulcerative colitis":                   "Inflammatory bowel disease",
    "rheumatoid arthritis":                 "Rheumatoid arthritis",
    "atopic dermatitis":                    "Other skin and subcutaneous diseases",
    "type 2 diabetes":                      "Diabetes mellitus type 2",
    "diabetes":                             "Diabetes mellitus",
    "hypertension":                         "Hypertensive heart disease",
    "depression":                           "Major depressive disorder",
    "hereditary angioedema":                None,   # not in GBD
    "phenylketonuria":                      None,
}


# ---------------------------------------------------------------------------
# DB schema
# ---------------------------------------------------------------------------

def init_epidemiology_table():
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS epidemiology (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            condition            TEXT NOT NULL,
            condition_normalized TEXT NOT NULL,
            measure              TEXT NOT NULL,   -- 'prevalence' or 'incidence'
            metric               TEXT NOT NULL,   -- 'number' or 'rate_per_100k'
            location             TEXT NOT NULL,   -- 'global' or 'united states'
            value                REAL,
            year                 INTEGER,
            source               TEXT,            -- 'gho' or 'gbd'
            indicator_code       TEXT,
            snapshot_date        TEXT NOT NULL,
            UNIQUE(condition_normalized, measure, metric, location, source)
        )
    """)
    conn.commit()
    conn.close()
    log.info("epidemiology table ready")


def upsert_epi_row(condition, measure, metric, location, value, year, source, indicator_code=None):
    conn  = get_conn()
    cur   = conn.cursor()
    today = datetime.utcnow().date().isoformat()
    norm  = _normalize(condition)

    cur.execute("""
        INSERT INTO epidemiology
            (condition, condition_normalized, measure, metric, location,
             value, year, source, indicator_code, snapshot_date)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(condition_normalized, measure, metric, location, source)
        DO UPDATE SET
            value          = excluded.value,
            year           = excluded.year,
            indicator_code = excluded.indicator_code,
            snapshot_date  = excluded.snapshot_date
    """, (condition, norm, measure, metric, location,
          value, year, source, indicator_code, today))

    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _normalize(text: str) -> str:
    """Lowercase, strip punctuation, collapse whitespace."""
    t = text.lower().strip()
    t = re.sub(r"[^a-z0-9\s]", " ", t)
    return re.sub(r"\s+", " ", t).strip()


def _fuzzy_match(query: str, candidates: list[str], cutoff: float = 0.6) -> str | None:
    """Return the best fuzzy match from a list, or None."""
    q = _normalize(query)
    normed = {_normalize(c): c for c in candidates}
    hit = get_close_matches(q, list(normed.keys()), n=1, cutoff=cutoff)
    return normed[hit[0]] if hit else None


def _map_condition(condition: str, mapping: dict) -> str | None:
    """
    Try exact-then-fuzzy lookup in a condition → code/name mapping dict.
    Returns the value (code or GBD cause name) or None.

    Fuzzy matching is intentionally conservative (cutoff=0.75) to avoid
    false positives between superficially similar disease names.
    """
    norm = _normalize(condition)
    # exact
    if norm in mapping:
        return mapping[norm]
    # word-overlap shortcut: require at least one shared meaningful token
    norm_tokens = set(norm.split()) - {"the", "of", "and", "or", "in", "a"}
    for key in mapping:
        key_tokens = set(key.split()) - {"the", "of", "and", "or", "in", "a"}
        if norm_tokens & key_tokens:                         # non-empty intersection
            hit = _fuzzy_match(norm, [key], cutoff=0.75)
            if hit:
                log.debug("Fuzzy matched '%s' → '%s'", condition, hit)
                return mapping[hit]
    return None


# ---------------------------------------------------------------------------
# Tier 1: WHO GHO API
# ---------------------------------------------------------------------------

def _gho_get(path: str, params: dict | None = None) -> dict | None:
    url = f"{GHO_BASE}/{path}"
    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as exc:
        log.warning("GHO request failed (%s): %s", url, exc)
        return None


def _gho_fetch_indicator(code: str, country_code: str = "USA") -> list[dict]:
    """
    Fetch data rows for a GHO indicator, filtered to a single country.
    Returns a list of value dicts.  Falls back to global if country has no data.
    """
    data = _gho_get(code, params={"$filter": f"SpatialDim eq '{country_code}'"})
    if data and data.get("value"):
        return data["value"]

    # Try global (GLOBAL or no filter)
    data = _gho_get(code, params={"$filter": "SpatialDim eq 'GLOBAL'"})
    if data and data.get("value"):
        return data["value"]

    return []


def _gho_latest(rows: list[dict]) -> dict | None:
    """Pick the most recent row from GHO result set (both-sex if available)."""
    if not rows:
        return None

    # prefer BTSX (both sexes), then any
    filtered = [r for r in rows if r.get("Dim1") in ("BTSX", None, "")]
    pool = filtered if filtered else rows

    # sort descending by year
    pool.sort(key=lambda r: r.get("TimeDim", 0) or 0, reverse=True)
    return pool[0]


def fetch_from_gho(condition: str) -> list[dict]:
    """
    Look up a condition in the GHO catalogue and return upserted rows.
    Returns a list of result dicts (may be empty if no GHO mapping exists).
    """
    entry = _map_condition(condition, GHO_CONDITION_MAP)
    if entry is None:
        log.debug("No GHO mapping for '%s'", condition)
        return []

    results = []
    for measure in ("prevalence", "incidence"):
        code = entry.get(measure)
        if not code:
            continue

        time.sleep(REQUEST_DELAY)
        rows = _gho_fetch_indicator(code, country_code="USA")
        location = "united states" if any(r.get("SpatialDim") == "USA" for r in rows) else "global"
        row  = _gho_latest(rows)
        if row is None:
            log.debug("No GHO data for %s / %s", condition, measure)
            continue

        value = row.get("NumericValue")
        year  = row.get("TimeDim")

        if value is None:
            continue

        # GHO values are usually rates per 100k or percentages; tag accordingly
        metric = "rate_per_100k"

        upsert_epi_row(condition, measure, metric, location, value, year, "gho", code)
        results.append({
            "condition": condition,
            "measure":   measure,
            "metric":    metric,
            "location":  location,
            "value":     value,
            "year":      year,
            "source":    "gho",
        })
        log.info("GHO  %-40s %-12s %s = %.1f (%s)", condition, measure, metric, value, year)

    return results


# ---------------------------------------------------------------------------
# Tier 2: GBD CSV
# ---------------------------------------------------------------------------

_gbd_df = None  # type: Optional[pd.DataFrame]  -- module-level cache


def _load_gbd_csv():
    """
    Load and cache the GBD CSV export.  Returns None if the file is missing.

    Expected columns (standard IHME export layout):
        measure_name, location_name, sex_name, age_name,
        cause_name, metric_name, year, val, upper, lower

    The GBD export also includes a population_group_name column when you
    select sub-populations.  We filter to the standard (unrestricted)
    population only to avoid inflated aggregate rows.
    """
    global _gbd_df
    if _gbd_df is not None:
        return _gbd_df

    if not os.path.exists(GBD_CSV_PATH):
        log.warning(
            "GBD CSV not found at '%s'. "
            "Download from https://vizhub.healthdata.org/gbd-results/ "
            "and set GBD_CSV_PATH.", GBD_CSV_PATH
        )
        return None

    log.info("Loading GBD CSV from %s …", GBD_CSV_PATH)
    df = pd.read_csv(GBD_CSV_PATH, low_memory=False)

    # normalise column names (IHME sometimes ships with slight variations)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    required = {"measure_name", "location_name", "cause_name", "metric_name", "year", "val"}
    missing  = required - set(df.columns)
    if missing:
        log.error("GBD CSV missing expected columns: %s", missing)
        return None

    # Filter to the standard (unrestricted) population group only.
    # GBD exports include a population_group_name column when sub-populations
    # are selected; keeping all groups produces duplicate inflated rows.
    if "population_group_name" in df.columns:
        standard_labels = {"standard population", "total population", "all populations", "all population"}
        mask = df["population_group_name"].str.lower().isin(standard_labels)
        if mask.any():
            df = df[mask].copy()
            log.info("Filtered to standard population group (%d rows)", len(df))
        else:
            # Log the unique values so the user knows what's in their CSV
            unique_groups = df["population_group_name"].unique().tolist()
            log.warning(
                "population_group_name column found but no standard label matched. "
                "Unique values: %s — keeping all rows. "
                "Set the correct label in _load_gbd_csv() if results look inflated.",
                unique_groups
            )

    # keep only global + US rows for speed
    locs = {"global", "united states of america"}
    df = df[df["location_name"].str.lower().isin(locs)].copy()

    # keep both-sex rows
    if "sex_name" in df.columns:
        df = df[df["sex_name"].str.lower().isin({"both", "both sexes"})].copy()

    # keep all-ages rows
    if "age_name" in df.columns:
        df = df[df["age_name"].str.lower().isin({"all ages", "age-standardized"})].copy()

    _gbd_df = df
    log.info("GBD CSV loaded: %d rows after filtering", len(df))

    # Print the unique population groups actually present so user can verify
    if "population_group_name" in df.columns:
        log.info("Population groups in filtered data: %s",
                 df["population_group_name"].unique().tolist())

    return _gbd_df


def fetch_from_gbd(condition: str) -> list[dict]:
    """
    Query the local GBD CSV for a condition.
    Returns a list of result dicts.
    """
    df = _load_gbd_csv()
    if df is None:
        return []

    gbd_cause = _map_condition(condition, GBD_CONDITION_MAP)
    if gbd_cause is None:
        # Try fuzzy match directly against all cause names in the CSV
        all_causes = df["cause_name"].unique().tolist()
        gbd_cause  = _fuzzy_match(condition, all_causes, cutoff=0.55)

    if gbd_cause is None:
        log.debug("No GBD cause mapping for '%s'", condition)
        return []

    subset = df[df["cause_name"].str.lower() == gbd_cause.lower()]
    if subset.empty:
        log.debug("GBD CSV: no rows for cause '%s'", gbd_cause)
        return []

    # Take the most recent year available
    latest_year = subset["year"].max()
    subset = subset[subset["year"] == latest_year]

    results = []
    for _, row in subset.iterrows():
        measure_raw = row.get("measure_name", "").lower()
        if "prevalence" in measure_raw:
            measure = "prevalence"
        elif "incidence" in measure_raw:
            measure = "incidence"
        else:
            continue

        metric_raw = row.get("metric_name", "").lower()
        if "number" in metric_raw:
            metric = "number"
        elif "rate" in metric_raw:
            metric = "rate_per_100k"
        elif "percent" in metric_raw:
            metric = "percent"
        else:
            metric = metric_raw

        location = row.get("location_name", "").lower()
        if "united states" in location:
            location = "united states"
        else:
            location = "global"

        value = row.get("val")
        if pd.isna(value):
            continue

        upsert_epi_row(condition, measure, metric, location, float(value),
                       int(latest_year), "gbd", gbd_cause)
        results.append({
            "condition": condition,
            "measure":   measure,
            "metric":    metric,
            "location":  location,
            "value":     float(value),
            "year":      int(latest_year),
            "source":    "gbd",
        })
        log.info("GBD  %-40s %-12s %-12s = %.0f (%s)", condition, measure, metric, value, latest_year)

    return results


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------

def get_prevalence(condition: str, prefer_us: bool = True) -> dict | None:
    """
    Return the best available prevalence estimate for a condition.

    Lookup order:
      1. DB cache (today's snapshot)
      2. GHO API  (live)
      3. GBD CSV  (local cache)

    Returns a dict with keys: condition, measure, metric, location, value, year, source
    Or None if nothing is found.
    """
    init_epidemiology_table()

    norm     = _normalize(condition)
    location_pref = "united states" if prefer_us else "global"

    # -- Check DB cache first ------------------------------------------------
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("""
        SELECT condition, measure, metric, location, value, year, source
        FROM epidemiology
        WHERE condition_normalized = ?
        AND   measure = 'prevalence'
        ORDER BY
            CASE WHEN location = ? THEN 0 ELSE 1 END,
            year DESC
        LIMIT 1
    """, (norm, location_pref))
    row = cur.fetchone()
    conn.close()

    if row:
        keys = ("condition", "measure", "metric", "location", "value", "year", "source")
        return dict(zip(keys, row))

    # -- Live fetch ----------------------------------------------------------
    results = fetch_from_gho(condition)
    if not results:
        results = fetch_from_gbd(condition)

    if not results:
        log.warning("No epidemiology data found for '%s'", condition)
        return None

    # Return the US prevalence preferentially, else global
    prevalence_rows = [r for r in results if r["measure"] == "prevalence"]
    if not prevalence_rows:
        return None

    us_rows = [r for r in prevalence_rows if r["location"] == "united states"]
    return (us_rows or prevalence_rows)[0]


def refresh_all_conditions(conditions: list[str] | None = None):
    """
    Refresh epidemiology data for all conditions in disease_landscape,
    or for a supplied list.

    Usage:
        refresh_all_conditions()                    # all conditions in DB
        refresh_all_conditions(["Breast Cancer"])   # specific list
    """
    if conditions is None:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("SELECT DISTINCT condition_searched FROM disease_landscape")
        conditions = [r[0] for r in cur.fetchall()]
        conn.close()

    log.info("Refreshing epidemiology for %d conditions", len(conditions))

    for condition in conditions:
        log.info("── %s", condition)
        fetched = fetch_from_gho(condition)
        if not any(r["measure"] == "prevalence" for r in fetched):
            fetch_from_gbd(condition)
        time.sleep(REQUEST_DELAY)

    log.info("Refresh complete.")


def get_epi_summary(conditions: list[str] | None = None) -> pd.DataFrame:
    """
    Return a DataFrame summarising cached epidemiology data,
    optionally filtered to a list of conditions.

    Columns: condition, measure, metric, location, value, year, source
    """
    init_epidemiology_table()
    conn = get_conn()

    base_query = """
        SELECT condition, measure, metric, location, value, year, source
        FROM epidemiology
    """
    if conditions:
        norms       = [_normalize(c) for c in conditions]
        placeholders = ",".join("?" * len(norms))
        base_query  += f" WHERE condition_normalized IN ({placeholders})"
        df = pd.read_sql_query(base_query, conn, params=norms)
    else:
        df = pd.read_sql_query(base_query, conn)

    conn.close()
    return df


# ---------------------------------------------------------------------------
# CLI convenience
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        condition = " ".join(sys.argv[1:])
        result    = get_prevalence(condition)
        print(result)
    else:
        refresh_all_conditions()
