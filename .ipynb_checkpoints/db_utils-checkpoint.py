import os
import sqlite3
import pandas as pd

DB_PATH = os.environ.get("DB_PATH", "../biotech.db")


def get_conn():
    return sqlite3.connect(DB_PATH)


def read_sql(query: str, params=()) -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df

def table_exists(name: str) -> bool:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,)
    )
    exists = cur.fetchone() is not None
    conn.close()
    return exists


