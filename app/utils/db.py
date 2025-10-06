import os
from functools import lru_cache
from typing import Any

import duckdb
import pandas as pd
import streamlit as st

DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "warehouse/nomad.duckdb")

@lru_cache(maxsize=1)
def _connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(DUCKDB_PATH, read_only=False)
    try:
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute("INSTALL json; LOAD json;")
    except Exception:
        pass
    return con

def get_con() -> duckdb.DuckDBPyConnection:
    return _connect()

@st.cache_data(show_spinner=False)
def query_df(sql: str, params: tuple[Any, ...] = ()) -> pd.DataFrame:
    con = get_con()
    return con.execute(sql, params).fetchdf()

def table_exists(name: str) -> bool:
    con = get_con()
    try:
        con.execute(f"SELECT 1 FROM {name} LIMIT 1")
        return True
    except Exception:
        return False
