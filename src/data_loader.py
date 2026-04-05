"""배포용 데이터 로더 — 캐시 Parquet 전용. 전처리 코드 없음."""
from __future__ import annotations

import json
import os
from typing import Optional

import pandas as pd
import streamlit as st

CACHE_VERSION = "6.1"


def is_cache_valid(cache_dir: str) -> bool:
    meta_path = os.path.join(cache_dir, "meta.json")
    if not os.path.exists(meta_path):
        return False
    try:
        with open(meta_path, "r") as f:
            return json.load(f).get("cache_version") == CACHE_VERSION
    except Exception:
        return False


@st.cache_data(show_spinner=False, ttl=None)
def load_daily_summary(cache_dir: str) -> pd.DataFrame:
    return pd.read_parquet(os.path.join(cache_dir, "daily_summary.parquet"))


@st.cache_data(show_spinner=False, ttl=None)
def load_hourly_summary(cache_dir: str) -> pd.DataFrame:
    return pd.read_parquet(os.path.join(cache_dir, "hourly_summary.parquet"))


@st.cache_data(show_spinner=False, ttl=None)
def load_gateway_summary(cache_dir: str) -> pd.DataFrame:
    return pd.read_parquet(os.path.join(cache_dir, "gateway_summary.parquet"))


@st.cache_data(show_spinner=False, ttl=None)
def load_gateway_daily(cache_dir: str) -> pd.DataFrame:
    return pd.read_parquet(os.path.join(cache_dir, "gateway_daily.parquet"))


@st.cache_data(show_spinner=False, ttl=None)
def load_fine_summary(cache_dir: str) -> pd.DataFrame:
    return pd.read_parquet(os.path.join(cache_dir, "fine_summary.parquet"))


@st.cache_data(show_spinner=False, ttl=None)
def load_gate_flow(cache_dir: str) -> pd.DataFrame:
    path = os.path.join(cache_dir, "gate_flow.parquet")
    if not os.path.exists(path):
        return pd.DataFrame()
    return pd.read_parquet(path)


def load_meta(cache_dir: str) -> Optional[dict]:
    meta_path = os.path.join(cache_dir, "meta.json")
    if not os.path.exists(meta_path):
        return None
    with open(meta_path, "r") as f:
        return json.load(f)
