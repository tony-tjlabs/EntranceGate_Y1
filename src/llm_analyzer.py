"""Entrance_Analysis_Y1 -- LLM 기반 트래픽 패턴 분석.

날짜별 5분 단위 트래픽 데이터의 이상 패턴, 피크 시간 차이 등을
Claude API로 분석하여 자연어 인사이트를 생성한다.
"""
from __future__ import annotations

import os
import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

try:
    import anthropic
    _ANTHROPIC_AVAILABLE = True
except ImportError:
    _ANTHROPIC_AVAILABLE = False

try:
    from dotenv import load_dotenv
    _PROJECT_ROOT = Path(__file__).resolve().parent.parent
    _ENV_PATH = _PROJECT_ROOT / ".env"
    if _ENV_PATH.exists():
        load_dotenv(_ENV_PATH, override=True)
except ImportError:
    pass

try:
    import streamlit as st
    _ST = True
except ImportError:
    _ST = False

_MODEL = "claude-sonnet-4-5"
_MAX_TOKENS = 600
_TEMPERATURE = 0.3


def _get_api_key() -> str | None:
    if _ST:
        try:
            key = st.secrets.get("ANTHROPIC_API_KEY")
            if key:
                return key
        except Exception:
            pass
    return os.getenv("ANTHROPIC_API_KEY") or None


def is_llm_ready() -> bool:
    return _ANTHROPIC_AVAILABLE and _get_api_key() is not None


def analyze_daily_pattern(
    fine_df: pd.DataFrame,
    date: str,
    commute_info: dict | None = None,
) -> str:
    """특정 날짜의 5분 단위 트래픽 패턴을 LLM으로 분석.

    Args:
        fine_df: 5분 단위 집계 DataFrame
        date: 분석 대상 날짜
        commute_info: 출퇴근 시간 정보 dict (optional)

    Returns:
        LLM 분석 결과 문자열. 실패 시 빈 문자열.
    """
    if not is_llm_ready():
        return ""

    day_df = fine_df[fine_df["date"] == date].sort_values("time_bin")
    if day_df.empty:
        return ""

    # 핵심 데이터 요약 (토큰 절약)
    # 시간별 DC 합산
    hourly = day_df.groupby(day_df["time_bin"] // 60)["dc"].sum()
    hourly_str = ", ".join(f"{int(h):02d}시:{int(v)}" for h, v in hourly.items() if v > 0)

    # 피크 구간
    top5 = day_df.nlargest(5, "dc")
    peaks_str = ", ".join(
        f"{int(r['time_bin'])//60:02d}:{int(r['time_bin'])%60:02d}(DC={int(r['dc'])})"
        for _, r in top5.iterrows()
    )

    commute_str = ""
    if commute_info:
        commute_str = (
            f"\n출근 시작: {commute_info.get('entry_start', '-')}, "
            f"출근 피크: {commute_info.get('entry_peak', '-')}, "
            f"퇴근 피크: {commute_info.get('exit_peak', '-')}, "
            f"퇴근 종료: {commute_info.get('exit_end', '-')}"
        )

    prompt = f"""건설현장 출입구(타각기) BLE 트래픽 데이터를 분석해주세요.

날짜: {date}
시간별 DC(디바이스 카운트): {hourly_str}
피크 5분 구간: {peaks_str}{commute_str}

다음을 간결하게 분석해주세요 (3-4문장):
1. 이 날의 출퇴근 패턴 특징 (다른 날과 다른 점이 있다면)
2. 피크 시간대가 평소(출근 06시, 퇴근 17:30)와 다르다면 가능한 원인
3. 특이한 패턴 (점심 시간 변동, 야간 잔류, 이중 피크 등)

한국어로 답변하세요."""

    try:
        client = anthropic.Anthropic(api_key=_get_api_key())
        response = client.messages.create(
            model=_MODEL,
            max_tokens=_MAX_TOKENS,
            temperature=_TEMPERATURE,
            messages=[{"role": "user", "content": prompt}],
        )
        return response.content[0].text
    except Exception as e:
        logger.warning(f"LLM 분석 실패: {e}")
        return ""


def compare_dates_pattern(
    fine_df: pd.DataFrame,
    dates: list[str],
) -> str:
    """여러 날짜의 패턴 차이를 LLM으로 비교 분석."""
    if not is_llm_ready() or len(dates) < 2:
        return ""

    summaries = []
    for date in dates[:5]:  # 최대 5일
        day_df = fine_df[fine_df["date"] == date].sort_values("time_bin")
        if day_df.empty:
            continue
        hourly = day_df.groupby(day_df["time_bin"] // 60)["dc"].sum()
        peak_bin = day_df.loc[day_df["dc"].idxmax()]
        peak_time = f"{int(peak_bin['time_bin'])//60:02d}:{int(peak_bin['time_bin'])%60:02d}"
        total_dc = int(day_df["dc"].sum())
        summaries.append(f"{date}: 총DC={total_dc}, 피크={peak_time}(DC={int(peak_bin['dc'])})")

    data_str = "\n".join(summaries)

    prompt = f"""건설현장 출입구 BLE 트래픽 날짜별 비교 분석을 해주세요.

{data_str}

다음을 간결하게 분석해주세요 (3-4문장):
1. 날짜 간 패턴 차이의 핵심 (피크 시간, 총량 등)
2. 가능한 원인 추정 (요일 효과, 공사 일정 변동, 날씨 등)
3. 주목할 만한 이상 패턴

한국어로 답변하세요."""

    try:
        client = anthropic.Anthropic(api_key=_get_api_key())
        response = client.messages.create(
            model=_MODEL,
            max_tokens=_MAX_TOKENS,
            temperature=_TEMPERATURE,
            messages=[{"role": "user", "content": prompt}],
        )
        return response.content[0].text
    except Exception as e:
        logger.warning(f"LLM 비교 분석 실패: {e}")
        return ""
