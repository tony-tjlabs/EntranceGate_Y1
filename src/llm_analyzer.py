"""Entrance_Analysis_Y1 -- LLM 기반 트래픽 패턴 분석.

날씨, 요일, 출퇴근 흐름, 대기 시간, 유출 속도 등 전체 컨텍스트를
Claude API에 전달하여 입체적 인사이트를 생성한다.
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
_MAX_TOKENS = 1000
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


# ── 데이터 요약 빌더 ──────────────────────────────────────────────────────

DAY_KR = {0: "월", 1: "화", 2: "수", 3: "목", 4: "금", 5: "토", 6: "일"}


def _build_day_summary(
    fine_df: pd.DataFrame,
    date: str,
    weather_map: dict | None = None,
    entry_info: dict | None = None,
    wait_results: list | None = None,
) -> str:
    """날짜 하나의 전체 컨텍스트를 텍스트로 구성."""
    dt = pd.to_datetime(date)
    dow = DAY_KR.get(dt.dayofweek, "")

    # 날씨
    w = (weather_map or {}).get(date, {})
    weather_str = w.get("weather", "Unknown")
    temp_str = ""
    if w.get("temp_min") is not None:
        temp_str = f", {w['temp_min']:.0f}°~{w['temp_max']:.0f}°C"
    precip = w.get("precipitation", 0) or 0
    snow = w.get("snowfall", 0) or 0
    precip_str = ""
    if precip > 0:
        precip_str = f", 강수 {precip:.1f}mm"
    if snow > 0:
        precip_str += f", 적설 {snow:.1f}cm"

    lines = [f"[{date} ({dow}) {weather_str}{temp_str}{precip_str}]"]

    # 시간별 DC
    day_df = fine_df[fine_df["date"] == date].sort_values("time_bin")
    if not day_df.empty:
        hourly = day_df.groupby(day_df["time_bin"] // 60)["dc"].sum()
        h_str = ", ".join(f"{int(h):02d}시:{int(v)}" for h, v in hourly.items() if v > 0)
        lines.append(f"  시간별 DC: {h_str}")

    # 출근 정보
    if entry_info:
        lines.append(
            f"  출근: 러시 {entry_info['rush_start']}~{entry_info['rush_end']} "
            f"({entry_info['rush_duration']}분), "
            f"피크 혼잡 {entry_info['peak_crowd']}명 ({entry_info['peak_time']}), "
            f"평균 통과 {entry_info['avg_throughput']}명/분, "
            f"피크 통과 {entry_info['peak_throughput']}명/분"
        )

    # 퇴근 정보
    if wait_results:
        for wr in wait_results:
            evt = wr["event"]
            stats = wr["stats"]
            lines.append(
                f"  퇴근 {evt['gate_open_time']}: "
                f"대기인원 {evt['peak_dc']}명, "
                f"모임 {evt['gathering_start_time']}~, "
                f"통과 {evt['drain_minutes']}분 (완료 {evt.get('gf_clear_time', '')}), "
                f"평균유출 {evt.get('gf_avg_outflow', 0)}명/분, "
                f"피크유출 {evt.get('gf_peak_outflow', 0)}명/분, "
                f"대기 중앙값={stats['median']}분, 최대={stats['max']}분"
            )

    return "\n".join(lines)


def _build_period_summary(
    fine_df: pd.DataFrame,
    dates: list[str],
    weather_map: dict | None = None,
) -> str:
    """여러 날짜의 간략 요약 (비교 맥락 제공)."""
    lines = []
    for d in dates:
        dt = pd.to_datetime(d)
        dow = DAY_KR.get(dt.dayofweek, "")
        w = (weather_map or {}).get(d, {})
        weather = w.get("weather", "?")
        temp = f"{w.get('temp_min', 0):.0f}°~{w.get('temp_max', 0):.0f}°" if w.get("temp_max") else ""

        day_df = fine_df[fine_df["date"] == d]
        if day_df.empty:
            continue
        total_dc = int(day_df["dc"].sum())
        peak_row = day_df.loc[day_df["dc"].idxmax()]
        peak_time = f"{int(peak_row['time_bin'])//60:02d}:{int(peak_row['time_bin'])%60:02d}"
        peak_dc = int(peak_row["dc"])

        lines.append(f"  {d}({dow}) {weather} {temp}: 총DC={total_dc:,}, 피크={peak_time}(DC={peak_dc})")

    return "\n".join(lines)


# ── LLM 분석 함수 ────────────────────────────────────────────────────────

SYSTEM_PROMPT = """당신은 건설현장 출입구(타각기) 트래픽 분석 전문가입니다.
BLE 센서로 수집된 모바일 디바이스 신호 데이터를 분석합니다.

핵심 배경지식:
- 출근: 게이트 항상 열림, 오는대로 바로 통과 (연속 흐름)
- 퇴근: 게이트 17:30/19:30에 오픈, 사전에 모여서 대기 후 일제 방출
- DC = 5분간 BLE 범위 내 unique 디바이스 수 (혼잡도)
- 유입/유출 속도 = 좁은 BLE 범위(게이트 바로 앞)에서 MAC 추적 기반 (처리량)
- 배경 트래픽 ~60-100 (지나다니는 사람들) → 차감 후 순수 인원
- 날씨/요일이 출퇴근 패턴에 큰 영향

분석 시 주의:
- 데이터 기반 사실만 서술, 추측은 "가능성" 표현
- 효율화/병목 개선 관점의 실용적 인사이트
- 한국어, 간결하게"""


def analyze_daily_pattern(
    fine_df: pd.DataFrame,
    date: str,
    weather_map: dict | None = None,
    entry_info: dict | None = None,
    wait_results: list | None = None,
    other_dates_summary: str = "",
) -> str:
    """특정 날짜의 전체 컨텍스트를 LLM으로 분석."""
    if not is_llm_ready():
        return ""

    target = _build_day_summary(fine_df, date, weather_map, entry_info, wait_results)

    prompt = f"""다음 건설현장 출입구 트래픽 데이터를 분석해주세요.

=== 분석 대상 ===
{target}

=== 최근 날짜 참고 (비교 맥락) ===
{other_dates_summary if other_dates_summary else "(없음)"}

다음을 분석해주세요:
1. 출근 흐름: 러시 시간대, 혼잡도, 처리 속도 평가
2. 퇴근 흐름: 대기 시간, 유출 속도, 병목 정도
3. 날씨/요일 영향: 다른 날과 비교하여 날씨나 요일이 미친 영향
4. 효율화 제안: 병목 개선이나 운영 최적화 포인트

한국어로 간결하게 답변하세요 (5-7문장)."""

    try:
        client = anthropic.Anthropic(api_key=_get_api_key())
        response = client.messages.create(
            model=_MODEL,
            max_tokens=_MAX_TOKENS,
            temperature=_TEMPERATURE,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )
        return response.content[0].text
    except Exception as e:
        logger.warning(f"LLM 분석 실패: {e}")
        return ""


def compare_dates_pattern(
    fine_df: pd.DataFrame,
    dates: list[str],
    weather_map: dict | None = None,
    all_entry_infos: dict | None = None,
    all_wait_results: dict | None = None,
) -> str:
    """여러 날짜의 패턴 차이를 LLM으로 비교 분석."""
    if not is_llm_ready() or len(dates) < 2:
        return ""

    summaries = []
    for d in dates[:5]:
        entry = (all_entry_infos or {}).get(d)
        waits = (all_wait_results or {}).get(d)
        summaries.append(_build_day_summary(fine_df, d, weather_map, entry, waits))

    data_str = "\n\n".join(summaries)

    prompt = f"""다음 건설현장 출입구 트래픽 데이터를 날짜별로 비교 분석해주세요.

{data_str}

다음을 분석해주세요:
1. 날짜 간 핵심 차이: 출근 러시, 퇴근 대기, 유출 속도 비교
2. 날씨·요일 효과: 눈/비/한파가 트래픽에 미친 영향
3. 병목 패턴: 어떤 조건에서 대기 시간이 길어지는지
4. 운영 인사이트: 날씨/요일별 게이트 운영 최적화 제안

한국어로 간결하게 답변하세요 (5-7문장)."""

    try:
        client = anthropic.Anthropic(api_key=_get_api_key())
        response = client.messages.create(
            model=_MODEL,
            max_tokens=_MAX_TOKENS,
            temperature=_TEMPERATURE,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )
        return response.content[0].text
    except Exception as e:
        logger.warning(f"LLM 비교 분석 실패: {e}")
        return ""
