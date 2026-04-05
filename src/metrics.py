"""Entrance_Analysis_Y1 -- 메트릭 계산 모듈.

집계된 Parquet 데이터로부터 대시보드 메트릭을 산출한다.
"""
from __future__ import annotations

import pandas as pd
import numpy as np

# 건설현장 시간대 구분
TIME_PERIODS = [
    (0, 4, "심야", "off"),
    (4, 7, "출근", "transit"),
    (7, 12, "오전작업", "work"),
    (12, 13, "점심", "rest"),
    (13, 17, "오후작업", "work"),
    (17, 20, "퇴근", "transit"),
    (20, 24, "야간", "off"),
]

# 설 연휴 등 특이일 (데이터 패턴 기반)
SPECIAL_DAYS = {
    "2026-01-01": "신정",
    "2026-02-16": "설 연휴",
    "2026-02-17": "설 연휴",
}


def get_time_period(hour: int) -> str:
    """시간 -> 건설현장 시간대명 반환."""
    for start, end, name, _ in TIME_PERIODS:
        if start <= hour < end:
            return name
    return "기타"


def get_day_type(date_str: str) -> str:
    """날짜 -> 평일/주말/공휴일 구분."""
    if date_str in SPECIAL_DAYS:
        return "공휴일"
    dt = pd.to_datetime(date_str)
    return "주말" if dt.dayofweek >= 5 else "평일"


def compute_overview_metrics(
    daily_df: pd.DataFrame,
    hourly_df: pd.DataFrame,
) -> dict:
    """개요 탭 메트릭 카드용 데이터 산출.

    Returns:
        dict with keys: total_records, avg_daily_udc, peak_am_hour, peak_pm_hour,
        ios_ratio, android_ratio, data_days, date_range
    """
    total_records = int(daily_df["total_records"].sum())
    avg_daily_udc = int(daily_df["udc"].mean())

    # 출퇴근 피크
    hourly_avg = hourly_df.groupby("hour")["dc"].mean()
    am_hours = hourly_avg[(hourly_avg.index >= 4) & (hourly_avg.index < 10)]
    pm_hours = hourly_avg[(hourly_avg.index >= 15) & (hourly_avg.index < 21)]

    peak_am_hour = int(am_hours.idxmax()) if not am_hours.empty else 8
    peak_am_dc = int(am_hours.max()) if not am_hours.empty else 0
    peak_pm_hour = int(pm_hours.idxmax()) if not pm_hours.empty else 17
    peak_pm_dc = int(pm_hours.max()) if not pm_hours.empty else 0

    # iOS / Android
    total_ios = int(daily_df["ios_records"].sum())
    total_android = int(daily_df["android_records"].sum())
    total_dev = total_ios + total_android
    ios_ratio = total_ios / total_dev if total_dev > 0 else 0
    android_ratio = total_android / total_dev if total_dev > 0 else 0

    return {
        "total_records": total_records,
        "avg_daily_udc": avg_daily_udc,
        "peak_am_hour": peak_am_hour,
        "peak_am_dc": peak_am_dc,
        "peak_pm_hour": peak_pm_hour,
        "peak_pm_dc": peak_pm_dc,
        "ios_ratio": ios_ratio,
        "android_ratio": android_ratio,
        "data_days": len(daily_df),
        "date_range": (daily_df["date"].min(), daily_df["date"].max()),
    }


def compute_peak_analysis(hourly_df: pd.DataFrame) -> dict:
    """출퇴근 피크 상세 분석.

    Returns:
        dict with am/pm peak details + lunch/night stats
    """
    hourly_avg = hourly_df.groupby("hour")["dc"].mean()

    # 출근 피크 (04-10, 건설현장 이른 출근)
    am_range = hourly_avg[(hourly_avg.index >= 4) & (hourly_avg.index < 10)]
    # 퇴근 피크 (16-20)
    pm_range = hourly_avg[(hourly_avg.index >= 16) & (hourly_avg.index < 20)]
    # 점심 (12-13)
    lunch_avg = float(hourly_avg.get(12, 0))
    # 야간 잔류 (22-06)
    night_hours = hourly_avg[(hourly_avg.index >= 22) | (hourly_avg.index < 6)]
    night_avg = float(night_hours.mean()) if not night_hours.empty else 0

    # 작업시간 대비 점심 감소율 (피크 대비)
    work_peak = float(hourly_avg[(hourly_avg.index >= 4) & (hourly_avg.index < 10)].max())
    lunch_drop = (1 - lunch_avg / work_peak) * 100 if work_peak > 0 else 0

    return {
        "peak_am_hour": int(am_range.idxmax()) if not am_range.empty else 8,
        "peak_am_dc": int(am_range.max()) if not am_range.empty else 0,
        "peak_pm_hour": int(pm_range.idxmax()) if not pm_range.empty else 17,
        "peak_pm_dc": int(pm_range.max()) if not pm_range.empty else 0,
        "lunch_avg_dc": int(lunch_avg),
        "lunch_drop_pct": round(lunch_drop, 1),
        "night_avg_dc": int(night_avg),
    }


def compute_weekday_stats(daily_df: pd.DataFrame) -> pd.DataFrame:
    """요일별 통계.

    Returns:
        DataFrame: dayofweek, day_name, mean_udc, median_udc, std_udc, count
    """
    df = daily_df.copy()
    df["dayofweek"] = pd.to_datetime(df["date"]).dt.dayofweek
    df["day_name"] = pd.to_datetime(df["date"]).dt.day_name()

    day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    day_kr = {"Monday": "월", "Tuesday": "화", "Wednesday": "수", "Thursday": "목",
              "Friday": "금", "Saturday": "토", "Sunday": "일"}

    stats = df.groupby(["dayofweek", "day_name"]).agg(
        mean_udc=("udc", "mean"),
        median_udc=("udc", "median"),
        std_udc=("udc", "std"),
        count=("udc", "count"),
    ).reset_index().sort_values("dayofweek")

    stats["day_kr"] = stats["day_name"].map(day_kr)
    return stats


def compute_weekly_trend(daily_df: pd.DataFrame) -> pd.DataFrame:
    """주차별 UDC 추이.

    Returns:
        DataFrame: week_num, week_label, mean_udc, total_records
    """
    df = daily_df.copy()
    dt = pd.to_datetime(df["date"])
    df["week_num"] = dt.dt.isocalendar().week.astype(int)
    df["year"] = dt.dt.year

    weekly = df.groupby(["year", "week_num"]).agg(
        mean_udc=("udc", "mean"),
        total_records=("total_records", "sum"),
        days=("date", "count"),
    ).reset_index()

    weekly = weekly.sort_values(["year", "week_num"])
    weekly["week_label"] = "W" + weekly["week_num"].astype(str)
    return weekly


def compute_monthly_comparison(daily_df: pd.DataFrame) -> dict:
    """월별 비교 통계.

    Returns:
        dict with jan/feb summary + change rates
    """
    df = daily_df.copy()
    df["month"] = pd.to_datetime(df["date"]).dt.month

    result = {}
    for month, label in [(1, "jan"), (2, "feb")]:
        m_df = df[df["month"] == month]
        if m_df.empty:
            result[label] = {"avg_udc": 0, "max_udc": 0, "min_udc": 0, "std_udc": 0,
                             "total_records": 0, "days": 0}
            continue
        result[label] = {
            "avg_udc": int(m_df["udc"].mean()),
            "max_udc": int(m_df["udc"].max()),
            "min_udc": int(m_df["udc"].min()),
            "std_udc": int(m_df["udc"].std()),
            "total_records": int(m_df["total_records"].sum()),
            "days": len(m_df),
            "peak_date": m_df.loc[m_df["udc"].idxmax(), "date"],
        }

    # 변화율
    if result.get("jan", {}).get("avg_udc", 0) > 0:
        result["change_pct"] = round(
            (result["feb"]["avg_udc"] - result["jan"]["avg_udc"]) / result["jan"]["avg_udc"] * 100, 1
        )
    else:
        result["change_pct"] = 0

    return result


def compute_gateway_stats(gw_daily_df: pd.DataFrame) -> pd.DataFrame:
    """게이트웨이별 전체 기간 통계.

    Returns:
        DataFrame: gateway_no, total_records, avg_dc, avg_rssi, pct_of_total
    """
    stats = gw_daily_df.groupby("gateway_no").agg(
        total_records=("total_records", "sum"),
        avg_dc=("dc", "mean"),
        avg_rssi=("avg_rssi", "mean"),
    ).reset_index()

    total = stats["total_records"].sum()
    stats["pct_of_total"] = (stats["total_records"] / total * 100).round(1) if total > 0 else 0
    stats = stats.sort_values("total_records", ascending=False)
    return stats


def compute_hourly_profile(hourly_df: pd.DataFrame) -> pd.DataFrame:
    """시간대별 평균 프로파일 (전체/평일/주말).

    Returns:
        DataFrame: hour, avg_dc, weekday_dc, weekend_dc, period_name
    """
    df = hourly_df.copy()
    df["day_type"] = df["date"].apply(get_day_type)
    df["period_name"] = df["hour"].apply(get_time_period)

    avg_all = df.groupby("hour")["dc"].mean().rename("avg_dc")
    avg_wd = df[df["day_type"] == "평일"].groupby("hour")["dc"].mean().rename("weekday_dc")
    avg_we = df[df["day_type"].isin(["주말", "공휴일"])].groupby("hour")["dc"].mean().rename("weekend_dc")

    result = pd.concat([avg_all, avg_wd, avg_we], axis=1).fillna(0).reset_index()
    result["period_name"] = result["hour"].apply(get_time_period)
    return result


# ── MAC 랜덤화 보정 상수 ──────────────────────────────────────────────────
# iPhone: 평균 15-20분마다 MAC 변경
# Android: 평균 3-5분마다 MAC 변경
IOS_MAC_PERIOD_MIN = 17.5  # 15~20분 중간값
ANDROID_MAC_PERIOD_MIN = 4.0  # 3~5분 중간값

# 5분 윈도우 내 예상 MAC 생성 수 (1인당)
# iPhone: 5분 / 17.5분 = ~0.29 → 대부분 1개 MAC (가끔 2개)
IOS_MACS_PER_PERSON_5MIN = 1.15  # ~15% 확률로 윈도우 중간에 변경
# Android: 5분 / 4분 = ~1.25 → 대부분 1-2개 MAC
ANDROID_MACS_PER_PERSON_5MIN = 1.5


def estimate_exit_headcount(
    fine_df: pd.DataFrame,
    exit_start_hour: int = 17,
    exit_start_min: int = 0,
    exit_end_hour: int = 18,
    exit_end_min: int = 30,
) -> pd.DataFrame:
    """퇴근 시간대 5분 윈도우 기반 인원 추정.

    핵심 원리:
    - 타각기 → 버스로 바로 이동 → MAC이 빠르게 사라짐
    - 5분 윈도우 내에서 unique MAC ≈ 실제 인원 (보정계수 적용)
    - 각 5분 윈도우의 추정 인원을 합산 → 총 퇴근 인원

    Returns:
        DataFrame: date, time_bin, dc, ios_dc, android_dc,
                   est_ios, est_android, est_total
    """
    exit_start_bin = exit_start_hour * 60 + exit_start_min
    exit_end_bin = exit_end_hour * 60 + exit_end_min

    exit_df = fine_df[
        (fine_df["time_bin"] >= exit_start_bin) &
        (fine_df["time_bin"] < exit_end_bin)
    ].copy()

    if exit_df.empty:
        return pd.DataFrame()

    # MAC 랜덤화 보정: unique MAC / (1인당 예상 MAC 수)
    exit_df["est_ios"] = (exit_df["ios_dc"] / IOS_MACS_PER_PERSON_5MIN).round(0).astype(int)
    exit_df["est_android"] = (exit_df["android_dc"] / ANDROID_MACS_PER_PERSON_5MIN).round(0).astype(int)
    exit_df["est_total"] = exit_df["est_ios"] + exit_df["est_android"]

    return exit_df


def compute_daily_exit_headcount(fine_df: pd.DataFrame, **kwargs) -> pd.DataFrame:
    """일별 퇴근 인원 추정.

    각 날짜의 퇴근 시간대 5분 윈도우 합산.

    Returns:
        DataFrame: date, exit_dc, est_headcount, ios_est, android_est, peak_5min_dc, peak_time
    """
    exit_df = estimate_exit_headcount(fine_df, **kwargs)
    if exit_df.empty:
        return pd.DataFrame()

    daily = exit_df.groupby("date").agg(
        exit_dc=("dc", "sum"),
        est_headcount=("est_total", "sum"),
        ios_est=("est_ios", "sum"),
        android_est=("est_android", "sum"),
        peak_5min_dc=("dc", "max"),
    ).reset_index()

    # 피크 시간 (DC가 가장 높은 5분 윈도우)
    peak_bins = exit_df.loc[exit_df.groupby("date")["dc"].idxmax()][["date", "time_bin"]].copy()
    peak_bins["peak_time"] = peak_bins["time_bin"].apply(
        lambda x: f"{x // 60:02d}:{x % 60:02d}"
    )
    daily = daily.merge(peak_bins[["date", "peak_time"]], on="date", how="left")

    return daily.sort_values("date")


def compute_entry_headcount(fine_df: pd.DataFrame) -> pd.DataFrame:
    """출근 시간대 5분 윈도우 기반 인원 추정 (04:00~08:00).

    Returns:
        DataFrame: date, entry_dc, est_headcount, peak_time
    """
    return _compute_period_headcount(fine_df, 4, 0, 8, 0, prefix="entry")


def _compute_period_headcount(
    fine_df: pd.DataFrame,
    start_h: int, start_m: int, end_h: int, end_m: int,
    prefix: str = "period",
) -> pd.DataFrame:
    """특정 시간대 인원 추정 (공통 로직)."""
    exit_df = estimate_exit_headcount(fine_df, start_h, start_m, end_h, end_m)
    if exit_df.empty:
        return pd.DataFrame()

    daily = exit_df.groupby("date").agg(
        dc_sum=("dc", "sum"),
        est_headcount=("est_total", "sum"),
        peak_5min_dc=("dc", "max"),
    ).reset_index()

    peak_bins = exit_df.loc[exit_df.groupby("date")["dc"].idxmax()][["date", "time_bin"]].copy()
    peak_bins["peak_time"] = peak_bins["time_bin"].apply(
        lambda x: f"{x // 60:02d}:{x % 60:02d}"
    )
    daily = daily.merge(peak_bins[["date", "peak_time"]], on="date", how="left")
    daily = daily.rename(columns={
        "dc_sum": f"{prefix}_dc",
        "est_headcount": f"{prefix}_est",
    })
    return daily.sort_values("date")


def compute_daily_commute_times(fine_df: pd.DataFrame, dc_threshold: int = 30) -> pd.DataFrame:
    """날짜별 출퇴근 시간 분석.

    5분 단위 데이터에서 출근 시작/피크, 퇴근 피크/종료를 식별한다.
    dc_threshold 이상인 구간을 '활성 시간'으로 정의.

    Returns:
        DataFrame: date, entry_start, entry_peak, exit_peak, exit_end,
                   peak_entry_dc, peak_exit_dc, active_hours
    """
    results = []
    for date, day_df in fine_df.groupby("date"):
        if day_df["dc"].max() < dc_threshold:
            continue

        active = day_df[day_df["dc"] >= dc_threshold].sort_values("time_bin")
        if active.empty:
            continue

        # 출근: 04:00~10:00 구간
        morning = active[(active["time_bin"] >= 240) & (active["time_bin"] < 600)]
        # 퇴근: 15:00~21:00 구간
        evening = active[(active["time_bin"] >= 900) & (active["time_bin"] < 1260)]

        entry_start = _bin_to_time(int(morning["time_bin"].min())) if not morning.empty else ""
        entry_peak_bin = morning.loc[morning["dc"].idxmax(), "time_bin"] if not morning.empty else 0
        entry_peak = _bin_to_time(int(entry_peak_bin)) if not morning.empty else ""
        peak_entry_dc = int(morning["dc"].max()) if not morning.empty else 0

        exit_peak_bin = evening.loc[evening["dc"].idxmax(), "time_bin"] if not evening.empty else 0
        exit_peak = _bin_to_time(int(exit_peak_bin)) if not evening.empty else ""
        exit_end = _bin_to_time(int(evening["time_bin"].max())) if not evening.empty else ""
        peak_exit_dc = int(evening["dc"].max()) if not evening.empty else 0

        # 활성 시간 (시간)
        active_bins = len(active)
        active_hours = round(active_bins * 5 / 60, 1)

        results.append({
            "date": date,
            "entry_start": entry_start,
            "entry_peak": entry_peak,
            "peak_entry_dc": peak_entry_dc,
            "exit_peak": exit_peak,
            "exit_end": exit_end,
            "peak_exit_dc": peak_exit_dc,
            "active_hours": active_hours,
        })

    return pd.DataFrame(results).sort_values("date") if results else pd.DataFrame()


def _bin_to_time(time_bin: int) -> str:
    """5분 단위 bin -> HH:MM 문자열."""
    return f"{time_bin // 60:02d}:{time_bin % 60:02d}"


# ── 출근 흐름 분석 ────────────────────────────────────────────────────────


def analyze_entry_flow(
    fine_df: pd.DataFrame,
    gate_flow_df: pd.DataFrame,
    date: str,
    entry_start_hour: int = 3,
    entry_end_hour: int = 9,
    rush_ratio: float = 0.2,
) -> dict | None:
    """출근 흐름 분석 — 혼잡도(넓은) + 처리량(좁은) 결합.

    - 혼잡도: fine_df (넓은 RSSI) → 타각기 근처 인원수, 피크
    - 처리량: gate_flow_df (좁은 RSSI, MAC 추적) → 분당 실제 통과 인원

    Returns:
        dict: 러시 구간, 피크 혼잡도, 분당 유입/유출 속도
    """
    # ── 혼잡도 (넓은 범위) ──
    day = fine_df[fine_df["date"] == date].sort_values("time_bin")
    start_bin = entry_start_hour * 60
    end_bin = entry_end_hour * 60
    morning = day[(day["time_bin"] >= start_bin) & (day["time_bin"] <= end_bin)]

    if morning.empty or morning["dc"].max() < 10:
        return None

    dc_series = morning.set_index("time_bin")["dc"]

    # 피크 혼잡도
    peak_bin = int(dc_series.idxmax())
    peak_crowd = int(dc_series.max())

    # 러시 = 피크의 N% 이상
    rush_threshold = peak_crowd * rush_ratio
    rush_bins = dc_series[dc_series >= rush_threshold]
    if not rush_bins.empty:
        rush_start = int(rush_bins.index.min())
        rush_end = int(rush_bins.index.max())
        rush_duration = rush_end - rush_start + 5
    else:
        rush_start = peak_bin
        rush_end = peak_bin
        rush_duration = 0

    # ── 처리량 (좁은 범위, MAC 추적) ──
    gf_day = gate_flow_df[gate_flow_df["date"] == date].sort_values("time_bin") if not gate_flow_df.empty else pd.DataFrame()
    gf_morning = gf_day[(gf_day["time_bin"] >= start_bin) & (gf_day["time_bin"] <= end_bin)] if not gf_day.empty else pd.DataFrame()

    if not gf_morning.empty:
        gf_rush = gf_morning[(gf_morning["time_bin"] >= rush_start) & (gf_morning["time_bin"] <= rush_end)]
        avg_throughput = round(float(gf_rush["flow_per_min"].mean()), 1) if not gf_rush.empty else 0
        peak_throughput = round(float(gf_rush["flow_per_min"].max()), 1) if not gf_rush.empty else 0
        avg_inflow = round(float(gf_rush["inflow_per_min"].mean()), 1) if not gf_rush.empty else 0
        peak_inflow = round(float(gf_rush["inflow_per_min"].max()), 1) if not gf_rush.empty else 0
    else:
        avg_throughput = peak_throughput = avg_inflow = peak_inflow = 0

    return {
        "peak_time": _bin_to_time(peak_bin),
        "peak_bin": peak_bin,
        "peak_crowd": peak_crowd,
        "rush_start": _bin_to_time(rush_start),
        "rush_start_bin": rush_start,
        "rush_end": _bin_to_time(rush_end),
        "rush_end_bin": rush_end,
        "rush_duration": rush_duration,
        "avg_throughput": avg_throughput,
        "peak_throughput": peak_throughput,
        "avg_inflow": avg_inflow,
        "peak_inflow": peak_inflow,
    }


def compute_all_entry_flows(fine_df: pd.DataFrame) -> pd.DataFrame:
    """전체 기간 출근 흐름 요약."""
    DAY_KR = {0: "월", 1: "화", 2: "수", 3: "목", 4: "금", 5: "토", 6: "일"}
    rows = []
    for d in sorted(fine_df["date"].unique()):
        result = analyze_entry_flow(fine_df, d)
        if result is None:
            continue
        dt = pd.to_datetime(d)
        rows.append({
            "date": d,
            "day_name_kr": DAY_KR.get(dt.dayofweek, ""),
            "day_type": get_day_type(d),
            **result,
        })
    return pd.DataFrame(rows) if rows else pd.DataFrame()


# ── 퇴근 대기 시간 추정 ──────────────────────────────────────────────────

# 건설현장 퇴근 게이트 오픈 시간 (현장 운영 규칙)
GATE_OPEN_TIMES = [
    (17, 30),  # 1차 퇴근
    (19, 30),  # 2차 퇴근
]


def _compute_baseline(dc_series: pd.Series) -> float:
    """배경 트래픽 베이스라인 산출 (15:00~16:00 중앙값).

    15시대가 가장 안정적 (점심 잔류 해소, 퇴근 모임 전).
    타각기 주변을 지나다니는 사람들의 신호로, 출퇴근과 무관한 배경 수준.
    """
    bg = dc_series[(dc_series.index >= 15 * 60) & (dc_series.index < 16 * 60)]
    if bg.empty:
        return 0
    return float(bg.median())


def detect_gate_openings(
    fine_df: pd.DataFrame,
    date: str,
    gate_flow_df: pd.DataFrame | None = None,
    gate_times: list[tuple[int, int]] | None = None,
) -> list[dict]:
    """고정 게이트 오픈 시간 기반 이벤트 분석.

    혼잡도(넓은 RSSI) + 처리량(좁은 RSSI, MAC 추적) 결합.

    Returns:
        list of dict per gate event
    """
    if gate_times is None:
        gate_times = GATE_OPEN_TIMES

    day = fine_df[fine_df["date"] == date].sort_values("time_bin")
    if day.empty or day["dc"].max() < 30:
        return []

    dc_series = day.set_index("time_bin")["dc"]
    baseline = _compute_baseline(dc_series)

    # gate_flow (좁은 RSSI) 준비
    gf_day = None
    if gate_flow_df is not None and not gate_flow_df.empty:
        gf_day = gate_flow_df[gate_flow_df["date"] == date].set_index("time_bin")

    events = []
    for h, m in gate_times:
        gate_bin = h * 60 + m

        # 오픈 시점 ±10분 내 실제 피크 DC 찾기
        nearby_bins = [b for b in dc_series.index if abs(b - gate_bin) <= 10]
        if not nearby_bins:
            continue

        actual_peak_bin = max(nearby_bins, key=lambda b: dc_series[b])
        raw_peak_dc = int(dc_series[actual_peak_bin])
        net_peak_dc = max(0, raw_peak_dc - int(baseline))

        if net_peak_dc < 50:  # 배경 차감 후에도 유의미해야
            continue

        # 모임 시작: 배경 대비 DC가 의미있게 올라가기 시작한 시점
        search_start = max(0, gate_bin - 90)
        gathering_start = _find_gathering_start(dc_series, actual_peak_bin, search_start, baseline)

        # 빠져나감 완료: 오픈 후 DC가 배경 수준으로 복귀
        search_end = gate_bin + 60
        drain_start_bin = gate_bin + 5
        clear_bin = _find_clear_end(dc_series, drain_start_bin, search_end, baseline)

        drain_minutes = max(5, (clear_bin - gate_bin) // 5 * 5)

        # 유출 속도: 오픈~완료 구간의 5분 단위 DC 감소량에서 산출
        drain_bins = sorted(b for b in dc_series.index if gate_bin <= b <= clear_bin)
        total_outflow = 0
        peak_outflow_per_min = 0.0
        for j in range(1, len(drain_bins)):
            prev_net = max(0, dc_series[drain_bins[j-1]] - baseline)
            curr_net = max(0, dc_series[drain_bins[j]] - baseline)
            outflow = max(0, prev_net - curr_net)  # 감소분 = 빠져나간 인원
            total_outflow += outflow
            per_min = outflow / 5
            if per_min > peak_outflow_per_min:
                peak_outflow_per_min = per_min

        avg_drain_per_min = round(total_outflow / drain_minutes, 1) if drain_minutes > 0 else 0
        peak_drain_per_min = round(peak_outflow_per_min, 1)

        # gate_flow 기반 실제 유출 속도 (좁은 RSSI, MAC 추적)
        gf_avg_outflow = 0.0
        gf_peak_outflow = 0.0
        if gf_day is not None:
            gf_drain = gf_day[(gf_day.index >= gate_bin) & (gf_day.index <= clear_bin)]
            if not gf_drain.empty:
                gf_avg_outflow = round(float(gf_drain["outflow_per_min"].mean()), 1)
                gf_peak_outflow = round(float(gf_drain["outflow_per_min"].max()), 1)

        events.append({
            "gate_open_bin": gate_bin,
            "gate_open_time": _bin_to_time(gate_bin),
            "peak_dc": net_peak_dc,
            "raw_peak_dc": raw_peak_dc,
            "baseline_dc": int(baseline),
            "gathering_start_bin": int(gathering_start),
            "gathering_start_time": _bin_to_time(int(gathering_start)),
            "clear_bin": int(clear_bin),
            "clear_time": _bin_to_time(int(clear_bin)),
            "drain_minutes": drain_minutes,
            "avg_drain_per_min": avg_drain_per_min,
            "peak_drain_per_min": peak_drain_per_min,
            "gf_avg_outflow": gf_avg_outflow,
            "gf_peak_outflow": gf_peak_outflow,
        })

    return events


def _find_gathering_start(
    dc_series: pd.Series, peak_bin: int, min_bin: int,
    baseline: float = 0, gather_ratio: float = 0.2,
) -> int:
    """모임 시작 = 배경 대비 순수 피크의 N% 인원이 추가로 관측되는 시점.

    예: baseline=60, peak=900, net_peak=840, ratio=0.2
    → threshold = 60 + 840 * 0.2 = 228
    → DC > 228이 되는 시점부터 "퇴근을 위해 모이기 시작"
    """
    bins = sorted(dc_series.index)
    peak_idx = bins.index(peak_bin) if peak_bin in bins else 0

    peak_dc = dc_series[peak_bin]
    net_peak = peak_dc - baseline
    threshold = baseline + net_peak * gather_ratio

    for i in range(peak_idx, -1, -1):
        b = bins[i]
        if b < min_bin:
            return bins[min(i + 1, peak_idx)] if i + 1 <= peak_idx else min_bin
        if dc_series[b] <= threshold:
            # threshold 이하 → 다음 빈(threshold를 넘는 첫 시점)이 모임 시작
            return bins[i + 1] if i + 1 <= peak_idx else b
    return min_bin


def _find_clear_end(
    dc_series: pd.Series, drop_bin: int, max_bin: int,
    baseline: float = 0, clear_ratio: float = 1.3,
) -> int:
    """퇴근 완료 = DC가 배경 수준에 근접하게 내려온 시점.

    배경 × clear_ratio 이하로 내려오면 퇴근 완료로 판정.
    (0이 될 수 없으므로 배경 근접을 기준으로 함)
    """
    bins = sorted(dc_series.index)
    drop_idx = bins.index(drop_bin) if drop_bin in bins else len(bins) - 1
    threshold = baseline * clear_ratio if baseline > 0 else 50

    for i in range(drop_idx, len(bins)):
        b = bins[i]
        if b > max_bin:
            return max_bin
        if dc_series[b] <= threshold:
            return b
    return min(drop_bin + 30, max_bin)


def estimate_wait_time_distribution(
    fine_df: pd.DataFrame,
    date: str,
    gate_flow_df: pd.DataFrame | None = None,
) -> list[dict]:
    """트래픽 기반 대기 시간 분포 추정 + 게이트 유출 속도.

    Returns:
        list of dict (per gate event):
            - event: dict (gate_open_time, peak_dc, drain, flow rates)
            - distribution: list of (wait_minutes, estimated_people)
            - stats: dict (median, p75, p90, p95, max, total_people)
    """
    events = detect_gate_openings(fine_df, date, gate_flow_df)
    if not events:
        return []

    day = fine_df[fine_df["date"] == date].sort_values("time_bin")
    dc_series = day.set_index("time_bin")["dc"]
    baseline = _compute_baseline(dc_series)

    results = []
    for evt in events:
        gate_bin = evt["gate_open_bin"]
        gather_bin = evt["gathering_start_bin"]

        # 모임 구간의 각 5분 빈에서 신규 도착 추정 (배경 차감)
        distribution = []
        bins_in_range = sorted(b for b in dc_series.index if gather_bin <= b <= gate_bin)

        prev_net_dc = 0
        for b in bins_in_range:
            net_dc = max(0, dc_series[b] - baseline)  # 배경 차감
            new_arrivals = max(0, net_dc - prev_net_dc)
            wait_minutes = (gate_bin - b) // 5 * 5
            if new_arrivals > 0 and wait_minutes >= 0:
                distribution.append({
                    "arrival_time": _bin_to_time(b),
                    "arrival_bin": b,
                    "wait_minutes": wait_minutes,
                    "estimated_people": int(new_arrivals),
                })
            prev_net_dc = net_dc

        if not distribution:
            continue

        # 통계 (인원 가중)
        all_waits = []
        for d in distribution:
            all_waits.extend([d["wait_minutes"]] * d["estimated_people"])

        if all_waits:
            waits_arr = np.array(all_waits)
            stats = {
                "median": int(np.median(waits_arr)),
                "p75": int(np.percentile(waits_arr, 75)),
                "p90": int(np.percentile(waits_arr, 90)),
                "p95": int(np.percentile(waits_arr, 95)),
                "max": int(np.max(waits_arr)),
                "total_people": len(all_waits),
            }
        else:
            stats = {"median": 0, "p75": 0, "p90": 0, "p95": 0, "max": 0, "total_people": 0}

        results.append({
            "event": evt,
            "distribution": distribution,
            "stats": stats,
        })

    return results


def compute_all_gate_events(fine_df: pd.DataFrame, gate_flow_df: pd.DataFrame | None = None) -> pd.DataFrame:
    """전체 기간 게이트 이벤트 ��약 테이블."""
    DAY_KR = {0: "월", 1: "화", 2: "수", 3: "목", 4: "금", 5: "토", 6: "일"}
    dates = sorted(fine_df["date"].unique())
    rows = []

    for d in dates:
        waits = estimate_wait_time_distribution(fine_df, d, gate_flow_df)
        dt = pd.to_datetime(d)
        day_type = get_day_type(d)

        for i, w in enumerate(waits):
            evt = w["event"]
            s = w["stats"]
            rows.append({
                "date": d,
                "dayofweek": dt.dayofweek,
                "day_name_kr": DAY_KR.get(dt.dayofweek, ""),
                "day_type": day_type,
                "event_num": i + 1,
                "gate_open": evt["gate_open_time"],
                "gathering_start": evt["gathering_start_time"],
                "peak_dc": evt["peak_dc"],
                "drain_minutes": evt.get("drain_minutes", 0),
                "gf_avg_outflow": evt.get("gf_avg_outflow", 0),
                "gf_peak_outflow": evt.get("gf_peak_outflow", 0),
                "wait_median": s["median"],
                "wait_p75": s["p75"],
                "wait_p90": s["p90"],
                "wait_p95": s["p95"],
                "wait_max": s["max"],
                "total_people": s["total_people"],
            })

    return pd.DataFrame(rows) if rows else pd.DataFrame()


def add_day_metadata(daily_df: pd.DataFrame) -> pd.DataFrame:
    """일별 데이터에 요일/유형/특이일 메타데이터 추가."""
    df = daily_df.copy()
    dt = pd.to_datetime(df["date"])
    df["dayofweek"] = dt.dt.dayofweek
    df["day_name_kr"] = dt.dt.day_name().map(
        {"Monday": "월", "Tuesday": "화", "Wednesday": "수", "Thursday": "목",
         "Friday": "금", "Saturday": "토", "Sunday": "일"}
    )
    df["day_type"] = df["date"].apply(get_day_type)
    df["special"] = df["date"].map(SPECIAL_DAYS).fillna("")
    return df
