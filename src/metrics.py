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


# ── 트래픽 기반 대기 시간 추정 ─────────────────────────────────────────────

def detect_gate_openings(
    fine_df: pd.DataFrame,
    date: str,
    search_start_hour: int = 16,
    search_end_hour: int = 21,
    drop_threshold: float = 0.3,
) -> list[dict]:
    """게이트 오픈 시점 자동 탐지.

    DC가 피크 대비 급락하는 시점을 게이트 오픈으로 식별한다.
    연속적인 오픈 이벤트(1차/2차 퇴근)를 각각 반환.

    Returns:
        list of dict: gate_open_bin, peak_bin, peak_dc, drop_dc, gathering_start_bin
    """
    day = fine_df[fine_df["date"] == date].sort_values("time_bin")
    search_start = search_start_hour * 60
    search_end = search_end_hour * 60
    evening = day[(day["time_bin"] >= search_start) & (day["time_bin"] <= search_end)]

    if evening.empty or evening["dc"].max() < 30:
        return []

    dc_series = evening.set_index("time_bin")["dc"]
    diff = dc_series.diff()

    # 피크 DC 기준 급락 탐지
    events = []
    used_bins = set()

    # 전체 피크 DC의 일정 비율 이상만 의미있는 이벤트로 취급
    overall_peak_dc = dc_series.max()
    min_peak_dc = max(200, overall_peak_dc * 0.25)

    # 큰 급락 순서로 이벤트 찾기
    drops = diff[diff < 0].sort_values()
    for drop_bin, drop_val in drops.items():
        # 이미 처리된 이벤트 근처 건너뛰기 (±30분)
        if any(abs(drop_bin - ub) < 30 for ub in used_bins):
            continue

        # 급락 직전이 피크 (게이트 오픈 직전 최고 밀집)
        peak_bin = drop_bin - 5  # 직전 5분 빈
        if peak_bin not in dc_series.index:
            continue

        peak_dc = dc_series[peak_bin]
        if peak_dc < min_peak_dc:  # 전체 피크 대비 유의미한 크기만
            continue

        # 급락 비율 확인
        drop_ratio = abs(drop_val) / peak_dc
        if drop_ratio < drop_threshold:
            continue

        # 모임 시작 탐지: 피크에서 역방향으로 DC가 지속 증가하기 시작한 시점
        gathering_start = _find_gathering_start(dc_series, peak_bin, search_start)

        events.append({
            "gate_open_bin": int(drop_bin),
            "gate_open_time": _bin_to_time(int(drop_bin)),
            "peak_bin": int(peak_bin),
            "peak_time": _bin_to_time(int(peak_bin)),
            "peak_dc": int(peak_dc),
            "drop_dc": int(abs(drop_val)),
            "gathering_start_bin": int(gathering_start),
            "gathering_start_time": _bin_to_time(int(gathering_start)),
        })
        used_bins.add(drop_bin)

        if len(events) >= 3:  # 최대 3회 퇴근
            break

    return sorted(events, key=lambda x: x["gate_open_bin"])


def _find_gathering_start(dc_series: pd.Series, peak_bin: int, min_bin: int) -> int:
    """피크에서 역방향으로 DC 축적 시작 시점 탐지.

    DC가 지속적으로 감소하기 시작하는 시점 (= 모임 시작 직전).
    """
    bins = sorted(dc_series.index)
    peak_idx = bins.index(peak_bin) if peak_bin in bins else 0

    # 피크에서 역방향 탐색
    baseline_dc = dc_series[peak_bin] * 0.15  # 피크의 15%를 기준
    for i in range(peak_idx, -1, -1):
        b = bins[i]
        if b < min_bin:
            return min_bin
        if dc_series[b] <= baseline_dc:
            return b
    return min_bin


def estimate_wait_time_distribution(
    fine_df: pd.DataFrame,
    date: str,
) -> list[dict]:
    """트래픽 기반 대기 시간 분포 추정.

    게이트 오픈 시점을 탐지하고, 각 5분 빈의 신규 도착 인원에
    대기 시간(= 게이트 오픈 시각 - 도착 시각)을 부여한다.

    Returns:
        list of dict (per gate event):
            - event: dict (gate_open_time, peak_dc, ...)
            - distribution: list of (wait_minutes, estimated_people)
            - stats: dict (median, p75, p90, p95, max, total_people)
    """
    events = detect_gate_openings(fine_df, date)
    if not events:
        return []

    day = fine_df[fine_df["date"] == date].sort_values("time_bin")
    dc_series = day.set_index("time_bin")["dc"]

    results = []
    for evt in events:
        gate_bin = evt["gate_open_bin"]
        gather_bin = evt["gathering_start_bin"]

        # 모임 구간의 각 5분 빈에서 신규 도착 추정
        distribution = []
        bins_in_range = sorted(b for b in dc_series.index if gather_bin <= b <= gate_bin)

        prev_dc = 0
        for b in bins_in_range:
            current_dc = dc_series[b]
            # 신규 도착 = DC 증가분 (양수만, 감소는 이탈)
            new_arrivals = max(0, current_dc - prev_dc)
            wait_minutes = (gate_bin - b) // 5 * 5  # 5분 단위
            if new_arrivals > 0 and wait_minutes >= 0:
                distribution.append({
                    "arrival_time": _bin_to_time(b),
                    "arrival_bin": b,
                    "wait_minutes": wait_minutes,
                    "estimated_people": int(new_arrivals),
                })
            prev_dc = current_dc

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
