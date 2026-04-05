"""Entrance_Analysis_Y1 -- Plotly 차트 생성 모듈.

모든 차트 함수는 go.Figure를 반환한다.
"""
from __future__ import annotations

import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

# ── 공통 테마 ────────────────────────────────────────────────────────────────

COLORS = {
    "primary": "#4A90D9",
    "secondary": "#F5A623",
    "ios": "#4A90D9",
    "android": "#50C878",
    "weekday": "#4A90D9",
    "weekend": "#F5A623",
    "holiday": "#E85D75",
    "peak_am": "#FF8C42",
    "peak_pm": "#6C5CE7",
    "positive": "#50C878",
    "negative": "#E85D75",
}

BG_COLOR = "#0E1117"
GRID_COLOR = "#1E2130"

CHART_LAYOUT = dict(
    template="plotly_dark",
    paper_bgcolor=BG_COLOR,
    plot_bgcolor=BG_COLOR,
    font=dict(family="sans-serif", color="#ccd6f6", size=12),
    margin=dict(l=50, r=20, t=50, b=50),
    height=400,
)

# 게이트웨이 고유 색상
GW_COLORS = {
    132: "#4A90D9", 133: "#50C878", 148: "#F5A623", 149: "#E85D75",
    227: "#6C5CE7", 232: "#FF6B6B", 233: "#00D2FF", 256: "#FFD93D",
}


def _base_layout(**kwargs) -> dict:
    """기본 레이아웃 + 오버라이드."""
    layout = {**CHART_LAYOUT}
    layout.update(kwargs)
    return layout


# ── 개요 탭 차트 ─────────────────────────────────────────────────────────────

def create_daily_udc_chart(daily_df: pd.DataFrame) -> go.Figure:
    """일별 UDC 추이 바 차트 (평일/주말/공휴일 색상 구분)."""
    df = daily_df.copy()

    color_map = {"평일": COLORS["weekday"], "주말": COLORS["weekend"], "공휴일": COLORS["holiday"]}

    fig = go.Figure()
    for day_type, color in color_map.items():
        mask = df["day_type"] == day_type
        sub = df[mask]
        if sub.empty:
            continue
        fig.add_trace(go.Bar(
            x=sub["date"],
            y=sub["udc"],
            name=day_type,
            marker_color=color,
            opacity=0.85,
            hovertemplate="%{x}<br>UDC: %{y:,.0f}<extra>" + day_type + "</extra>",
        ))

    fig.update_layout(
        **_base_layout(
            title="일별 감지 디바이스 수 (UDC)",
            xaxis_title="날짜",
            yaxis_title="Unique Device Count",
            barmode="stack",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
    )
    return fig


def create_hourly_profile_chart(profile_df: pd.DataFrame) -> go.Figure:
    """시간대별 평균 DC 프로파일 (전체/평일/주말 + 시간대 배경)."""
    fig = go.Figure()

    # 시간대 배경 밴드 (건설현장 실제 패턴)
    bands = [
        (0, 4, "rgba(100,100,100,0.1)", "심야"),
        (4, 7, "rgba(255,140,66,0.15)", "출근"),
        (7, 12, "rgba(74,144,217,0.1)", "오전"),
        (12, 13, "rgba(80,200,120,0.1)", "점심"),
        (13, 17, "rgba(74,144,217,0.1)", "오후"),
        (17, 20, "rgba(108,92,231,0.15)", "퇴근"),
        (20, 24, "rgba(100,100,100,0.1)", "야간"),
    ]
    for x0, x1, color, label in bands:
        fig.add_vrect(x0=x0 - 0.5, x1=x1 - 0.5, fillcolor=color, line_width=0,
                      annotation_text=label, annotation_position="top left",
                      annotation_font_size=9, annotation_font_color="#5a6785")

    fig.add_trace(go.Scatter(
        x=profile_df["hour"], y=profile_df["weekday_dc"],
        mode="lines+markers", name="평일",
        line=dict(color=COLORS["weekday"], width=2),
        marker=dict(size=5),
    ))
    fig.add_trace(go.Scatter(
        x=profile_df["hour"], y=profile_df["weekend_dc"],
        mode="lines+markers", name="주말/공휴일",
        line=dict(color=COLORS["weekend"], width=2, dash="dash"),
        marker=dict(size=5),
    ))

    fig.update_layout(
        **_base_layout(
            title="시간대별 평균 DC (평일 vs 주말)",
            xaxis_title="시간",
            yaxis_title="평균 Device Count",
            xaxis=dict(dtick=1, range=[-0.5, 23.5]),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
    )
    return fig


def create_device_ratio_chart(daily_df: pd.DataFrame) -> go.Figure:
    """iOS vs Android 일별 추이 스택 바."""
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=daily_df["date"], y=daily_df["ios_udc"],
        name="iOS", marker_color=COLORS["ios"], opacity=0.85,
    ))
    fig.add_trace(go.Bar(
        x=daily_df["date"], y=daily_df["android_udc"],
        name="Android", marker_color=COLORS["android"], opacity=0.85,
    ))
    fig.update_layout(
        **_base_layout(
            title="iOS vs Android 일별 디바이스 수",
            xaxis_title="날짜",
            yaxis_title="Unique Device Count",
            barmode="stack",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
    )
    return fig


# ── 시간 패턴 탭 차트 ────────────────────────────────────────────────────────

def create_heatmap_chart(hourly_df: pd.DataFrame) -> go.Figure:
    """일 x 시간 DC 히트맵."""
    pivot = hourly_df.pivot_table(index="date", columns="hour", values="dc", aggfunc="sum", fill_value=0)
    pivot = pivot.sort_index()

    fig = go.Figure(go.Heatmap(
        z=pivot.values,
        x=[f"{h:02d}:00" for h in pivot.columns],
        y=pivot.index,
        colorscale="YlOrRd",
        colorbar=dict(title="DC"),
        hovertemplate="날짜: %{y}<br>시간: %{x}<br>DC: %{z:,.0f}<extra></extra>",
    ))
    fig.update_layout(
        **_base_layout(
            title="시간대별 트래픽 히트맵 (일 x 시간)",
            xaxis_title="시간",
            yaxis_title="날짜",
            height=max(500, len(pivot) * 10),
        )
    )
    fig.update_yaxes(autorange="reversed")
    return fig


def create_half_hourly_profile(hourly_df: pd.DataFrame) -> go.Figure:
    """30분 단위 DC 프로파일 (평일/주말)."""
    from .metrics import get_day_type

    df = hourly_df.copy()
    df["day_type"] = df["date"].apply(get_day_type)

    # 시간별 평균 (30분 bin 없으므로 1시간 사용)
    wd = df[df["day_type"] == "평일"].groupby("hour")["dc"].mean()
    we = df[df["day_type"].isin(["주말", "공휴일"])].groupby("hour")["dc"].mean()

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=wd.index, y=wd.values,
        mode="lines+markers", name="평일 평균",
        line=dict(color=COLORS["weekday"], width=2.5),
        fill="tozeroy", fillcolor="rgba(74,144,217,0.15)",
    ))
    fig.add_trace(go.Scatter(
        x=we.index, y=we.values,
        mode="lines+markers", name="주말/공휴일 평균",
        line=dict(color=COLORS["weekend"], width=2, dash="dash"),
    ))
    fig.update_layout(
        **_base_layout(
            title="시간별 DC 프로파일 (평일 vs 주말)",
            xaxis_title="시간",
            yaxis_title="평균 DC",
            xaxis=dict(dtick=1, range=[-0.5, 23.5]),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
    )
    return fig


def create_period_stats_table(hourly_df: pd.DataFrame) -> pd.DataFrame:
    """시간대별 통계 테이블 (표시용 DataFrame 반환)."""
    from .metrics import get_time_period

    df = hourly_df.copy()
    df["period"] = df["hour"].apply(get_time_period)

    stats = df.groupby("period").agg(
        avg_dc=("dc", "mean"),
        max_dc=("dc", "max"),
        total_records=("total_records", "sum"),
    ).round(0).astype(int)

    period_order = ["야간/새벽", "출근", "오전작업", "점심", "오후작업", "퇴근", "야간"]
    stats = stats.reindex([p for p in period_order if p in stats.index])
    stats.columns = ["평균 DC", "최대 DC", "총 레코드"]
    return stats


# ── 트렌드 탭 차트 ───────────────────────────────────────────────────────────

def create_weekday_boxplot(daily_df: pd.DataFrame) -> go.Figure:
    """요일별 UDC 박스플롯."""
    df = daily_df.copy()
    df["dayofweek"] = pd.to_datetime(df["date"]).dt.dayofweek
    day_labels = ["월", "화", "수", "목", "금", "토", "일"]

    fig = go.Figure()
    for i, label in enumerate(day_labels):
        vals = df[df["dayofweek"] == i]["udc"]
        color = COLORS["weekend"] if i >= 5 else COLORS["weekday"]
        fig.add_trace(go.Box(
            y=vals, name=label,
            marker_color=color,
            boxmean=True,
        ))

    fig.update_layout(
        **_base_layout(
            title="요일별 UDC 분포",
            yaxis_title="Unique Device Count",
            showlegend=False,
        )
    )
    return fig


def create_weekly_trend_chart(weekly_df: pd.DataFrame) -> go.Figure:
    """주차별 UDC 추이."""
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=weekly_df["week_label"],
        y=weekly_df["mean_udc"],
        marker_color=COLORS["primary"],
        opacity=0.8,
        name="주 평균 UDC",
        hovertemplate="주차: %{x}<br>평균 UDC: %{y:,.0f}<extra></extra>",
    ))

    # 추세선
    x_num = np.arange(len(weekly_df))
    if len(x_num) > 1:
        z = np.polyfit(x_num, weekly_df["mean_udc"].values, 1)
        p = np.poly1d(z)
        fig.add_trace(go.Scatter(
            x=weekly_df["week_label"], y=p(x_num),
            mode="lines", name="추세선",
            line=dict(color=COLORS["secondary"], width=2, dash="dash"),
        ))

    fig.update_layout(
        **_base_layout(
            title="주차별 평균 UDC 추이",
            xaxis_title="주차",
            yaxis_title="평균 UDC",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
    )
    return fig


def create_monthly_comparison_chart(monthly_data: dict) -> go.Figure:
    """1월 vs 2월 비교 바 차트."""
    metrics = ["avg_udc", "max_udc"]
    labels = ["평균 UDC", "최대 UDC"]

    jan = monthly_data.get("jan", {})
    feb = monthly_data.get("feb", {})

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=labels,
        y=[jan.get(m, 0) for m in metrics],
        name="1월",
        marker_color=COLORS["primary"],
    ))
    fig.add_trace(go.Bar(
        x=labels,
        y=[feb.get(m, 0) for m in metrics],
        name="2월",
        marker_color=COLORS["secondary"],
    ))
    fig.update_layout(
        **_base_layout(
            title="월별 비교 (1월 vs 2월)",
            yaxis_title="Unique Device Count",
            barmode="group",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
    )
    return fig


# ── 게이트웨이 탭 차트 ──────────────────────────────────────────────────────

def create_gateway_donut(gw_stats: pd.DataFrame) -> go.Figure:
    """게이트웨이별 트래픽 비율 도넛."""
    colors = [GW_COLORS.get(gw, "#888") for gw in gw_stats["gateway_no"]]

    fig = go.Figure(go.Pie(
        labels=gw_stats["gateway_no"].astype(str),
        values=gw_stats["total_records"],
        hole=0.45,
        marker=dict(colors=colors),
        textinfo="label+percent",
        hovertemplate="GW %{label}<br>레코드: %{value:,.0f}<br>비율: %{percent}<extra></extra>",
    ))
    fig.update_layout(
        **_base_layout(
            title="게이트웨이별 트래픽 비율",
            height=380,
        )
    )
    return fig


def create_gateway_bars(gw_stats: pd.DataFrame) -> go.Figure:
    """게이트웨이별 일평균 DC 수평 바."""
    gw_sorted = gw_stats.sort_values("avg_dc")
    colors = [GW_COLORS.get(gw, "#888") for gw in gw_sorted["gateway_no"]]

    fig = go.Figure(go.Bar(
        x=gw_sorted["avg_dc"],
        y=gw_sorted["gateway_no"].astype(str),
        orientation="h",
        marker_color=colors,
        hovertemplate="GW %{y}<br>일평균 DC: %{x:,.0f}<extra></extra>",
    ))
    fig.update_layout(
        **_base_layout(
            title="게이트웨이별 일평균 DC",
            xaxis_title="평균 Device Count",
            yaxis_title="Gateway No.",
        )
    )
    return fig


def create_gateway_timeline(gw_hourly_df: pd.DataFrame) -> go.Figure:
    """게이트웨이별 시간 프로파일 라인 차트."""
    gw_avg = gw_hourly_df.groupby(["hour", "gateway_no"])["dc"].mean().reset_index()

    fig = go.Figure()
    for gw in sorted(gw_avg["gateway_no"].unique()):
        sub = gw_avg[gw_avg["gateway_no"] == gw]
        fig.add_trace(go.Scatter(
            x=sub["hour"], y=sub["dc"],
            mode="lines+markers",
            name=f"GW {gw}",
            line=dict(color=GW_COLORS.get(gw, "#888"), width=2),
            marker=dict(size=4),
        ))

    fig.update_layout(
        **_base_layout(
            title="게이트웨이별 시간대 프로파일",
            xaxis_title="시간",
            yaxis_title="평균 DC",
            xaxis=dict(dtick=2, range=[-0.5, 23.5]),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
    )
    return fig


def create_gateway_rssi_violin(gw_hourly_df: pd.DataFrame) -> go.Figure:
    """게이트웨이별 RSSI 분포 박스플롯."""
    fig = go.Figure()
    for gw in sorted(gw_hourly_df["gateway_no"].unique()):
        sub = gw_hourly_df[gw_hourly_df["gateway_no"] == gw]
        fig.add_trace(go.Box(
            y=sub["avg_rssi"],
            name=f"GW {gw}",
            marker_color=GW_COLORS.get(gw, "#888"),
        ))

    fig.update_layout(
        **_base_layout(
            title="게이트웨이별 RSSI 분포",
            yaxis_title="평균 RSSI (dBm)",
            showlegend=False,
        )
    )
    return fig


# ── 인원 추정 탭 차트 ─────────────────────────────────────────────────────


def create_exit_flow_chart(exit_fine_df: pd.DataFrame, selected_date: str = None) -> go.Figure:
    """퇴근 시간대 5분 윈도우 DC + 추정 인원 비교 차트.

    Args:
        exit_fine_df: estimate_exit_headcount() 결과
        selected_date: 특정 날짜 필터 (None이면 전체 평균)
    """
    if selected_date:
        df = exit_fine_df[exit_fine_df["date"] == selected_date].copy()
        title_suffix = f" ({selected_date})"
    else:
        df = exit_fine_df.groupby("time_bin").agg(
            dc=("dc", "mean"),
            est_total=("est_total", "mean"),
            ios_dc=("ios_dc", "mean"),
            android_dc=("android_dc", "mean"),
        ).reset_index()
        title_suffix = " (전체 기간 평균)"

    if df.empty:
        return go.Figure().update_layout(**_base_layout(title="데이터 없음"))

    time_labels = [f"{int(t) // 60:02d}:{int(t) % 60:02d}" for t in df["time_bin"]]

    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=time_labels, y=df["dc"],
        name="Raw DC (MAC 수)",
        marker_color="rgba(74,144,217,0.4)",
        hovertemplate="%{x}<br>Raw DC: %{y:,.0f}<extra></extra>",
    ))

    fig.add_trace(go.Scatter(
        x=time_labels, y=df["est_total"],
        mode="lines+markers",
        name="추정 인원",
        line=dict(color=COLORS["secondary"], width=3),
        marker=dict(size=7),
        hovertemplate="%{x}<br>추정 인원: %{y:,.0f}<extra></extra>",
    ))

    fig.update_layout(
        **_base_layout(
            title=f"퇴근 시간대 5분 단위 트래픽{title_suffix}",
            xaxis_title="시간",
            yaxis_title="디바이스/인원 수",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            height=420,
        )
    )
    return fig


def create_daily_headcount_chart(daily_exit_df: pd.DataFrame) -> go.Figure:
    """일별 퇴근 추정 인원 추이 차트."""
    from .metrics import get_day_type

    df = daily_exit_df.copy()
    df["day_type"] = df["date"].apply(get_day_type)

    color_map = {"평일": COLORS["weekday"], "주말": COLORS["weekend"], "공휴일": COLORS["holiday"]}

    fig = go.Figure()
    for day_type, color in color_map.items():
        mask = df["day_type"] == day_type
        sub = df[mask]
        if sub.empty:
            continue
        fig.add_trace(go.Bar(
            x=sub["date"], y=sub["est_headcount"],
            name=day_type, marker_color=color, opacity=0.85,
            hovertemplate="%{x}<br>추정 인원: %{y:,.0f}<extra>" + day_type + "</extra>",
        ))

    # 추세선
    x_num = np.arange(len(df))
    if len(x_num) > 1:
        z = np.polyfit(x_num, df["est_headcount"].values, 1)
        p = np.poly1d(z)
        fig.add_trace(go.Scatter(
            x=df["date"], y=p(x_num),
            mode="lines", name="추세선",
            line=dict(color=COLORS["secondary"], width=2, dash="dash"),
        ))

    fig.update_layout(
        **_base_layout(
            title="일별 퇴근 추정 인원",
            xaxis_title="날짜",
            yaxis_title="추정 인원",
            barmode="stack",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
    )
    return fig


def create_headcount_comparison_chart(daily_exit_df: pd.DataFrame, daily_df: pd.DataFrame) -> go.Figure:
    """UDC vs 퇴근 추정 인원 비교 차트."""
    merged = daily_df[["date", "udc"]].merge(
        daily_exit_df[["date", "est_headcount"]], on="date", how="inner"
    )

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=merged["date"], y=merged["udc"],
        mode="lines", name="일 UDC (MAC 기반)",
        line=dict(color="rgba(74,144,217,0.5)", width=1.5),
        fill="tozeroy", fillcolor="rgba(74,144,217,0.08)",
    ))
    fig.add_trace(go.Scatter(
        x=merged["date"], y=merged["est_headcount"],
        mode="lines+markers", name="퇴근 추정 인원",
        line=dict(color=COLORS["secondary"], width=2.5),
        marker=dict(size=4),
    ))

    fig.update_layout(
        **_base_layout(
            title="UDC vs 퇴근 추정 인원 비교",
            xaxis_title="날짜",
            yaxis_title="수",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
    )
    return fig


def create_intraday_fine_chart(fine_df: pd.DataFrame, selected_date: str) -> go.Figure:
    """특정 날짜의 전일 5분 단위 트래픽 (통합)."""
    df = fine_df[fine_df["date"] == selected_date].copy()
    if df.empty:
        return go.Figure().update_layout(**_base_layout(title="데이터 없음"))

    time_labels = [f"{int(t) // 60:02d}:{int(t) % 60:02d}" for t in df["time_bin"]]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=time_labels, y=df["dc"],
        name="DC", marker_color=COLORS["primary"], opacity=0.85,
        hovertemplate="%{x}<br>DC: %{y:,.0f}<extra></extra>",
    ))

    fig.update_layout(
        **_base_layout(
            title=f"5분 단위 트래픽 — {selected_date}",
            xaxis_title="시간",
            yaxis_title="Unique Device Count",
            showlegend=False,
            height=380,
            xaxis=dict(dtick=12, tickangle=-45),
        )
    )
    return fig


def create_entry_exit_comparison(
    entry_df: pd.DataFrame, exit_df: pd.DataFrame
) -> go.Figure:
    """출근 vs 퇴근 추정 인원 비교."""
    merged = entry_df[["date", "entry_est"]].merge(
        exit_df[["date", "est_headcount"]], on="date", how="inner"
    )

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=merged["date"], y=merged["entry_est"],
        name="출근 추정", marker_color=COLORS["peak_am"], opacity=0.8,
    ))
    fig.add_trace(go.Bar(
        x=merged["date"], y=merged["est_headcount"],
        name="퇴근 추정", marker_color=COLORS["peak_pm"], opacity=0.8,
    ))

    fig.update_layout(
        **_base_layout(
            title="출근 vs 퇴근 추정 인원 비교",
            xaxis_title="날짜",
            yaxis_title="추정 인원",
            barmode="group",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
    )
    return fig


def create_multidate_comparison_chart(
    fine_df: pd.DataFrame,
    selected_dates: list[str],
) -> go.Figure:
    """여러 날짜의 5분 단위 DC를 꺾은선 그래프로 겹쳐 비교."""
    fig = go.Figure()
    palette = [
        "#4A90D9", "#F5A623", "#50C878", "#E85D75", "#6C5CE7",
        "#FF6B6B", "#00D2FF", "#FFD93D", "#FF8C42", "#A3CB38",
    ]

    for i, date_str in enumerate(selected_dates):
        day_df = fine_df[fine_df["date"] == date_str].sort_values("time_bin")
        if day_df.empty:
            continue
        time_labels = [f"{int(t) // 60:02d}:{int(t) % 60:02d}" for t in day_df["time_bin"]]
        color = palette[i % len(palette)]
        fig.add_trace(go.Scatter(
            x=time_labels, y=day_df["dc"], mode="lines", name=date_str,
            line=dict(color=color, width=2),
            hovertemplate=f"{date_str}<br>%{{x}}<br>DC: %{{y:,.0f}}<extra></extra>",
        ))

    fig.update_layout(
        **_base_layout(
            title=f"날짜별 5분 단위 트래픽 비교 ({len(selected_dates)}일)",
            xaxis_title="시간", yaxis_title="Device Count",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            height=480, xaxis=dict(dtick=12, tickangle=-45),
        )
    )
    return fig


def create_period_avg_chart(fine_df: pd.DataFrame, selected_dates: list[str]) -> go.Figure:
    """선택 기간의 평균 5분 단위 프로파일."""
    subset = fine_df[fine_df["date"].isin(selected_dates)]
    if subset.empty:
        return go.Figure().update_layout(**_base_layout(title="데이터 없음"))

    avg = subset.groupby("time_bin")["dc"].mean().reset_index()
    time_labels = [f"{int(t) // 60:02d}:{int(t) % 60:02d}" for t in avg["time_bin"]]

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=time_labels, y=avg["dc"],
        mode="lines",
        name="평균 DC",
        line=dict(color=COLORS["primary"], width=2.5),
        fill="tozeroy", fillcolor="rgba(74,144,217,0.15)",
        hovertemplate="%{x}<br>평균 DC: %{y:,.0f}<extra></extra>",
    ))

    fig.update_layout(
        **_base_layout(
            title=f"선택 기간 평균 트래픽 프로파일 ({len(selected_dates)}일)",
            xaxis_title="시간",
            yaxis_title="평균 Device Count",
            showlegend=False,
            height=380,
            xaxis=dict(dtick=12, tickangle=-45),
        )
    )
    return fig


def create_dwell_histogram(dwell_df: pd.DataFrame) -> go.Figure:
    """출근 vs 퇴근 체류시간 히스토그램."""
    fig = go.Figure()

    for period, label, color in [("entry", "출근", COLORS["peak_am"]), ("exit", "퇴근", COLORS["peak_pm"])]:
        sub = dwell_df[dwell_df["period"] == period]
        if sub.empty:
            continue
        dwell_min = sub["dwell_sec"] / 60
        fig.add_trace(go.Histogram(
            x=dwell_min,
            name=label,
            marker_color=color,
            opacity=0.7,
            xbins=dict(start=0, end=20, size=0.5),
            hovertemplate=f"{label}<br>%{{x:.1f}}분: %{{y}}건<extra></extra>",
        ))

    fig.update_layout(
        **_base_layout(
            title="타각기 체류 시간 분포 (출근 vs 퇴근)",
            xaxis_title="체류 시간 (분)",
            yaxis_title="MAC 수",
            barmode="overlay",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            xaxis=dict(range=[0, 20]),
        )
    )
    return fig


def create_daily_dwell_chart(dwell_df: pd.DataFrame) -> go.Figure:
    """일별 출근/퇴근 평균 체류시간 추이."""
    stats = dwell_df.groupby(["date", "period"])["dwell_sec"].median().reset_index()
    stats["dwell_min"] = stats["dwell_sec"] / 60

    fig = go.Figure()
    for period, label, color in [("entry", "출근 중앙값", COLORS["peak_am"]), ("exit", "퇴근 중앙값", COLORS["peak_pm"])]:
        sub = stats[stats["period"] == period].sort_values("date")
        if sub.empty:
            continue
        fig.add_trace(go.Scatter(
            x=sub["date"], y=sub["dwell_min"],
            mode="lines+markers", name=label,
            line=dict(color=color, width=2),
            marker=dict(size=4),
            hovertemplate="%{x}<br>%{y:.1f}분<extra>" + label + "</extra>",
        ))

    fig.update_layout(
        **_base_layout(
            title="일별 타각기 체류 시간 (중앙값)",
            xaxis_title="날짜",
            yaxis_title="체류 시간 (분)",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
    )
    return fig


def create_intraday_fine_with_range(fine_df: pd.DataFrame, selected_date: str,
                                     hour_start: int = 0, hour_end: int = 24) -> go.Figure:
    """특정 날짜 + 시간 범위의 5분 단위 DC 바 차트."""
    df = fine_df[fine_df["date"] == selected_date].copy()
    bin_start = hour_start * 60
    bin_end = hour_end * 60
    df = df[(df["time_bin"] >= bin_start) & (df["time_bin"] < bin_end)]

    if df.empty:
        return go.Figure().update_layout(**_base_layout(title="데이터 없음"))

    time_labels = [f"{int(t) // 60:02d}:{int(t) % 60:02d}" for t in df["time_bin"]]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=time_labels, y=df["dc"],
        marker_color=COLORS["primary"], opacity=0.85,
        hovertemplate="%{x}<br>DC: %{y:,.0f}<extra></extra>",
    ))

    fig.update_layout(
        **_base_layout(
            title=f"5분 단위 트래픽 — {selected_date}",
            xaxis_title="시간",
            yaxis_title="Device Count",
            showlegend=False,
            height=380,
        )
    )
    return fig


def create_single_date_dwell_chart(dwell_df: pd.DataFrame, date: str) -> go.Figure:
    """특정 날짜의 출근/퇴근 체류시간 분포 히스토그램."""
    day = dwell_df[dwell_df["date"] == date]
    if day.empty:
        return go.Figure().update_layout(**_base_layout(title="체류시간 데이터 없음"))

    fig = go.Figure()
    for period, label, color in [("entry", "출근", COLORS["peak_am"]), ("exit", "퇴근", COLORS["peak_pm"])]:
        sub = day[day["period"] == period]
        if sub.empty:
            continue
        dwell_min = sub["dwell_sec"] / 60
        med = dwell_min.median()
        fig.add_trace(go.Histogram(
            x=dwell_min, name=f"{label} (중앙값 {med:.1f}분)",
            marker_color=color, opacity=0.7,
            xbins=dict(start=0, end=15, size=0.5),
            hovertemplate=f"{label}<br>%{{x:.1f}}분: %{{y}}건<extra></extra>",
        ))

    fig.update_layout(
        **_base_layout(
            title=f"체류 시간 분포 — {date}",
            xaxis_title="체류 시간 (분)", yaxis_title="MAC 수",
            barmode="overlay", xaxis=dict(range=[0, 15]),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            height=320,
        )
    )
    return fig


def create_gateway_correlation(gw_hourly_df: pd.DataFrame) -> go.Figure:
    """게이트웨이 간 시간별 DC 상관관계 히트맵."""
    pivot = gw_hourly_df.pivot_table(
        index=["date", "hour"], columns="gateway_no", values="dc", fill_value=0
    )
    corr = pivot.corr()

    fig = go.Figure(go.Heatmap(
        z=corr.values,
        x=[str(c) for c in corr.columns],
        y=[str(c) for c in corr.index],
        colorscale="RdBu_r",
        zmid=0.5,
        text=corr.round(2).values,
        texttemplate="%{text}",
        hovertemplate="GW %{x} - GW %{y}<br>상관계수: %{z:.2f}<extra></extra>",
    ))
    fig.update_layout(
        **_base_layout(
            title="게이트웨이 간 DC 상관관계",
            height=450,
        )
    )
    return fig


# ── 트래픽 기반 대기 시간 차트 ──────────────────────────────────────────────

def create_wait_time_chart(wait_results: list[dict]) -> go.Figure | None:
    """퇴근 대기 시간 분포 차트 (트래픽 기반 추정).

    각 게이트 오픈 이벤트별로 도착 시각 vs 추정 인원 막대 + 백분위 주석.
    """
    if not wait_results:
        return None

    n = len(wait_results)
    fig = make_subplots(
        rows=n, cols=1,
        subplot_titles=[
            f"{r['event']['gate_open_time']} 게이트 오픈 (피크 DC {r['event']['peak_dc']:,})"
            for r in wait_results
        ],
        vertical_spacing=0.15 if n > 1 else 0.1,
    )

    palette = ["#4A90D9", "#F5A623", "#50C878"]

    for i, result in enumerate(wait_results):
        dist = result["distribution"]
        stats = result["stats"]
        row = i + 1

        arrival_times = [d["arrival_time"] for d in dist]
        wait_mins = [d["wait_minutes"] for d in dist]
        people = [d["estimated_people"] for d in dist]

        fig.add_trace(go.Bar(
            x=arrival_times, y=people,
            name=f"{result['event']['gate_open_time']} 오픈",
            marker_color=palette[i % len(palette)],
            text=[f"{w}분" for w in wait_mins],
            textposition="outside",
            textfont=dict(size=10),
            hovertemplate="도착: %{x}<br>추정 인원: %{y}명<br>대기: %{text}<extra></extra>",
        ), row=row, col=1)

        # 백분위 주석
        ann = (
            f"중앙값 {stats['median']}분 | "
            f"P75 {stats['p75']}분 | "
            f"P90 {stats['p90']}분 | "
            f"P95 {stats['p95']}분 | "
            f"최대 {stats['max']}분 | "
            f"총 {stats['total_people']:,}명"
        )
        fig.add_annotation(
            text=ann,
            xref=f"x{row} domain", yref=f"y{row} domain",
            x=0.5, y=1.0,
            xanchor="center", yanchor="bottom",
            showarrow=False,
            font=dict(size=11, color="#8892b0"),
        )

        fig.update_yaxes(title_text="추정 인원", row=row, col=1)

    fig.update_layout(
        **_base_layout(
            title="퇴근 대기 시간 분포 (트래픽 패턴 기반 추정)",
            height=max(350, 300 * n),
        ),
        showlegend=False,
    )
    return fig


def create_gate_flow_chart(fine_df: pd.DataFrame, date: str, events: list[dict]) -> go.Figure | None:
    """게이트 오픈 전후 DC 흐름 + 모임/오픈 시점 마커."""
    day = fine_df[fine_df["date"] == date].sort_values("time_bin")
    evening = day[(day["time_bin"] >= 15 * 60) & (day["time_bin"] <= 21 * 60)]

    if evening.empty:
        return None

    times = evening["time_bin"].apply(lambda x: f"{x // 60:02d}:{x % 60:02d}")

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=times, y=evening["dc"],
        mode="lines+markers",
        name="DC (5분 단위)",
        line=dict(color=COLORS["primary"], width=2.5),
        fill="tozeroy", fillcolor="rgba(74,144,217,0.15)",
    ))

    for evt in events:
        fig.add_vline(
            x=evt["gate_open_time"],
            line_dash="dash", line_color="#E85D75", line_width=2,
            annotation_text=f"게이트 오픈 {evt['gate_open_time']}",
            annotation_position="top",
            annotation_font_color="#E85D75",
        )
        fig.add_vline(
            x=evt["gathering_start_time"],
            line_dash="dot", line_color="#F5A623", line_width=1.5,
            annotation_text=f"모임 시작 {evt['gathering_start_time']}",
            annotation_position="top left",
            annotation_font_color="#F5A623",
        )

    fig.update_layout(
        **_base_layout(
            title=f"{date} 퇴근 흐름 — 모임 → 게이트 오픈 → 빠져나감",
            xaxis_title="시간",
            yaxis_title="DC (5분 unique MAC)",
        )
    )
    return fig
