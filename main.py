"""Entrance_Analysis_Y1 -- Y1 건설현장 출입구 트래픽 분석 대시보드.

실행:
    cd SandBox/Entrance_Analysis_Y1
    streamlit run main.py --server.port 8550
"""
from __future__ import annotations

import os
from pathlib import Path

import streamlit as st
import pandas as pd

from src.data_loader import (
    is_cache_valid,
    load_daily_summary, load_hourly_summary, load_gateway_summary,
    load_gateway_daily, load_fine_summary, load_gate_flow, load_meta,
)
from src.metrics import (
    compute_overview_metrics, compute_peak_analysis, compute_weekly_trend,
    compute_monthly_comparison, compute_gateway_stats, add_day_metadata,
    estimate_exit_headcount, compute_daily_exit_headcount,
    compute_entry_headcount, compute_daily_commute_times,
    detect_gate_openings, estimate_wait_time_distribution,
    compute_all_gate_events, analyze_entry_flow, compute_all_entry_flows,
    fetch_weather, WEATHER_EMOJI, SPECIAL_DAYS,
)
from src.charts import (
    create_daily_udc_chart, create_device_ratio_chart,
    create_weekday_boxplot, create_weekly_trend_chart,
    create_gateway_donut, create_gateway_bars, create_gateway_timeline,
    create_exit_flow_chart, create_daily_headcount_chart,
    create_headcount_comparison_chart, create_entry_exit_comparison,
    create_multidate_comparison_chart, create_period_avg_chart,
    create_intraday_fine_with_range,
    create_wait_time_chart, create_gate_flow_chart, create_entry_flow_chart,
    create_gate_events_trend, create_gate_events_by_dow, create_gate_events_scatter,
)
from src.llm_analyzer import (
    is_llm_ready, analyze_daily_pattern, compare_dates_pattern,
    _build_period_summary,
)

# ── 경로 ────────────────────────────────────────────────────────────────────

BASE_DIR = Path(__file__).parent
CACHE_DIR = str(BASE_DIR / "Cache")

# ── 페이지 설정 ─────────────────────────────────────────────────────────────

st.set_page_config(page_title="Y1 출입구 트래픽", page_icon="🏗️", layout="wide")

st.markdown("""
<style>
    .metric-card {
        background: linear-gradient(135deg, #1a1f36 0%, #252b48 100%);
        border-radius: 12px; padding: 20px; text-align: center;
        border: 1px solid #2d3456;
    }
    .metric-value { font-size: 2.0rem; font-weight: 700; color: #FFFFFF; margin: 4px 0; }
    .metric-label { font-size: 0.82rem; color: #8892b0; text-transform: uppercase; letter-spacing: 0.5px; }
    .metric-sub { font-size: 0.72rem; color: #5a6785; margin-top: 4px; }
    .section-header {
        font-size: 1.1rem; font-weight: 600; color: #ccd6f6;
        margin: 24px 0 12px 0; padding-bottom: 8px; border-bottom: 1px solid #2d3456;
    }
    .info-box {
        background: #1a1f36; border-left: 3px solid #4A90D9;
        padding: 12px 16px; border-radius: 4px; font-size: 0.85rem;
        color: #8892b0; margin: 8px 0;
    }
    .ai-box {
        background: #1a1f36; border-left: 3px solid #F5A623;
        padding: 12px 16px; border-radius: 4px; font-size: 0.85rem;
        color: #ccd6f6; margin: 8px 0; line-height: 1.6;
    }
</style>
""", unsafe_allow_html=True)


def render_metric_card(label: str, value: str, sub: str = ""):
    sub_html = f'<div class="metric-sub">{sub}</div>' if sub else ""
    st.markdown(
        f'<div class="metric-card">'
        f'<div class="metric-label">{label}</div>'
        f'<div class="metric-value">{value}</div>'
        f'{sub_html}</div>', unsafe_allow_html=True,
    )


# ── 인증 ──────────────────────────────────────────────────────────────────

PASSWORD = "wonderful2$"

if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

if not st.session_state.authenticated:
    st.markdown("### Y1 출입구 트래픽 분석")
    pw = st.text_input("비밀번호", type="password", key="pw_input")
    if pw:
        if pw == PASSWORD:
            st.session_state.authenticated = True
            st.rerun()
        else:
            st.error("비밀번호가 틀렸습니다.")
    st.stop()

# ── 사이드바 ────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("### Y1 출입구 트래픽 분석")
    st.caption("건설현장 타각기 BLE 센서 데이터")
    st.divider()

    meta = load_meta(CACHE_DIR)

    if meta:
        st.success(f"캐시 로드됨 ({meta['days']}일)")
        st.caption(f"기간: {meta['date_range'][0]} ~ {meta['date_range'][1]}")
    else:


    st.divider()

# ── 데이터 로드 ─────────────────────────────────────────────────────────────

if not is_cache_valid(CACHE_DIR):
    st.error("캐시 데이터가 없습니다.")
    st.stop()

daily_df = load_daily_summary(CACHE_DIR)
hourly_df = load_hourly_summary(CACHE_DIR)
gw_hourly_df = load_gateway_summary(CACHE_DIR)
gw_daily_df = load_gateway_daily(CACHE_DIR)
fine_df = load_fine_summary(CACHE_DIR)
gate_flow_df = load_gate_flow(CACHE_DIR)

daily_df = add_day_metadata(daily_df)

# 날씨 데이터 (Open-Meteo, 24시간 캐시)
all_dates_for_weather = sorted(daily_df["date"].unique())
weather_df = fetch_weather(all_dates_for_weather[0], all_dates_for_weather[-1])
weather_map = {}
if not weather_df.empty:
    weather_map = weather_df.set_index("date").to_dict("index")


def _date_label(d: str) -> str:
    """날짜 → '2026-01-05 (월) ☀️ -5°~0°' 형식."""
    dt = pd.to_datetime(d)
    day_kr = {0: "월", 1: "화", 2: "수", 3: "목", 4: "금", 5: "토", 6: "일"}
    dow = day_kr.get(dt.dayofweek, "")
    w = weather_map.get(d, {})
    emoji = WEATHER_EMOJI.get(w.get("weather", ""), "")
    temp = ""
    if w.get("temp_min") is not None and w.get("temp_max") is not None:
        temp = f" {w['temp_min']:.0f}°~{w['temp_max']:.0f}°"
    special = SPECIAL_DAYS.get(d, "")
    label = f"{d} ({dow}) {emoji}{temp}"
    if special:
        label += f" · {special}"
    return label

# ── 사이드바 필터 + 설정 ──────────────────────────────────────────────────

with st.sidebar:
    all_dates = sorted(daily_df["date"].unique())
    if len(all_dates) >= 2:
        date_start = st.date_input("시작일", value=pd.to_datetime(all_dates[0]),
                                    min_value=pd.to_datetime(all_dates[0]),
                                    max_value=pd.to_datetime(all_dates[-1]))
        date_end = st.date_input("종료일", value=pd.to_datetime(all_dates[-1]),
                                  min_value=pd.to_datetime(all_dates[0]),
                                  max_value=pd.to_datetime(all_dates[-1]))
    else:
        date_start = pd.to_datetime(all_dates[0])
        date_end = pd.to_datetime(all_dates[-1])

    st.divider()
    st.markdown("**출퇴근 시간 설정**")
    entry_range = st.slider(
        "출근 시간대", min_value=0, max_value=12, value=(4, 8),
        step=1, format="%02d:00", key="entry_range",
    )
    exit_range = st.slider(
        "퇴근 시간대", min_value=12, max_value=24, value=(16, 20),
        step=1, format="%02d:00", key="exit_range",
    )

    st.divider()
    st.caption("**게이트 오픈**: 17:30, 19:30")

    st.divider()
    llm_ready = is_llm_ready()
    if llm_ready:
        st.success("Claude API 연결됨")
    else:
        st.caption("AI 분석: .env에 ANTHROPIC_API_KEY 설정 필요")

# ── 날짜 범위 필터 ────────────────────────────────────────────────────────

ds, de = str(date_start), str(date_end)
daily_f = daily_df[(daily_df["date"] >= ds) & (daily_df["date"] <= de)].copy()
hourly_f = hourly_df[(hourly_df["date"] >= ds) & (hourly_df["date"] <= de)].copy()
gw_hourly_f = gw_hourly_df[(gw_hourly_df["date"] >= ds) & (gw_hourly_df["date"] <= de)].copy()
gw_daily_f = gw_daily_df[(gw_daily_df["date"] >= ds) & (gw_daily_df["date"] <= de)].copy()
fine_f = fine_df[(fine_df["date"] >= ds) & (fine_df["date"] <= de)].copy()
gf_f = gate_flow_df[(gate_flow_df["date"] >= ds) & (gate_flow_df["date"] <= de)].copy() if not gate_flow_df.empty else pd.DataFrame()

if daily_f.empty:
    st.warning("선택한 기간에 데이터가 없습니다.")
    st.stop()

# ── 사전 계산 (출퇴근 시간 설정 반영) ────────────────────────────────────

exit_fine = estimate_exit_headcount(fine_f, exit_range[0], 0, exit_range[1], 0)
daily_exit = compute_daily_exit_headcount(fine_f, exit_start_hour=exit_range[0],
                                           exit_end_hour=exit_range[1])
entry_est = compute_entry_headcount(fine_f)
commute_times = compute_daily_commute_times(fine_f)

# ── 탭 ─────────────────────────────────────────────────────────────────────

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "날짜별 분석", "기간별 비교", "AI Insight", "인원 추정", "상세"
])

# ── 탭1: 날짜별 분석 ─────────────────────────────────────────────────────

with tab1:
    if not commute_times.empty and not daily_exit.empty:
        day_summary = commute_times.merge(
            daily_exit[["date", "est_headcount"]], on="date", how="left"
        ).merge(
            daily_f[["date", "day_name_kr", "day_type", "udc"]], on="date", how="left"
        )
        day_summary["est_headcount"] = day_summary["est_headcount"].fillna(0).astype(int)

        avg_exit_est = int(daily_exit["est_headcount"].mean())
        avg_entry_time = commute_times[commute_times["entry_peak"] != ""]["entry_peak"].mode()
        avg_exit_time = commute_times[commute_times["exit_peak"] != ""]["exit_peak"].mode()

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            render_metric_card("일 평균 추정 인원", f"{avg_exit_est:,}명", "퇴근 5분 윈도우 기반")
        with c2:
            render_metric_card("출근 피크", avg_entry_time.iloc[0] if len(avg_entry_time) > 0 else "-",
                              "가장 빈번한 피크 시각")
        with c3:
            render_metric_card("퇴근 피크", avg_exit_time.iloc[0] if len(avg_exit_time) > 0 else "-",
                              "가장 빈번한 피크 시각")
        with c4:
            render_metric_card("평균 활성 시간", f"{commute_times['active_hours'].mean():.1f}시간",
                              "DC≥30 기준")

        st.markdown("")

        # 날짜별 테이블
        display_cols = day_summary[[
            "date", "day_name_kr", "day_type",
            "entry_start", "entry_peak", "exit_peak", "exit_end",
            "est_headcount", "active_hours"
        ]].copy()
        display_cols.columns = [
            "날짜", "요일", "구분", "출근 시작", "출근 피크", "퇴근 피크", "퇴근 종료",
            "추정 인원", "활성(시간)"
        ]
        st.dataframe(display_cols, use_container_width=True, hide_index=True)

        # ── 날짜 선택 → 출근/퇴근 상세 분석 ──
        st.markdown("")
        avail_dates = sorted(day_summary["date"].unique())
        date_options = {d: _date_label(d) for d in avail_dates}
        sel_date = st.selectbox(
            "날짜 선택", avail_dates, index=len(avail_dates) - 1,
            format_func=lambda d: date_options.get(d, d), key="day_select",
        )

        if sel_date:

            # 전체 하루 개요
            st.plotly_chart(
                create_intraday_fine_with_range(fine_f, sel_date, 0, 24),
                use_container_width=True,
            )

            # 출근 / 퇴근 서브탭
            sub_entry, sub_exit = st.tabs(["출근 분석", "퇴근 분석"])

            # ── 출근 분석 ──
            with sub_entry:
                entry_info = analyze_entry_flow(fine_f, gf_f, sel_date)
                if entry_info:
                    c1, c2, c3, c4, c5 = st.columns(5)
                    with c1:
                        render_metric_card("피크 혼잡도",
                                          f"{entry_info['peak_crowd']:,}명",
                                          f"피크 {entry_info['peak_time']}")
                    with c2:
                        render_metric_card("러시",
                                          f"{entry_info['rush_start']}~{entry_info['rush_end']}",
                                          f"{entry_info['rush_duration']}분간")
                    with c3:
                        render_metric_card("평균 통과",
                                          f"{entry_info['avg_throughput']}명/분",
                                          "게이트 처리량")
                    with c4:
                        render_metric_card("피크 통과",
                                          f"{entry_info['peak_throughput']}명/분",
                                          "최대 처리량")
                    with c5:
                        render_metric_card("평균 유입",
                                          f"{entry_info['avg_inflow']}명/분",
                                          "신규 MAC 기반")

                    entry_fig = create_entry_flow_chart(fine_f, sel_date, entry_info)
                    if entry_fig:
                        st.plotly_chart(entry_fig, use_container_width=True, key="entry_flow")

                    st.markdown(
                        '<div class="info-box">'
                        '<b>출근 특성</b> — 게이트가 항상 열려 있어 도착하는 대로 바로 통과합니다. '
                        '대기 시간 없이 연속적으로 유입되며, 개인별 출근 시각에 따라 분산됩니다.'
                        '</div>', unsafe_allow_html=True,
                    )
                else:
                    st.caption("해당 날짜에 출근 데이터가 없습니다.")

            # ── 퇴근 분석 ──
            with sub_exit:
                events = detect_gate_openings(fine_f, sel_date, gf_f)
                if events:
                    gate_fig = create_gate_flow_chart(fine_f, sel_date, events)
                    if gate_fig:
                        st.plotly_chart(gate_fig, use_container_width=True, key="gate_flow")

                    wait_results = estimate_wait_time_distribution(fine_f, sel_date, gf_f)
                    if wait_results:
                        bl = wait_results[0]["event"].get("baseline_dc", 0)
                        if bl > 0:
                            st.caption(f"배경 트래픽 DC ~{bl} (14~16시 기준) 차감 후 분석")

                        for idx, wr in enumerate(wait_results):
                            evt = wr["event"]
                            stats = wr["stats"]
                            drain_min = evt.get("drain_minutes", 0)
                            clear_t = evt.get("gf_clear_time", evt.get("clear_time", ""))
                            cols = st.columns(6)
                            with cols[0]:
                                render_metric_card(
                                    f"{idx+1}차 퇴근 ({evt['gate_open_time']})",
                                    f"{evt['peak_dc']:,}명",
                                    f"모임 {evt['gathering_start_time']} ~")
                            with cols[1]:
                                render_metric_card("통과 소요", f"{drain_min}분",
                                                  f"완료 {clear_t}")
                            with cols[2]:
                                render_metric_card("평균 유출",
                                                  f"{evt.get('gf_avg_outflow', 0)}명/분", "")
                            with cols[3]:
                                render_metric_card("피크 유출",
                                                  f"{evt.get('gf_peak_outflow', 0)}명/분", "")
                            with cols[4]:
                                render_metric_card("대기 중앙값", f"{stats['median']}분",
                                                  f"{stats['total_people']:,}명")
                            with cols[5]:
                                render_metric_card("최대 대기", f"{stats['max']}분", "")

                        wait_fig = create_wait_time_chart(wait_results)
                        if wait_fig:
                            st.plotly_chart(wait_fig, use_container_width=True, key="wait_dist")

                    st.markdown(
                        '<div class="info-box">'
                        '<b>퇴근 특성</b> — 게이트가 정해진 시간(17:30, 19:30)에 열립니다. '
                        '근로자들은 오픈 전에 모여서 대기하며, 오픈 후 일제히 빠져나갑니다.'
                        '</div>', unsafe_allow_html=True,
                    )
                else:
                    st.caption("해당 날짜에 퇴근 이벤트가 없습니다 (공휴일 등).")

    else:
        st.warning("데이터가 부족합니다.")

# ── 탭2: 기간별 비교 ─────────────────────────────────────────────────────

with tab2:
    avail_dates_all = sorted(fine_f["date"].unique())
    date_opts_all = {d: _date_label(d) for d in avail_dates_all}

    compare_dates = st.multiselect(
        "비교할 날짜 선택 (최대 10일)",
        avail_dates_all,
        default=avail_dates_all[-3:] if len(avail_dates_all) >= 3 else avail_dates_all,
        format_func=lambda d: date_opts_all.get(d, d),
        max_selections=10, key="compare_dates",
    )

    if compare_dates:
        st.plotly_chart(create_multidate_comparison_chart(fine_f, compare_dates), use_container_width=True)
        st.plotly_chart(create_period_avg_chart(fine_f, compare_dates), use_container_width=True)

    # 대기 시간 · 통과 시간 전체 기간 분석
    st.markdown('<div class="section-header">퇴근 대기 · 통과 시간 분석</div>', unsafe_allow_html=True)

    gate_events = compute_all_gate_events(fine_f, gf_f)
    if not gate_events.empty:
        # 전체 요약 메트릭
        e1 = gate_events[gate_events["gate_open"].str.startswith("17")]
        e2 = gate_events[gate_events["gate_open"].str.startswith("19")]

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            v = e1["wait_p90"].median() if not e1.empty else 0
            render_metric_card("1차 대기 P90", f"{v:.0f}분", f"~17:30 오픈 ({len(e1)}회)")
        with c2:
            v = e1["drain_minutes"].median() if not e1.empty else 0
            render_metric_card("1차 통과 소요", f"{v:.0f}분", "오픈→완료")
        with c3:
            v = e2["wait_p90"].median() if not e2.empty else 0
            render_metric_card("2차 대기 P90", f"{v:.0f}분", f"~19:30 오픈 ({len(e2)}회)")
        with c4:
            v = e2["drain_minutes"].median() if not e2.empty else 0
            render_metric_card("2차 통과 소요", f"{v:.0f}분", "오픈→완료")

        st.plotly_chart(create_gate_events_trend(gate_events), use_container_width=True, key="gate_trend")
        st.plotly_chart(create_gate_events_by_dow(gate_events), use_container_width=True, key="gate_dow")
        st.plotly_chart(create_gate_events_scatter(gate_events), use_container_width=True, key="gate_scatter")

        with st.expander("전체 게이트 이벤트 테이블"):
            display_events = gate_events[[
                "date", "day_name_kr", "day_type", "gate_open",
                "gathering_start", "peak_dc", "drain_minutes",
                "wait_median", "wait_p90", "wait_max", "total_people"
            ]].copy()
            display_events.columns = [
                "날짜", "요일", "구분", "오픈 시각", "모임 시작",
                "피크 DC", "통과(분)", "대기 중앙값", "대기 P90", "대기 최대", "추정 인원"
            ]
            st.dataframe(display_events, use_container_width=True, hide_index=True)
    else:
        st.caption("게이트 이벤트가 없습니다.")

# ── 탭3: AI Insight ───────────────────────────────────────────────────────

with tab3:
    if not llm_ready:
        st.markdown(
            '<div class="info-box">'
            'Claude API가 연결되지 않았습니다.<br>'
            '<code>.env</code> 파일에 <code>ANTHROPIC_API_KEY=sk-ant-...</code>를 설정하세요.'
            '</div>', unsafe_allow_html=True,
        )
    else:
        st.markdown(
            '<div class="info-box">'
            'Claude API로 날짜별 트래픽 패턴을 분석합니다. '
            '피크 시간 변동, 이상 패턴, 날짜 간 차이의 원인을 추론합니다.'
            '</div>', unsafe_allow_html=True,
        )

    ai_mode = st.radio(
        "분석 모드",
        ["날짜 분석", "날짜 비교"],
        horizontal=True, key="ai_mode",
    )

    avail_dates_ai = sorted(fine_f["date"].unique())
    date_opts_ai = {d: _date_label(d) for d in avail_dates_ai}

    if ai_mode == "날짜 분석":
        ai_date = st.selectbox("날짜 선택", avail_dates_ai,
                                format_func=lambda d: date_opts_ai.get(d, d),
                                index=len(avail_dates_ai) - 1, key="ai_date")

        if ai_date:
            st.plotly_chart(
                create_intraday_fine_with_range(fine_f, ai_date, 0, 24),
                use_container_width=True, key="ai_intraday_chart",
            )

            if llm_ready:
                if st.button("AI 분석 실행", key="run_ai_single", type="primary"):
                    with st.spinner("Claude가 분석 중..."):
                        # 출근/퇴근 분석 데이터 구성
                        ai_entry = analyze_entry_flow(fine_f, gf_f, ai_date)
                        ai_waits = estimate_wait_time_distribution(fine_f, ai_date, gf_f)
                        # 최근 10일 요약 (비교 맥락)
                        recent = [d for d in avail_dates_ai if d != ai_date][-10:]
                        other_summary = _build_period_summary(fine_f, recent, weather_map)

                        result = analyze_daily_pattern(
                            fine_f, ai_date,
                            weather_map=weather_map,
                            entry_info=ai_entry,
                            wait_results=ai_waits,
                            other_dates_summary=other_summary,
                        )
                    if result:
                        st.markdown(f'<div class="ai-box">{result}</div>', unsafe_allow_html=True)
                    else:
                        st.warning("분석 결과를 생성하지 못했습니다.")

    else:  # 날짜 비교
        ai_compare = st.multiselect(
            "비교할 날짜 선택 (2~5일)",
            avail_dates_ai,
            default=avail_dates_ai[-2:] if len(avail_dates_ai) >= 2 else avail_dates_ai,
            format_func=lambda d: date_opts_ai.get(d, d),
            max_selections=5, key="ai_compare",
        )

        if ai_compare and len(ai_compare) >= 2:
            st.plotly_chart(
                create_multidate_comparison_chart(fine_f, ai_compare),
                use_container_width=True, key="ai_compare_chart",
            )

            if llm_ready:
                if st.button("AI 비교 분석 실행", key="run_ai_compare", type="primary"):
                    with st.spinner("Claude가 비교 분석 중..."):
                        # 각 날짜별 출근/퇴근 데이터 수집
                        cmp_entries = {}
                        cmp_waits = {}
                        for d in ai_compare:
                            cmp_entries[d] = analyze_entry_flow(fine_f, gf_f, d)
                            cmp_waits[d] = estimate_wait_time_distribution(fine_f, d, gf_f)

                        result = compare_dates_pattern(
                            fine_f, ai_compare,
                            weather_map=weather_map,
                            all_entry_infos=cmp_entries,
                            all_wait_results=cmp_waits,
                        )
                    if result:
                        st.markdown(f'<div class="ai-box">{result}</div>', unsafe_allow_html=True)
                    else:
                        st.warning("분석 결과를 생성하지 못했습니다.")
        else:
            st.info("2개 이상의 날짜를 선택하세요.")

# ── 탭4: 인원 추정 ────────────────────────────────────────────────────────

with tab4:
    st.markdown(
        '<div class="info-box">'
        f'<b>인원 추정 기준</b> — 퇴근 시간대({exit_range[0]:02d}:00~{exit_range[1]:02d}:00) '
        '5분 윈도우 unique MAC 기반. 사이드바에서 시간대 조정 가능.'
        '</div>', unsafe_allow_html=True,
    )

    if not daily_exit.empty:
        st.plotly_chart(create_daily_headcount_chart(daily_exit), use_container_width=True)
        if not entry_est.empty:
            st.plotly_chart(create_entry_exit_comparison(entry_est, daily_exit), use_container_width=True)
    else:
        st.warning("퇴근 시간대 데이터가 없습니다.")

# ── 탭5: 상세 ──────────────────────────────────────────────────────────

with tab5:
    sub1, sub2, sub3 = st.tabs(["게이트웨이", "일별 데이터", "캐시 정보"])

    with sub1:
        gw_stats = compute_gateway_stats(gw_daily_f)
        c1, c2 = st.columns(2)
        with c1:
            st.plotly_chart(create_gateway_donut(gw_stats), use_container_width=True)
        with c2:
            st.plotly_chart(create_gateway_bars(gw_stats), use_container_width=True)
        st.plotly_chart(create_gateway_timeline(gw_hourly_f), use_container_width=True)

    with sub2:
        display_df = daily_f[["date", "day_name_kr", "day_type", "udc", "total_records",
                              "ios_udc", "android_udc", "avg_rssi", "special"]].copy()
        if not daily_exit.empty:
            display_df = display_df.merge(daily_exit[["date", "est_headcount"]], on="date", how="left")
            display_df["est_headcount"] = display_df["est_headcount"].fillna(0).astype(int)
        display_df.columns = (["날짜", "요일", "구분", "UDC", "총 레코드",
                               "iOS UDC", "Android UDC", "평균 RSSI", "특이일"]
                              + (["추정 인원"] if not daily_exit.empty else []))
        st.dataframe(display_df, use_container_width=True, hide_index=True)
        csv_data = display_df.to_csv(index=False).encode("utf-8-sig")
        st.download_button("CSV 다운로드", csv_data, "entrance_daily_summary.csv", "text/csv")

    with sub3:
        meta = load_meta(CACHE_DIR)
        if meta:
            c1, c2, c3 = st.columns(3)
            with c1:
                render_metric_card("캐시 버전", meta["cache_version"], f"생성: {meta['created_at'][:10]}")
            with c2:
                render_metric_card("총 행", f"{meta['total_rows']:,}", "원본 CSV")
            with c3:
                render_metric_card("기간", f"{meta['days']}일",
                                  f"{meta['date_range'][0]} ~ {meta['date_range'][1]}")
