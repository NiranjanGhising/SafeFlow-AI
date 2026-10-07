"""SafeFlow AI monitoring dashboard powered directly by SQLite."""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATABASE_PATH = PROJECT_ROOT / "data" / "safeflow_analytics.db"

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


st.set_page_config(
    page_title="SafeFlow AI Dashboard",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
        .block-container {padding-top: 1.4rem; padding-bottom: 2rem;}
        [data-testid="stMetric"] {
            background: #111827;
            border: 1px solid #263348;
            border-radius: 12px;
            padding: 14px;
        }
        [data-testid="stMetricLabel"] {color: #a9b7cc;}
        [data-testid="stMetricValue"] {color: #f8fafc;}
        .sf-note {
            padding: 0.8rem 1rem;
            border-left: 4px solid #22c55e;
            background: rgba(34, 197, 94, 0.08);
            border-radius: 6px;
        }
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_data(ttl=15)
def query_data(query: str, parameters: tuple = ()) -> pd.DataFrame:
    """Execute a read-only SQLite query and return a DataFrame."""

    if not DATABASE_PATH.exists():
        raise FileNotFoundError(
            f"Database not found: {DATABASE_PATH}. Run the analytics pipeline first."
        )

    connection = sqlite3.connect(DATABASE_PATH)

    try:
        return pd.read_sql_query(
            query,
            connection,
            params=parameters,
        )
    finally:
        connection.close()


@st.cache_data(ttl=15)
def load_safety_events() -> pd.DataFrame:
    """Load the flattened analytical safety-event dataset."""

    return query_data(
        """
        SELECT
            f.safety_event_key,
            f.request_id,
            f.pipeline_run_id,
            f.event_timestamp,
            d.full_date,
            d.day_name,
            d.month_name,
            d.quarter,
            d.year,
            c.category_name,
            c.is_attack_category,
            a.action_name,
            a.severity_rank,
            a.risk_level,
            f.corpus,
            f.fixture_id,
            f.prompt,
            f.final_output,
            f.pre_gen_category,
            f.pre_gen_confidence,
            f.pre_gen_blocked,
            f.terminated_early,
            f.post_gen_checked,
            f.is_attack,
            f.was_allowed,
            f.was_warned,
            f.was_redacted,
            f.was_blocked,
            f.latency_ms,
            f.latency_band,
            f.loaded_at
        FROM fact_safety_event AS f
        JOIN dim_date AS d
          ON d.date_key = f.date_key
        JOIN dim_category AS c
          ON c.category_key = f.category_key
        JOIN dim_action AS a
          ON a.action_key = f.action_key
        ORDER BY f.safety_event_key;
        """
    )


@st.cache_data(ttl=15)
def load_pipeline_audit() -> pd.DataFrame:
    """Load pipeline audit history."""

    return query_data(
        """
        SELECT
            pipeline_run_id,
            start_time,
            end_time,
            watermark_start,
            watermark_end,
            source_count,
            valid_count,
            rejected_count,
            duplicate_count,
            filtered_count,
            loaded_count,
            unexplained_count,
            status,
            error_message
        FROM pipeline_audit
        ORDER BY start_time DESC;
        """
    )


@st.cache_data(ttl=15)
def load_rejections() -> pd.DataFrame:
    """Load quarantined safety events."""

    return query_data(
        """
        SELECT
            rejection_id,
            pipeline_run_id,
            request_id,
            source_position,
            error_reason,
            rejected_at
        FROM rejected_safety_events
        ORDER BY rejection_id DESC;
        """
    )


@st.cache_data(ttl=15)
def load_pipeline_state() -> pd.DataFrame:
    """Load the current incremental-processing state."""

    return query_data(
        """
        SELECT
            pipeline_name,
            last_successful_watermark,
            last_pipeline_run_id,
            updated_at
        FROM pipeline_state
        WHERE pipeline_name = 'safety_analytics_pipeline';
        """
    )


def metric_row(events: pd.DataFrame) -> None:
    """Render primary safety metrics."""

    total = int(events["request_id"].nunique())
    attacks = int(events["is_attack"].sum())
    benign = total - attacks
    blocked = int(events["was_blocked"].sum())
    redacted = int(events["was_redacted"].sum())
    latency = float(events["latency_ms"].mean()) if total else 0.0

    columns = st.columns(6)
    values = [
        ("Total requests", total),
        ("Attack requests", attacks),
        ("Benign requests", benign),
        ("Blocked", blocked),
        ("Redacted", redacted),
        ("Avg latency", f"{latency:.2f} ms"),
    ]

    for column, (label, value) in zip(columns, values):
        column.metric(label, value)


if st.sidebar.button("Refresh warehouse data", use_container_width=True):
    st.cache_data.clear()
    st.rerun()

st.sidebar.header("Dashboard filters")

try:
    events = load_safety_events()
    audit = load_pipeline_audit()
    rejections = load_rejections()
    pipeline_state = load_pipeline_state()
except Exception as error:
    st.error(str(error))
    st.stop()

if events.empty:
    st.warning("No warehouse events are available. Run: python -m pipeline.run_pipeline")
    st.stop()

category_options = sorted(events["category_name"].dropna().unique().tolist())
action_options = sorted(events["action_name"].dropna().unique().tolist())
risk_options = sorted(events["risk_level"].dropna().unique().tolist())

selected_categories = st.sidebar.multiselect(
    "Categories",
    category_options,
    default=category_options,
)
selected_actions = st.sidebar.multiselect(
    "Actions",
    action_options,
    default=action_options,
)
selected_risks = st.sidebar.multiselect(
    "Risk levels",
    risk_options,
    default=risk_options,
)

filtered = events[
    events["category_name"].isin(selected_categories)
    & events["action_name"].isin(selected_actions)
    & events["risk_level"].isin(selected_risks)
].copy()

st.title("🛡️ SafeFlow AI Monitoring Dashboard")
st.caption(
    "AI safety decisions, attack categories, checkpoint interventions, "
    "and analytics-pipeline reliability from the reconciled SQLite warehouse."
)

if filtered.empty:
    st.warning("No records match the selected filters.")
    st.stop()

metric_row(filtered)

safety_tab, checkpoint_tab, reliability_tab, events_tab = st.tabs(
    [
        "Safety overview",
        "Checkpoint analysis",
        "Pipeline reliability",
        "Event explorer",
    ]
)

with safety_tab:
    left, right = st.columns(2)

    action_counts = (
        filtered.groupby("action_name", as_index=False)
        .agg(request_count=("request_id", "nunique"))
    )

    action_chart = (
        alt.Chart(action_counts)
        .mark_arc(innerRadius=55)
        .encode(
            theta=alt.Theta("request_count:Q"),
            color=alt.Color(
                "action_name:N",
                scale=alt.Scale(
                    domain=["allow", "warn", "redact", "block"],
                    range=["#22c55e", "#eab308", "#f97316", "#ef4444"],
                ),
                title="Final action",
            ),
            tooltip=["action_name:N", "request_count:Q"],
        )
        .properties(title="Final safety-action distribution", height=330)
    )

    left.altair_chart(action_chart, use_container_width=True)

    category_counts = (
        filtered.groupby("category_name", as_index=False)
        .agg(request_count=("request_id", "nunique"))
        .sort_values("request_count", ascending=False)
    )

    category_chart = (
        alt.Chart(category_counts)
        .mark_bar(color="#38bdf8")
        .encode(
            x=alt.X("request_count:Q", title="Requests"),
            y=alt.Y(
                "category_name:N",
                sort="-x",
                title="Category",
            ),
            tooltip=["category_name:N", "request_count:Q"],
        )
        .properties(title="Requests by safety category", height=330)
    )

    right.altair_chart(category_chart, use_container_width=True)

    category_action = (
        filtered.groupby(["category_name", "action_name"], as_index=False)
        .agg(request_count=("request_id", "nunique"))
    )

    category_action_chart = (
        alt.Chart(category_action)
        .mark_bar()
        .encode(
            x=alt.X("category_name:N", title="Safety category"),
            y=alt.Y("request_count:Q", title="Requests"),
            color=alt.Color(
                "action_name:N",
                scale=alt.Scale(
                    domain=["allow", "warn", "redact", "block"],
                    range=["#22c55e", "#eab308", "#f97316", "#ef4444"],
                ),
                title="Action",
            ),
            tooltip=["category_name:N", "action_name:N", "request_count:Q"],
        )
        .properties(title="Safety actions by category", height=360)
    )

    st.altair_chart(category_action_chart, use_container_width=True)

with checkpoint_tab:
    pre_blocks = int(filtered["pre_gen_blocked"].sum())
    terminations = int(filtered["terminated_early"].sum())
    post_checks = int(filtered["post_gen_checked"].sum())
    avg_confidence = float(filtered["pre_gen_confidence"].mean())

    columns = st.columns(4)
    columns[0].metric("Pre-generation blocks", pre_blocks)
    columns[1].metric("Early stream terminations", terminations)
    columns[2].metric("Post-generation checks", post_checks)
    columns[3].metric("Average pre-gen confidence", f"{avg_confidence:.2%}")

    checkpoint_data = pd.DataFrame(
        {
            "checkpoint": [
                "Pre-generation blocks",
                "During-generation stops",
                "Post-generation checks",
            ],
            "count": [pre_blocks, terminations, post_checks],
        }
    )

    checkpoint_chart = (
        alt.Chart(checkpoint_data)
        .mark_bar(color="#8b5cf6")
        .encode(
            x=alt.X("checkpoint:N", sort=None, title="Checkpoint"),
            y=alt.Y("count:Q", title="Events"),
            tooltip=["checkpoint:N", "count:Q"],
        )
        .properties(title="Safety-checkpoint activity", height=350)
    )
    st.altair_chart(checkpoint_chart, use_container_width=True)

    latency_data = (
        filtered.groupby("latency_band", as_index=False)
        .agg(request_count=("request_id", "nunique"))
    )
    latency_chart = (
        alt.Chart(latency_data)
        .mark_bar(color="#06b6d4")
        .encode(
            x=alt.X("latency_band:N", sort=["Fast", "Moderate", "Slow"]),
            y=alt.Y("request_count:Q", title="Requests"),
            tooltip=["latency_band:N", "request_count:Q"],
        )
        .properties(title="Local safety-gate latency bands", height=300)
    )
    st.altair_chart(latency_chart, use_container_width=True)
    st.info(
        "Latency covers the local safety-gate prototype and excludes production "
        "network and LLM inference latency."
    )

with reliability_tab:
    successful_runs = int((audit["status"] == "SUCCESS").sum()) if not audit.empty else 0
    failed_runs = int((audit["status"] == "FAILED").sum()) if not audit.empty else 0
    total_loaded = int(audit["loaded_count"].sum()) if not audit.empty else 0
    unexplained = int(audit["unexplained_count"].sum()) if not audit.empty else 0
    current_watermark = (
        pipeline_state.iloc[0]["last_successful_watermark"]
        if not pipeline_state.empty
        else None
    )

    columns = st.columns(5)
    columns[0].metric("Successful runs", successful_runs)
    columns[1].metric("Failed runs", failed_runs)
    columns[2].metric("Loaded across runs", total_loaded)
    columns[3].metric("Unexplained records", unexplained)
    columns[4].metric("Current watermark", current_watermark or "None")

    if unexplained == 0:
        st.markdown(
            '<div class="sf-note"><strong>Reconciliation is healthy:</strong> '
            'all processed records are accounted for.</div>',
            unsafe_allow_html=True,
        )
    else:
        st.error("Pipeline reconciliation has unexplained records.")

    st.subheader("Pipeline run history")
    display_columns = [
        "pipeline_run_id",
        "start_time",
        "end_time",
        "watermark_start",
        "watermark_end",
        "source_count",
        "loaded_count",
        "rejected_count",
        "duplicate_count",
        "unexplained_count",
        "status",
        "error_message",
    ]
    st.dataframe(
        audit[display_columns],
        use_container_width=True,
        hide_index=True,
    )

    st.subheader("Quarantined records")
    if rejections.empty:
        st.success("No records are currently in quarantine.")
    else:
        st.dataframe(rejections, use_container_width=True, hide_index=True)

with events_tab:
    search_text = st.text_input(
        "Search prompts",
        placeholder="Enter text contained in a prompt...",
    )

    event_view = filtered.copy()
    if search_text.strip():
        event_view = event_view[
            event_view["prompt"].str.contains(
                search_text,
                case=False,
                na=False,
            )
        ]

    event_columns = [
        "request_id",
        "event_timestamp",
        "corpus",
        "category_name",
        "action_name",
        "risk_level",
        "pre_gen_confidence",
        "terminated_early",
        "latency_ms",
        "prompt",
    ]

    st.dataframe(
        event_view[event_columns],
        use_container_width=True,
        hide_index=True,
    )

st.divider()
st.caption(
    "Prototype limitations: deterministic mock model, simplified local safety "
    "classifiers, limited fixture corpus, and non-production latency measurements."
)
