import base64
import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import logging
from database import init_db, load_energy_data, load_same_hour_last_week, load_anomalies
from ai_summary import load_latest_summary
from scheduler import start_scheduler

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

st.set_page_config(
    page_title="Brandenburg Energy Monitor",
    page_icon="⚡",
    layout="wide"
)

COLORS = {
    "wind_onshore": "#4db8ff",
    "solar": "#ffcc00",
    "consumption": "#ff4444",
}

LABELS = {
    "wind_onshore": "Wind Onshore",
    "solar": "Solar",
    "consumption": "Consumption",
}


def apply_dark_theme() -> None:
    """Apply custom dark cyberpunk CSS theme to the Streamlit app."""
    st.markdown("""
        <style>
        .stApp {
            background-color: #0a0e1a;
            color: #e0e8ff;
        }
        .metric-card {
            background: #0d1424;
            border-radius: 10px;
            padding: 16px 20px;
            border: 1px solid #1a2540;
            text-align: center;
        }
        .metric-label {
            font-size: 11px;
            letter-spacing: 0.12em;
            text-transform: uppercase;
            color: #4a6080;
            margin-bottom: 8px;
        }
        .metric-value {
            font-size: 28px;
            font-weight: 700;
            font-family: 'Courier New', monospace;
        }
        .metric-unit {
            font-size: 12px;
            color: #4a6080;
            margin-top: 4px;
        }
        .wind-color { color: #4db8ff; text-shadow: 0 0 12px rgba(77,184,255,0.6); }
        .solar-color { color: #ffcc00; text-shadow: 0 0 12px rgba(255,204,0,0.6); }
        .consumption-color { color: #ff4444; text-shadow: 0 0 12px rgba(255,68,68,0.6); }
        .section-title {
            font-size: 11px;
            letter-spacing: 0.12em;
            text-transform: uppercase;
            color: #4a6080;
            margin-bottom: 12px;
            border-bottom: 1px solid #1a2540;
            padding-bottom: 8px;
        }
        div[data-testid="stMetricValue"] { color: #e0e8ff; }
        </style>
    """, unsafe_allow_html=True)


def convert_df_to_csv(df: pd.DataFrame) -> bytes:
    """
    Convert a DataFrame to CSV bytes for download.
    Uses UTF-8 BOM encoding so Excel opens it correctly on all systems.

    Parameters:
        df: DataFrame to convert

    Returns:
        CSV content as bytes
    """
    return df.to_csv(index=False).encode("utf-8-sig")


def get_download_link(df: pd.DataFrame, metric: str) -> str:
    """
    Generate a styled HTML download link for a DataFrame as CSV.

    Parameters:
        df: DataFrame to export
        metric: metric name for filename

    Returns:
        HTML anchor tag string
    """
    csv_bytes = convert_df_to_csv(df)
    b64 = base64.b64encode(csv_bytes).decode()
    filename = f"{metric}_{pd.Timestamp.now().strftime('%Y%m%d')}.csv"
    label = metric.replace("_", " ").title()
    return f"""
        <a href="data:text/csv;base64,{b64}" download="{filename}" style="
            display:block;
            text-align:center;
            padding:8px 0;
            color:#4a6080;
            font-family:'Courier New',monospace;
            font-size:10px;
            letter-spacing:0.1em;
            text-transform:uppercase;
            text-decoration:none;
            border:1px solid #1a2540;
            border-radius:6px;
            margin-top:8px;
        ">⬇ Export {label} CSV</a>
    """


def render_metric_cards(data: dict) -> None:
    """
    Render the three live metric cards at the top of the dashboard.

    Parameters:
        data: Dictionary mapping metric name to its DataFrame

    Returns:
        None
    """
    col1, col2, col3 = st.columns(3)
    columns = [col1, col2, col3]
    metrics = ["wind_onshore", "solar", "consumption"]
    css_classes = ["wind-color", "solar-color", "consumption-color"]

    for col, metric, css_class in zip(columns, metrics, css_classes):
        with col:
            if metric in data and data[metric] is not None:
                df = data[metric]
                latest_value = df["value_mw"].iloc[-1]
                latest_timestamp = int(df["timestamp"].iloc[-1])

                last_week_value = load_same_hour_last_week(metric, latest_timestamp)

                if last_week_value is not None and last_week_value > 0:
                    delta = ((latest_value - last_week_value) / last_week_value) * 100
                    delta_symbol = "▲" if delta > 0 else "▼"
                    delta_color = "#00ff88" if delta > 0 else "#ff4444"
                    delta_text = f"{delta_symbol} {abs(delta):.1f}% vs same hour last week"
                elif last_week_value is not None and last_week_value == 0 and latest_value == 0:
                    delta_text = "Same as last week (0 MW)"
                    delta_color = "#4a6080"
                elif last_week_value is not None and last_week_value == 0:
                    delta_text = "↑ vs 0 MW same hour last week"
                    delta_color = "#4a6080"
                else:
                    delta_text = "No data from last week yet"
                    delta_color = "#4a6080"

                st.markdown(f"""
                    <div class="metric-card">
                        <div class="metric-label">{LABELS[metric]}</div>
                        <div class="metric-value {css_class}">
                            {latest_value:,.0f} <span style="font-size:14px">MW</span>
                        </div>
                        <div class="metric-unit">Latest reading</div>
                        <div style="font-size:11px;margin-top:6px;color:{delta_color}">
                            {delta_text}
                        </div>
                    </div>
                """, unsafe_allow_html=True)
            else:
                st.markdown(f"""
                    <div class="metric-card">
                        <div class="metric-label">{LABELS.get(metric, metric)}</div>
                        <div class="metric-value" style="color:#4a6080">No data</div>
                    </div>
                """, unsafe_allow_html=True)


def render_energy_chart(data: dict) -> None:
    """
    Render the main hourly energy chart with all three metrics.
    Anomaly points are overlaid as red markers on each metric line.

    Parameters:
        data: Dictionary mapping metric name to its DataFrame

    Returns:
        None
    """
    st.markdown('<div class="section-title">Energy mix — hourly</div>',
                unsafe_allow_html=True)

    fig = go.Figure()

    # Load anomalies for all metrics once
    anomalies_df = load_anomalies(limit=500)

    for metric, df in data.items():
        if df is None or df.empty:
            continue

        # Draw the main line for this metric
        fig.add_trace(go.Scatter(
            x=df["datetime"],
            y=df["value_mw"],
            mode="lines",
            name=LABELS[metric],
            line=dict(
                color=COLORS[metric],
                width=2,
            ),
            hovertemplate=(
                f"<b>{LABELS[metric]}</b><br>"
                "Time: %{x}<br>"
                "Value: %{y:,.0f} MW<br>"
                "<extra></extra>"
            )
        ))

        # Overlay anomaly markers for this metric
        if anomalies_df is not None and not anomalies_df.empty:
            metric_anomalies = anomalies_df[anomalies_df["metric"] == metric]

            if not metric_anomalies.empty:
                fig.add_trace(go.Scatter(
                    x=metric_anomalies["datetime"],
                    y=metric_anomalies["value_mw"],
                    mode="markers",
                    name=f"{LABELS[metric]} anomaly",
                    marker=dict(
                        color="#ff4444",
                        size=10,
                        symbol="circle",
                        line=dict(color="#ffffff", width=1),
                    ),
                    hovertemplate=(
                        f"<b>⚠ ANOMALY — {LABELS[metric]}</b><br>"
                        "Time: %{x}<br>"
                        "Value: %{y:,.0f} MW<br>"
                        "<extra></extra>"
                    )
                ))

    fig.update_layout(
        paper_bgcolor="#0a0e1a",
        plot_bgcolor="#0d1424",
        font=dict(color="#e0e8ff", family="Courier New"),
        xaxis=dict(
            gridcolor="#1a2540",
            color="#e0e8ff",
            showgrid=True,
            tickfont=dict(color="#e0e8ff")
        ),
        yaxis=dict(
            gridcolor="#1a2540",
            color="#e0e8ff",
            showgrid=True,
            title="MW",
            tickfont=dict(color="#e0e8ff")
        ),
        legend=dict(
            bgcolor="#0d1424",
            bordercolor="#1a2540",
            borderwidth=1,
            font=dict(color="#e0e8ff"),
        ),
        hovermode="x unified",
        margin=dict(l=0, r=0, t=10, b=0),
        height=350,
    )

    st.plotly_chart(fig, use_container_width=True)


def render_data_summary(data: dict) -> None:
    """
    Render the data summary cards with stats and CSV export links.

    Parameters:
        data: Dictionary mapping metric name to its DataFrame

    Returns:
        None
    """
    st.markdown('<div class="section-title">Data summary</div>',
                unsafe_allow_html=True)

    col1, col2, col3 = st.columns(3)

    for col, metric in zip([col1, col2, col3],
                           ["wind_onshore", "solar", "consumption"]):
        with col:
            if metric in data:
                df = data[metric]
                st.markdown(f"""
                    <div class="metric-card">
                        <div class="metric-label;color:"#e0e8ff">{LABELS[metric]}</div>
                        <div style="font-size:12px;color:#e0e8ff;
                                    margin-top:8px;line-height:1.8;">
                            Rows: {len(df):,}<br>
                            From: {pd.to_datetime(df['datetime'].iloc[0]).strftime('%b %d %H:%M')}<br>
                            To: {pd.to_datetime(df['datetime'].iloc[-1]).strftime('%b %d %H:%M')}<br>
                            Min: {df['value_mw'].min():,.0f} MW<br>
                            Max: {df['value_mw'].max():,.0f} MW<br>
                            Avg: {df['value_mw'].mean():,.0f} MW
                        </div>
                    </div>
                    {get_download_link(df, metric)}
                """, unsafe_allow_html=True)


def render_ai_summary() -> None:
    """
    Render the AI-generated daily energy summary panel.

    Returns:
        None
    """
    st.markdown('<div class="section-title">AI Daily Summary</div>',
                unsafe_allow_html=True)

    summary = load_latest_summary()

    if summary is None:
        st.markdown("""
            <div class="metric-card">
                <div style="color:#4a6080;font-size:13px;text-align:center;padding:12px;">
                    No summary generated yet. The AI summary runs daily at 08:00.
                </div>
            </div>
        """, unsafe_allow_html=True)
        return

    st.markdown(f"""
        <div class="metric-card" style="text-align:left;padding:20px;">
            <div style="font-size:14px;color:#e0e8ff;line-height:1.8;">
                {summary['summary_text']}
            </div>
            <div style="font-size:10px;color:#4a6080;margin-top:12px;">
                Generated: {summary['generated_at']} ·
                Data date: {summary['summary_date']}
            </div>
        </div>
    """, unsafe_allow_html=True)

def render_anomaly_log() -> None:
    """
    Render a table of all detected anomalies from the database.

    Returns:
        None
    """
    st.markdown('<div class="section-title">Anomaly Log</div>',
                unsafe_allow_html=True)

    anomalies_df = load_anomalies(limit=100)

    if anomalies_df is None or anomalies_df.empty:
        st.markdown("""
            <div class="metric-card">
                <div style="color:#4a6080;font-size:13px;text-align:center;padding:12px;">
                    No anomalies detected yet. The system is monitoring continuously.
                </div>
            </div>
        """, unsafe_allow_html=True)
        return

    display_df = anomalies_df.copy()
    display_df = display_df.rename(columns={
        "datetime":    "Time",
        "metric":      "Metric",
        "value_mw":    "Value (MW)",
        "mean_mw":     "Baseline Mean (MW)",
        "std_mw":      "Std Dev (MW)",
        "detected_at": "Detected At",
    })

    display_df["Metric"] = display_df["Metric"].map({
        "wind_onshore": "Wind Onshore",
        "solar":        "Solar",
        "consumption":  "Consumption",
    })

    for col in ["Value (MW)", "Baseline Mean (MW)", "Std Dev (MW)"]:
        display_df[col] = display_df[col].round(1)

    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
    )

    st.markdown(
        get_download_link(anomalies_df, "anomalies"),
        unsafe_allow_html=True
    )

def main() -> None:
    """
    Main function that runs the Streamlit dashboard.
    Initialises the database, loads data, and renders all panels.

    Parameters:
        None

    Returns:
        None
    """
    apply_dark_theme()
    init_db()
    start_scheduler()

    st.markdown("""
        <div style="margin-bottom:24px;">
            <div style="font-size:22px;font-weight:700;color:#e0e8ff;
                        letter-spacing:0.1em;text-transform:uppercase;
                        font-family:'Courier New',monospace;">
                ⚡ Brandenburg Energy Monitor
            </div>
            <div style="font-size:11px;color:#4a6080;
                        letter-spacing:0.08em;margin-top:4px;">
                Bundesnetzagentur SMARD · 50Hertz TSO · Local SQLite Database
            </div>
        </div>
    """, unsafe_allow_html=True)

    with st.spinner("Loading energy data..."):
        data = {}
        for metric in ["wind_onshore", "solar", "consumption"]:
            df = load_energy_data(metric)
            if df is not None and not df.empty:
                data[metric] = df

    if not data:
        st.error("No data found in database. Run database.py first to fetch and store data.")
        st.stop()

    render_metric_cards(data)

    st.markdown("<br>", unsafe_allow_html=True)

    render_energy_chart(data)

    st.markdown("<br>", unsafe_allow_html=True)

    render_data_summary(data)

    st.markdown("<br>", unsafe_allow_html=True)
    render_anomaly_log()

    st.markdown("<br>", unsafe_allow_html=True)

    st.markdown("""
        <div style="font-size:10px;color:#2a3550;text-align:center;
                    letter-spacing:0.08em;">
            DATA SOURCE: BUNDESNETZAGENTUR SMARD · CC BY 4.0 ·
            LOCAL DATABASE ONLY · NO PERSONAL DATA COLLECTED
        </div>
    """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    render_ai_summary()


if __name__ == "__main__":
    main()