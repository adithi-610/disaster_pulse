
import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path

st.set_page_config(
    page_title="Disaster Pulse | Fire Intelligence",
    page_icon="🔥",
    layout="wide"
)

BASE_DIR = Path(__file__).resolve().parent
DATA_PATH = BASE_DIR / "data" / "processed" / "clean_regional.csv"

st.title("🔥 Disaster Pulse")
st.subheader("Satellite-Based Wildfire Detection and Regional Analysis")
st.caption("Analysis of NASA FIRMS satellite fire-detection observations filtered to the India bounding box.")

@st.cache_data
def load_data(path):
    df = pd.read_csv(path)

    required = ["latitude", "longitude", "acq_date"]
    missing = [column for column in required if column not in df.columns]

    if missing:
        raise ValueError(f"Required columns are missing: {missing}")

    df["acq_date"] = pd.to_datetime(df["acq_date"], errors="coerce")

    for column in ["latitude", "longitude", "brightness", "frp"]:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")

    return df

try:
    df = load_data(DATA_PATH)
except Exception as error:
    st.error(f"Could not load the dataset: {error}")
    st.stop()

st.sidebar.header("Dashboard Filters")

filtered = df.copy()

if "acq_date" in filtered.columns:
    min_date = filtered["acq_date"].min()
    max_date = filtered["acq_date"].max()

    if pd.notna(min_date) and pd.notna(max_date):
        selected_dates = st.sidebar.date_input(
            "Acquisition date range",
            value=(min_date.date(), max_date.date()),
            min_value=min_date.date(),
            max_value=max_date.date()
        )

        if isinstance(selected_dates, (tuple, list)) and len(selected_dates) == 2:
            start_date, end_date = selected_dates
            filtered = filtered[
                filtered["acq_date"].dt.date.between(start_date, end_date)
            ]

if "satellite" in filtered.columns:
    satellites = sorted(filtered["satellite"].dropna().astype(str).unique())
    selected_satellites = st.sidebar.multiselect(
        "Satellite",
        options=satellites,
        default=satellites
    )
    filtered = filtered[
        filtered["satellite"].astype(str).isin(selected_satellites)
    ]

if "confidence" in filtered.columns:
    confidence_values = sorted(
        filtered["confidence"].dropna().astype(str).unique()
    )
    selected_confidence = st.sidebar.multiselect(
        "Confidence",
        options=confidence_values,
        default=confidence_values
    )
    filtered = filtered[
        filtered["confidence"].astype(str).isin(selected_confidence)
    ]

st.sidebar.caption("Filters apply to the charts and map below.")

total_records = len(filtered)
satellite_count = (
    filtered["satellite"].nunique()
    if "satellite" in filtered.columns else 0
)
average_frp = (
    filtered["frp"].mean()
    if "frp" in filtered.columns and total_records else 0
)
high_confidence_count = (
    filtered["confidence"].astype(str).str.lower().isin(["h", "high"]).sum()
    if "confidence" in filtered.columns else None
)

c1, c2, c3, c4 = st.columns(4)
c1.metric("Fire detections", f"{total_records:,}")
c2.metric("Satellites represented", satellite_count)
c3.metric("Average FRP", f"{average_frp:.2f} MW" if pd.notna(average_frp) else "N/A")

if high_confidence_count is not None:
    c4.metric("High-confidence detections", f"{high_confidence_count:,}")
else:
    c4.metric("High-confidence detections", "N/A")

st.divider()

left, right = st.columns(2)

with left:
    st.subheader("Fire Detections Over Time")

    if total_records and "acq_date" in filtered.columns:
        daily = (
            filtered.dropna(subset=["acq_date"])
            .groupby(filtered["acq_date"].dt.to_period("M"))
            .size()
            .reset_index(name="detections")
        )
        daily["month"] = daily["acq_date"].astype(str)

        fig = px.line(
            daily,
            x="month",
            y="detections",
            markers=True,
            title="Monthly Detection Count"
        )
        fig.update_layout(xaxis_title="Month", yaxis_title="Detections")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No date data is available for the selected filters.")

with right:
    st.subheader("Detections by Satellite")

    if total_records and "satellite" in filtered.columns:
        satellite_counts = (
            filtered["satellite"].fillna("Unknown")
            .value_counts()
            .rename_axis("Satellite")
            .reset_index(name="Detections")
        )

        fig = px.bar(
            satellite_counts,
            x="Satellite",
            y="Detections",
            color="Satellite",
            title="Observations by Satellite"
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No satellite data is available for the selected filters.")

left, right = st.columns(2)

with left:
    st.subheader("Fire Radiative Power")

    if total_records and "frp" in filtered.columns:
        frp_data = filtered.dropna(subset=["frp"])

        if not frp_data.empty:
            fig = px.histogram(
                frp_data,
                x="frp",
                nbins=40,
                title="Distribution of Fire Radiative Power",
                labels={"frp": "FRP (MW)"}
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No FRP values are available for the selected filters.")
    else:
        st.info("FRP data is unavailable.")

with right:
    st.subheader("Confidence Distribution")

    if total_records and "confidence" in filtered.columns:
        confidence_counts = (
            filtered["confidence"].fillna("Unknown")
            .value_counts()
            .rename_axis("Confidence")
            .reset_index(name="Detections")
        )

        fig = px.pie(
            confidence_counts,
            names="Confidence",
            values="Detections",
            hole=0.4,
            title="Detection Confidence Categories"
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Confidence data is unavailable.")

st.divider()
st.subheader("🗺️ Fire Detection Map")
st.caption("Showing a sample of up to 5,000 observations to keep the dashboard responsive.")

map_columns = ["latitude", "longitude"]
if all(column in filtered.columns for column in map_columns):
    map_data = filtered.dropna(subset=map_columns)

    if not map_data.empty:
        if len(map_data) > 5000:
            map_data = map_data.sample(5000, random_state=42)

        hover_columns = [
            column for column in
            ["acq_date", "satellite", "confidence", "frp", "brightness"]
            if column in map_data.columns
        ]

        fig = px.scatter_mapbox(
            map_data,
            lat="latitude",
            lon="longitude",
            color="frp" if "frp" in map_data.columns else None,
            hover_data=hover_columns,
            zoom=3,
            height=550,
            title="Geographic Distribution of Fire Detections",
            mapbox_style="open-street-map"
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No valid coordinates are available.")
else:
    st.warning("Latitude and longitude columns are required for the map.")

with st.expander("View filtered data"):
    st.write(f"Showing {min(100, len(filtered)):,} of {len(filtered):,} filtered records.")
    st.dataframe(filtered.head(100), use_container_width=True)

st.caption(
    "Academic project | Data source: NASA FIRMS near-real-time fire detections | "
    "A satellite detection is not necessarily a unique wildfire incident."
)
