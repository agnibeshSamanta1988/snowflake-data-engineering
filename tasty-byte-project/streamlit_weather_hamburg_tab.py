# ====================================================
# Imports
# ====================================================
import streamlit as st
import altair as alt
import pandas as pd
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col

# ====================================================
# REQUIRED FIX: Allow multiple Altair charts
# ====================================================
alt.data_transformers.disable_max_rows()

# ====================================================
# Page Configuration
# ====================================================
st.set_page_config(
    page_title="Hamburg Weather & Sales Analytics",
    layout="wide"
)

# ====================================================
# Snowflake Session
# ====================================================
session = get_active_session()

# ====================================================
# App Title
# ====================================================
st.title("🌦️ Weather and Sales Trends for Hamburg, Germany")

# ====================================================
# Load Data from Snowflake
# ====================================================
hamburg_weather = session.table(
    "TASTY_BYTES.HARMONIZED.WEATHER_HAMBURG"
).select(
    col("DATE"),
    col("DAILY_SALES"),
    col("AVG_TEMPERATURE_FAHRENHEIT"),
    col("AVG_PRECIPITATION_INCHES"),
    col("MAX_WIND_SPEED_100M_MPH")
).to_pandas()

hamburg_weather["DATE"] = pd.to_datetime(hamburg_weather["DATE"])

# ====================================================
# KPI Metrics
# ====================================================
st.subheader("📊 Key Metrics")

k1, k2, k3, k4 = st.columns(4)

k1.metric("Total Sales ($)", f"{hamburg_weather['DAILY_SALES'].sum():,.0f}")
k2.metric("Avg Temperature (°F)", f"{hamburg_weather['AVG_TEMPERATURE_FAHRENHEIT'].mean():.1f}")
k3.metric("Total Precipitation (in)", f"{hamburg_weather['AVG_PRECIPITATION_INCHES'].sum():.1f}")
k4.metric("Max Wind Speed (mph)", f"{hamburg_weather['MAX_WIND_SPEED_100M_MPH'].max():.1f}")

# ====================================================
# Tabs for Performance
# ====================================================
tab1, tab2, tab3, tab4 = st.tabs([
    "📈 Trends",
    "🌡️ Weather Impact",
    "🗓️ Monthly View",
    "🎛️ Explorer"
])

# ====================================================
# TAB 1 — Daily Trends (Multi-Line)
# ====================================================
with tab1:
    st.subheader("Daily Sales & Weather Trends")

    long_df = hamburg_weather.melt(
        id_vars="DATE",
        var_name="Measure",
        value_name="Value"
    )

    long_df["Measure"] = long_df["Measure"].replace({
        "DAILY_SALES": "Daily Sales ($)",
        "AVG_TEMPERATURE_FAHRENHEIT": "Avg Temperature (°F)",
        "AVG_PRECIPITATION_INCHES": "Avg Precipitation (in)",
        "MAX_WIND_SPEED_100M_MPH": "Max Wind Speed (mph)"
    })

    line_chart = alt.Chart(long_df).mark_line(point=True).encode(
        x=alt.X("DATE:T", title="Date"),
        y=alt.Y("Value:Q", title="Value"),
        color=alt.Color(
            "Measure:N",
            scale=alt.Scale(
                range=["#29B5E8", "#FF6F61", "#0072CE", "#FFC300"]
            ),
            title="Legend"
        ),
        tooltip=["DATE:T", "Measure:N", "Value:Q"]
    ).interactive().properties(height=400)

    st.altair_chart(line_chart, use_container_width=True)

# ====================================================
# TAB 2 — Weather Impact + Regression
# ====================================================
with tab2:
    st.subheader("Sales vs Temperature (with Trend Line)")

    points_temp = alt.Chart(hamburg_weather).mark_circle(size=70).encode(
        x=alt.X("AVG_TEMPERATURE_FAHRENHEIT:Q", title="Avg Temperature (°F)"),
        y=alt.Y("DAILY_SALES:Q", title="Daily Sales ($)"),
        tooltip=["DATE:T", "DAILY_SALES:Q", "AVG_TEMPERATURE_FAHRENHEIT:Q"],
        color=alt.value("#FF6F61")
    )

    regression_temp = points_temp.transform_regression(
        "AVG_TEMPERATURE_FAHRENHEIT",
        "DAILY_SALES"
    ).mark_line(color="black")

    st.altair_chart(
        (points_temp + regression_temp).interactive().properties(height=350),
        use_container_width=True
    )

    st.subheader("Sales vs Precipitation (with Trend Line)")

    points_rain = alt.Chart(hamburg_weather).mark_circle(size=70).encode(
        x=alt.X("AVG_PRECIPITATION_INCHES:Q", title="Avg Precipitation (in)"),
        y=alt.Y("DAILY_SALES:Q", title="Daily Sales ($)"),
        tooltip=["DATE:T", "DAILY_SALES:Q", "AVG_PRECIPITATION_INCHES:Q"],
        color=alt.value("#0072CE")
    )

    regression_rain = points_rain.transform_regression(
        "AVG_PRECIPITATION_INCHES",
        "DAILY_SALES"
    ).mark_line(color="black")

    st.altair_chart(
        (points_rain + regression_rain).interactive().properties(height=350),
        use_container_width=True
    )

# ====================================================
# TAB 3 — Monthly Aggregation
# ====================================================
with tab3:
    st.subheader("Monthly Sales Trend")

    hamburg_weather["MONTH"] = hamburg_weather["DATE"].dt.to_period("M").astype(str)

    monthly_sales = hamburg_weather.groupby(
        "MONTH",
        as_index=False
    )["DAILY_SALES"].sum()

    monthly_chart = alt.Chart(monthly_sales).mark_bar().encode(
        x=alt.X("MONTH:N", title="Month"),
        y=alt.Y("DAILY_SALES:Q", title="Total Sales ($)"),
        tooltip=["MONTH:N", "DAILY_SALES:Q"],
        color=alt.value("#29B5E8")
    ).properties(height=350)

    st.altair_chart(monthly_chart, use_container_width=True)

    st.subheader("Temperature Distribution")

    temp_hist = alt.Chart(hamburg_weather).mark_bar().encode(
        x=alt.X(
            "AVG_TEMPERATURE_FAHRENHEIT:Q",
            bin=alt.Bin(maxbins=20),
            title="Avg Temperature (°F)"
        ),
        y=alt.Y("count()", title="Number of Days"),
        tooltip=["count()"]
    ).properties(height=300)

    st.altair_chart(temp_hist, use_container_width=True)

# ====================================================
# TAB 4 — Interactive Measure Explorer
# ====================================================
with tab4:
    st.subheader("Interactive Measure Explorer")

    selected_measure = st.selectbox(
        "Select a Measure",
        [
            "DAILY_SALES",
            "AVG_TEMPERATURE_FAHRENHEIT",
            "AVG_PRECIPITATION_INCHES",
            "MAX_WIND_SPEED_100M_MPH"
        ]
    )

    explorer_chart = alt.Chart(hamburg_weather).mark_line(point=True).encode(
        x=alt.X("DATE:T", title="Date"),
        y=alt.Y(
            f"{selected_measure}:Q",
            title=selected_measure.replace("_", " ").title()
        ),
        tooltip=["DATE:T", f"{selected_measure}:Q"],
        color=alt.value("#29B5E8")
    ).interactive().properties(height=350)

    st.altair_chart(explorer_chart, use_container_width=True)
