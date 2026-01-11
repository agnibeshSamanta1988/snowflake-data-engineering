# Import Python packages
import streamlit as st
import altair as alt
from snowflake.snowpark.context import get_active_session

# --------------------------------
# Page Configuration
# --------------------------------
st.set_page_config(
    page_title="US Inflation & Wage Trends",
    page_icon="📈",
    layout="wide"
)

# --------------------------------
# Snowflake Session
# --------------------------------
session = get_active_session()

monthly_cpi_table = session.table("wages_cpi.data.monthly_cpi_usa")
annual_wages_cpi_table = session.table("wages_cpi.data.annual_wages_cpi_usa")

monthly_cpi_data = monthly_cpi_table.to_pandas()
annual_wages_cpi_data = annual_wages_cpi_table.to_pandas()

# --------------------------------
# Header
# --------------------------------
st.title("📊 Inflation vs Wages in the United States")
st.markdown(
    """
    This dashboard explores how **consumer prices** and **wages** have evolved over time, 
    highlighting whether income growth has kept pace with inflation.
    """
)

# --------------------------------
# Key Metrics (Insights at a Glance)
# --------------------------------
latest_cpi = monthly_cpi_data.sort_values("MONTH").iloc[-1]["AVG_CPI"]
cpi_start = monthly_cpi_data.sort_values("MONTH").iloc[0]["AVG_CPI"]
cpi_growth_pct = ((latest_cpi - cpi_start) / cpi_start) * 100

wage_start = annual_wages_cpi_data.sort_values("YEAR").iloc[0]["AVG_ANNUAL_WAGES"]
wage_latest = annual_wages_cpi_data.sort_values("YEAR").iloc[-1]["AVG_ANNUAL_WAGES"]
wage_growth_pct = ((wage_latest - wage_start) / wage_start) * 100

col1, col2, col3, col4 = st.columns(4)

col1.metric("Latest CPI", f"{latest_cpi:.1f}")
col2.metric("CPI Growth (2021–2024)", f"{cpi_growth_pct:.1f}%")
col3.metric("Wage Growth (2012–2022)", f"{wage_growth_pct:.1f}%")
col4.metric(
    "Wage vs Inflation Gap",
    f"{(wage_growth_pct - cpi_growth_pct):.1f}%",
    help="Positive means wages grew faster than inflation"
)

st.divider()

# --------------------------------
# Monthly CPI Trend
# --------------------------------
st.subheader("📈 Monthly Consumer Price Index (CPI)")
st.caption("June 2021 – April 2024")

cpi_chart = (
    alt.Chart(monthly_cpi_data)
    .mark_line(color="#2C7BE5", strokeWidth=3)
    .encode(
        x=alt.X("MONTH:T", title="Month"),
        y=alt.Y("AVG_CPI:Q", title="Average CPI"),
        tooltip=[
            alt.Tooltip("MONTH:T", title="Month"),
            alt.Tooltip("AVG_CPI:Q", title="CPI", format=".1f")
        ],
    )
    .properties(height=350)
)

st.altair_chart(cpi_chart, use_container_width=True)

st.markdown(
    """
    **Insight:**  
    CPI rose sharply after 2021, reflecting sustained inflationary pressure driven by 
    supply chain disruptions, energy costs, and demand recovery after the pandemic.
    """
)

st.divider()

# --------------------------------
# Wages vs CPI (Dual Axis)
# --------------------------------
st.subheader("💰 Wages vs Inflation")
st.caption("Annual comparison, 2012 – 2022")

base = alt.Chart(annual_wages_cpi_data).encode(x="YEAR:T")

wages_line = base.mark_line(
    color="#2C7BE5", strokeWidth=3
).encode(
    y=alt.Y(
        "AVG_ANNUAL_WAGES:Q",
        title="Average Annual Wages (USD)",
        axis=alt.Axis(titleColor="#2C7BE5"),
    ),
    tooltip=[
        alt.Tooltip("YEAR:T", title="Year"),
        alt.Tooltip("AVG_ANNUAL_WAGES:Q", title="Wages", format=",.0f")
    ]
)

cpi_line = base.mark_line(
    color="#E5533D", strokeDash=[5, 3], strokeWidth=3
).encode(
    y=alt.Y(
        "CPI:Q",
        title="CPI",
        axis=alt.Axis(titleColor="#E5533D"),
    ),
    tooltip=[
        alt.Tooltip("YEAR:T", title="Year"),
        alt.Tooltip("CPI:Q", title="CPI", format=".1f")
    ]
)

combined_chart = (
    alt.layer(wages_line, cpi_line)
    .resolve_scale(y="independent")
    .properties(height=400)
)

st.altair_chart(combined_chart, use_container_width=True)

st.markdown(
    """
    **Insight:**  
    While wages have steadily increased over the last decade, CPI growth accelerated more rapidly 
    in recent years—suggesting that **purchasing power has been under pressure**, especially post-2020.
    """
)

# --------------------------------
# Footer
# --------------------------------
st.caption("Data source: Snowflake | Visualization: Streamlit + Altair")
