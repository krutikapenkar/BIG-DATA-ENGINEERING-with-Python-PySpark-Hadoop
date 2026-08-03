"""
BI dashboard for the Gold-layer exports produced by
exports/08_export_gold_for_bi.py.

This is NOT part of the Spark pipeline - it's a plain Streamlit app that
reads the CSVs the pipeline already wrote to data/exports/*_csv/ and
renders them as charts. Run the pipeline (through the export step) first,
then launch this.

Run with: streamlit run dashboard/app.py
"""

import glob
import os

import pandas as pd
import plotly.express as px
import streamlit as st

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EXPORTS_DIR = os.path.join(PROJECT_ROOT, "data", "exports")

st.set_page_config(page_title="E-Commerce Analytics", layout="wide")


@st.cache_data
def load_table(table_name: str) -> pd.DataFrame:
    """Spark's coalesce(1).write.csv() drops one part-*.csv file inside a
    folder named after the table - glob for it since the exact filename
    is randomly generated."""
    pattern = os.path.join(EXPORTS_DIR, f"{table_name}_csv", "part-*.csv")
    files = glob.glob(pattern)
    if not files:
        return pd.DataFrame()
    return pd.read_csv(files[0])


st.title("E-Commerce Customer Behaviour & Revenue Analytics")

customer_360 = load_table("customer_360")
category_rollup = load_table("daily_category_rollup")
revenue_leakage = load_table("revenue_leakage_daily")
kmeans_segments = load_table("customer_segments_kmeans")

if customer_360.empty and category_rollup.empty and revenue_leakage.empty and kmeans_segments.empty:
    st.error(
        f"No exported data found under `{EXPORTS_DIR}`.\n\n"
        "Run the pipeline first, ending with:\n\n"
        "```\npython exports/08_export_gold_for_bi.py\n```"
    )
    st.stop()

# ---- KPI row --------------------------------------------------------------
col1, col2, col3, col4 = st.columns(4)
col1.metric("Customers", f"{len(customer_360):,}")
col2.metric("Total Revenue", f"${customer_360['monetary'].sum():,.0f}" if not customer_360.empty else "-")
col3.metric(
    "Revenue Lost (leakage)",
    f"${revenue_leakage['estimated_revenue_lost'].sum():,.0f}" if not revenue_leakage.empty else "-",
)
col4.metric("Champions", f"{(customer_360['rfm_segment'] == 'Champions').sum():,}" if not customer_360.empty else "-")

tab1, tab2, tab3, tab4 = st.tabs(
    ["RFM Segments", "Revenue by Category", "Revenue Leakage", "K-Means Segments"]
)

with tab1:
    st.subheader("Customer distribution by RFM segment")
    if not customer_360.empty:
        seg_counts = customer_360["rfm_segment"].value_counts().reset_index()
        seg_counts.columns = ["rfm_segment", "count"]
        fig = px.bar(seg_counts, x="rfm_segment", y="count", color="rfm_segment")
        st.plotly_chart(fig, width='stretch')

        st.subheader("Customer 360 (filterable)")
        segment_filter = st.multiselect(
            "Filter by segment", options=sorted(customer_360["rfm_segment"].unique())
        )
        filtered = (
            customer_360[customer_360["rfm_segment"].isin(segment_filter)]
            if segment_filter
            else customer_360
        )
        st.dataframe(filtered, width='stretch')
    else:
        st.info("customer_360 export not found.")

with tab2:
    st.subheader("Daily revenue by product category")
    if not category_rollup.empty:
        category_rollup["order_date"] = pd.to_datetime(category_rollup["order_date"])
        daily_by_cat = category_rollup.groupby(["order_date", "category"], as_index=False)["revenue"].sum()
        fig = px.line(daily_by_cat, x="order_date", y="revenue", color="category")
        st.plotly_chart(fig, width='stretch')

        st.subheader("Total revenue by category")
        totals = category_rollup.groupby("category", as_index=False)["revenue"].sum().sort_values(
            "revenue", ascending=False
        )
        fig2 = px.bar(totals, x="category", y="revenue")
        st.plotly_chart(fig2, width='stretch')
    else:
        st.info("daily_category_rollup export not found.")

with tab3:
    st.subheader("Revenue leakage by type")
    if not revenue_leakage.empty:
        totals = revenue_leakage.groupby("leakage_type", as_index=False)[
            ["num_incidents", "estimated_revenue_lost"]
        ].sum()
        fig = px.bar(totals, x="leakage_type", y="estimated_revenue_lost", color="leakage_type")
        st.plotly_chart(fig, width='stretch')

        revenue_leakage["event_date"] = pd.to_datetime(revenue_leakage["event_date"])
        trend = revenue_leakage.groupby(["event_date", "leakage_type"], as_index=False)[
            "estimated_revenue_lost"
        ].sum()
        fig2 = px.line(trend, x="event_date", y="estimated_revenue_lost", color="leakage_type")
        st.plotly_chart(fig2, width='stretch')
    else:
        st.info("revenue_leakage_daily export not found.")

with tab4:
    st.subheader("K-Means customer segments (vs. RFM, side by side)")
    if not kmeans_segments.empty:
        seg_counts = kmeans_segments["segment_label"].value_counts().reset_index()
        seg_counts.columns = ["segment_label", "count"]
        fig = px.bar(seg_counts, x="segment_label", y="count", color="segment_label")
        st.plotly_chart(fig, width='stretch')

        fig2 = px.scatter(
            kmeans_segments,
            x="frequency",
            y="monetary",
            color="segment_label",
            hover_data=["user_id", "recency_days", "cart_abandonment_rate"],
        )
        st.plotly_chart(fig2, width='stretch')
        st.dataframe(kmeans_segments, width='stretch')
    else:
        st.info("customer_segments_kmeans export not found.")
