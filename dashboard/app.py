"""
Streamlit dashboard — reads directly from DuckDB.
Run with: streamlit run dashboard/app.py
"""

import os
import sys
from pathlib import Path

import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).parent.parent
load_dotenv(dotenv_path=PROJECT_ROOT / ".env")

DB_PATH = os.getenv("DB_PATH", str(PROJECT_ROOT / "retail.db"))

# ── Page Config ────────────────────────────────────────────
st.set_page_config(
    page_title="Retail main_analytics",
    layout="wide",
    initial_sidebar_state="expanded"
)


# ── Query Helper ───────────────────────────────────────────
@st.cache_data(ttl=300)
def query(sql):
    """Run SQL against DuckDB and return DataFrame."""
    con = duckdb.connect(DB_PATH, read_only=True)
    df = con.execute(sql).df()
    con.close()
    return df


# ── Sidebar ────────────────────────────────────────────────
st.sidebar.title("Retail main_analytics")
st.sidebar.markdown("---")

years_df = query("SELECT DISTINCT purchase_year FROM main_analytics.fct_orders ORDER BY 1")
all_years = years_df["purchase_year"].tolist()
selected_years = st.sidebar.multiselect("Year", all_years, default=all_years)

states_df = query("SELECT DISTINCT customer_state FROM main_analytics.fct_orders ORDER BY 1")
all_states = states_df["customer_state"].tolist()
selected_states = st.sidebar.multiselect("State", all_states, default=all_states)

st.sidebar.markdown("---")
st.sidebar.markdown("Built with DuckDB + dbt + Streamlit")

# Build filter strings
year_filter  = f"purchase_year IN ({','.join(map(str, selected_years))})" if selected_years else "1=1"
state_filter = f"customer_state IN ({','.join([repr(s) for s in selected_states])})" if selected_states else "1=1"
base_filter  = f"{year_filter} AND {state_filter} AND order_status = 'delivered'"

# ── Page Navigation ────────────────────────────────────────
page = st.sidebar.radio("Navigate", ["Overview", "Delivery", "Products", "Customers"])


# ══════════════════════════════════════════════════════════
# PAGE 1: OVERVIEW
# ══════════════════════════════════════════════════════════
if page == "Overview":
    st.title("Executive Overview")

    kpis = query(f"""
        SELECT
            COUNT(*)                         AS total_orders,
            ROUND(SUM(total_revenue), 0)     AS total_revenue,
            ROUND(AVG(total_revenue), 2)     AS avg_order_value,
            ROUND(AVG(review_score), 2)      AS avg_review_score
        FROM main_analytics.fct_orders
        WHERE {base_filter}
    """)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Orders",     f"{int(kpis['total_orders'][0]):,}")
    c2.metric("Total Revenue",    f"R$ {int(kpis['total_revenue'][0]):,}")
    c3.metric("Avg Order Value",  f"R$ {kpis['avg_order_value'][0]:.2f}")
    c4.metric("Avg Review Score", f"{kpis['avg_review_score'][0]:.2f}")

    st.markdown("---")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Revenue Over Time")
        rev = query(f"""
            SELECT year_month, ROUND(SUM(total_revenue), 0) AS revenue
            FROM main_analytics.fct_orders
            WHERE {base_filter}
            GROUP BY year_month ORDER BY year_month
        """)
        fig = px.line(rev, x="year_month", y="revenue",
                      labels={"year_month": "Month", "revenue": "Revenue (R$)"},
                      markers=True)
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Order Status Breakdown")
        status = query(f"""
            SELECT order_status, COUNT(*) AS orders
            FROM main_analytics.fct_orders
            WHERE {year_filter} AND {state_filter}
            GROUP BY order_status ORDER BY orders DESC
        """)
        fig = px.pie(status, names="order_status", values="orders", hole=0.4)
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Revenue by State (Top 10)")
    state_rev = query(f"""
        SELECT customer_state AS state, ROUND(SUM(total_revenue), 0) AS revenue
        FROM main_analytics.fct_orders
        WHERE {base_filter}
        GROUP BY customer_state ORDER BY revenue DESC LIMIT 10
    """)
    fig = px.bar(state_rev, x="state", y="revenue",
                 labels={"revenue": "Revenue (R$)", "state": "State"},
                 color="revenue", color_continuous_scale="Blues")
    st.plotly_chart(fig, use_container_width=True)


# ══════════════════════════════════════════════════════════
# PAGE 2: DELIVERY
# ══════════════════════════════════════════════════════════
elif page == "Delivery":
    st.title("Delivery Performance")

    del_kpis = query(f"""
        SELECT
            ROUND(AVG(actual_delivery_days), 1) AS avg_days,
            COUNT(CASE WHEN delivery_status = 'On Time' THEN 1 END) * 100.0
                / COUNT(*) AS on_time_rate,
            COUNT(CASE WHEN delivery_status = 'Late' THEN 1 END) * 100.0
                / COUNT(*) AS late_rate
        FROM main_analytics.fct_orders
        WHERE {base_filter} AND actual_delivery_days IS NOT NULL
    """)

    c1, c2, c3 = st.columns(3)
    c1.metric("Avg Delivery Days", f"{del_kpis['avg_days'][0]} days")
    c2.metric("On-Time Rate",       f"{del_kpis['on_time_rate'][0]:.1f}%")
    c3.metric("Late Rate",          f"{del_kpis['late_rate'][0]:.1f}%")

    st.markdown("---")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("On-Time vs Late")
        split = query(f"""
            SELECT delivery_status, COUNT(*) AS orders
            FROM main_analytics.fct_orders
            WHERE {base_filter} AND delivery_status != 'Not Delivered'
            GROUP BY delivery_status
        """)
        fig = px.pie(split, names="delivery_status", values="orders",
                     color="delivery_status",
                     color_discrete_map={"On Time": "#2ecc71", "Late": "#e74c3c"})
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Avg Delivery Days by State")
        del_state = query(f"""
            SELECT customer_state AS state,
                   ROUND(AVG(actual_delivery_days), 1) AS avg_days
            FROM main_analytics.fct_orders
            WHERE {base_filter} AND actual_delivery_days IS NOT NULL
            GROUP BY customer_state ORDER BY avg_days DESC LIMIT 15
        """)
        fig = px.bar(del_state, x="avg_days", y="state", orientation="h",
                     labels={"avg_days": "Avg Days", "state": "State"},
                     color="avg_days", color_continuous_scale="Reds")
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Delivery Trend Over Time")
    trend = query(f"""
        SELECT year_month,
               ROUND(AVG(actual_delivery_days), 1) AS avg_days,
               ROUND(AVG(days_vs_estimate), 1) AS avg_vs_estimate
        FROM main_analytics.fct_orders
        WHERE {base_filter} AND actual_delivery_days IS NOT NULL
        GROUP BY year_month ORDER BY year_month
    """)
    fig = px.line(trend, x="year_month", y=["avg_days", "avg_vs_estimate"],
                  labels={"year_month": "Month", "value": "Days"}, markers=True)
    st.plotly_chart(fig, use_container_width=True)


# ══════════════════════════════════════════════════════════
# PAGE 3: PRODUCTS
# ══════════════════════════════════════════════════════════
elif page == "Products":
    st.title("Product Analysis")

    year_clause = f"EXTRACT('year' FROM purchased_at)::int IN ({','.join(map(str, selected_years))})" if selected_years else "1=1"

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Revenue by Category (Top 15)")
        cat_rev = query(f"""
            SELECT product_category,
                   ROUND(SUM(price), 0) AS revenue,
                   COUNT(*) AS items_sold
            FROM main_analytics.fct_order_items
            WHERE {year_clause}
              AND order_status = 'delivered'
              AND product_category IS NOT NULL
            GROUP BY product_category ORDER BY revenue DESC LIMIT 15
        """)
        fig = px.bar(cat_rev, x="revenue", y="product_category", orientation="h",
                     labels={"revenue": "Revenue (R$)", "product_category": ""},
                     color="revenue", color_continuous_scale="Viridis")
        fig.update_layout(yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Items Sold by Category (Top 15)")
        fig = px.bar(cat_rev, x="items_sold", y="product_category", orientation="h",
                     labels={"items_sold": "Items Sold", "product_category": ""},
                     color="items_sold", color_continuous_scale="Purples")
        fig.update_layout(yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Top 5 Categories Over Time")
    top5 = query("""
        SELECT product_category FROM main_analytics.fct_order_items
        WHERE order_status = 'delivered' AND product_category IS NOT NULL
        GROUP BY product_category ORDER BY SUM(price) DESC LIMIT 5
    """)["product_category"].tolist()

    if top5:
        cat_time = query(f"""
            SELECT year_month, product_category, ROUND(SUM(price), 0) AS revenue
            FROM main_analytics.fct_order_items
            WHERE order_status = 'delivered'
              AND product_category IN ({','.join([repr(c) for c in top5])})
            GROUP BY year_month, product_category ORDER BY year_month
        """)
        fig = px.line(cat_time, x="year_month", y="revenue", color="product_category",
                      labels={"year_month": "Month", "revenue": "Revenue (R$)"},
                      markers=True)
        st.plotly_chart(fig, use_container_width=True)


# ══════════════════════════════════════════════════════════
# PAGE 4: CUSTOMERS
# ══════════════════════════════════════════════════════════
elif page == "Customers":
    st.title("Customer Insights")

    cust_kpis = query("""
        SELECT
            COUNT(*)                                      AS total_customers,
            SUM(CASE WHEN is_repeat_customer THEN 1 ELSE 0 END) AS repeat_customers,
            ROUND(AVG(lifetime_value), 2)                 AS avg_ltv,
            ROUND(AVG(avg_review_score), 2)               AS avg_review
        FROM main_analytics.dim_customers
    """)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Customers",    f"{int(cust_kpis['total_customers'][0]):,}")
    c2.metric("Repeat Customers",   f"{int(cust_kpis['repeat_customers'][0]):,}")
    c3.metric("Avg Lifetime Value", f"R$ {cust_kpis['avg_ltv'][0]:.2f}")
    c4.metric("Avg Review Score",   f"{cust_kpis['avg_review'][0]:.2f}")

    st.markdown("---")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Customer Segments")
        segments = query("""
            SELECT customer_segment, COUNT(*) AS customers
            FROM main_analytics.dim_customers
            GROUP BY customer_segment
        """)
        fig = px.pie(segments, names="customer_segment", values="customers",
                     color="customer_segment",
                     color_discrete_map={
                         "High Value": "#f39c12",
                         "Mid Value":  "#3498db",
                         "Low Value":  "#95a5a6"
                     })
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Customers by State (Top 10)")
        st_cust = query("""
            SELECT state, COUNT(*) AS customers
            FROM main_analytics.dim_customers
            GROUP BY state ORDER BY customers DESC LIMIT 10
        """)
        fig = px.bar(st_cust, x="state", y="customers",
                     labels={"customers": "Customers", "state": "State"},
                     color="customers", color_continuous_scale="Blues")
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("New vs Repeat Customers")
    repeat = query("""
        SELECT CASE WHEN is_repeat_customer THEN 'Repeat' ELSE 'New' END AS type,
               COUNT(*) AS customers
        FROM main_analytics.dim_customers GROUP BY is_repeat_customer
    """)
    fig = px.bar(repeat, x="type", y="customers",
                 color="type",
                 color_discrete_map={"New": "#3498db", "Repeat": "#2ecc71"})
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Data Explorer")
    sample = query("""
        SELECT customer_unique_id, state, customer_segment,
               total_orders, lifetime_value, avg_review_score
        FROM main_analytics.dim_customers
        ORDER BY lifetime_value DESC LIMIT 100
    """)
    st.dataframe(sample, use_container_width=True)
