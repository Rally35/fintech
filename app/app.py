"""
Streamlit Frontend for FinTech Fundamental Analysis
Main entry point: streamlit run app.py
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import logging
import sys
import os

# Add utils to path to ensure modules are found
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.db import DatabaseConnection
from utils.metrics import MetricsCalculator
from utils.logger import setup_logger
from utils.cache import cached_query

# ============================================================
# CONFIGURATION
# ============================================================

logger = setup_logger(__name__)

st.set_page_config(
    page_title="FinTech Analysis - GPW",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ============================================================
# SESSION STATE
# ============================================================

if 'db_connection' not in st.session_state:
    try:
        st.session_state.db_connection = DatabaseConnection()
    except Exception as e:
        st.error(f"Critical Error: Database connection failed. {e}")
        st.stop()

db = st.session_state.db_connection

# ============================================================
# UI: SIDEBAR
# ============================================================

st.sidebar.title("📊 FinTech Analysis")
st.sidebar.write("Fundamental analysis tool for GPW stocks")

# Refresh data button
if st.sidebar.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()

# Last update info
try:
    last_update = db.get_last_price_update()
    if last_update:
        st.sidebar.caption(f"Last price update: {last_update}")
    else:
        st.sidebar.caption("No price data available.")
except Exception as e:
    logger.error(f"Error fetching update time: {e}")
    st.sidebar.warning("Could not fetch status.")

# Sidebar divider
st.sidebar.divider()

# ============================================================
# UI: MAIN CONTENT
# ============================================================

st.title("📈 Fundamental Analysis - GPW")
st.markdown("---")

# ============================================================
# SECTION 1: COMPANY SELECTOR
# ============================================================

st.subheader("1. Select Company")

try:
    companies = db.get_all_companies()
    if not companies:
        st.error("❌ No companies found in database. Please import data first.")
        st.stop()
    
    company_options = {f"{row['ticker']} - {row['name']}": row['ticker'] 
                       for row in companies}
    selected_company_display = st.selectbox(
        "Choose company:",
        options=company_options.keys(),
        help="Select a company to analyze"
    )
    selected_ticker = company_options[selected_company_display]
    
except Exception as e:
    st.error(f"❌ Error loading companies: {str(e)}")
    logger.error(f"Error loading companies: {e}")
    st.stop()

# ============================================================
# SECTION 2: LIVE METRICS
# ============================================================

st.markdown("---")
st.subheader("2. Live Metrics")

try:
    # Fetch latest price and fundamentals
    latest_price = db.get_latest_price(selected_ticker)
    latest_financials = db.get_latest_financials(selected_ticker)
    
    if not latest_price or not latest_financials:
        st.warning(f"⚠️ Incomplete data for {selected_ticker}. Price or Financials missing.")
    else:
        # Calculate metrics
        calculator = MetricsCalculator(latest_price, latest_financials)
        
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            prev_close = latest_price.get('open', latest_price['close']) # Fallback
            delta_val = ((latest_price['close'] - prev_close) / prev_close * 100) if prev_close else 0
            
            st.metric(
                "🎯 Price (PLN)",
                f"{latest_price['close']:.2f}",
                delta=f"{delta_val:.2f}%",
                help="Current stock price"
            )
        
        with col2:
            pe_ratio = calculator.calculate_pe_ratio()
            st.metric(
                "P/E Ratio",
                f"{pe_ratio:.2f}x" if pe_ratio else "N/A",
                help="Price-to-Earnings ratio"
            )
        
        with col3:
            roe = calculator.calculate_roe()
            st.metric(
                "ROE",
                f"{roe:.2f}%" if roe else "N/A",
                help="Return on Equity"
            )
        
        with col4:
            ebitda_margin = calculator.calculate_ebitda_margin()
            st.metric(
                "EBITDA Margin",
                f"{ebitda_margin:.2f}%" if ebitda_margin else "N/A",
                help="EBITDA margin %"
            )
        
        with col5:
            eps = calculator.calculate_eps()
            st.metric(
                "EPS",
                f"{eps:.2f}" if eps else "N/A",
                help="EPS"
            )

except Exception as e:
    st.error(f"❌ Error calculating metrics: {str(e)}")
    logger.error(f"Error calculating metrics: {e}")

# ============================================================
# SECTION 3: FINANCIAL STATEMENTS
# ============================================================

st.markdown("---")
st.subheader("3. Financial Statements (Last 8 Quarters)")

try:
    financials_df = db.get_financials_history(selected_ticker, quarters=8)
    
    if financials_df is None or len(financials_df) == 0:
        st.info("No financial data available")
    else:
        # Display as table
        display_cols = ['rok', 'kwartal', 'przychody', 'ebitda', 'zysk_netto', 
                       'aktywa_razem', 'kapital_wlasny', 'przeplywy_operacyjne']
        
        # Filter only existing columns
        valid_cols = [c for c in display_cols if c in financials_df.columns]
        df_display = financials_df[valid_cols].copy()
        
        # Format numbers
        for col in valid_cols[2:]: # Skip rok, kwartal
             df_display[col] = df_display[col].apply(
                lambda x: f"{x:,.0f}" if pd.notna(x) else "N/A"
            )
        
        # Rename for UI
        df_display.columns = [c.replace('_', ' ').title() for c in df_display.columns]
        
        st.dataframe(
            df_display,
            use_container_width=True,
            hide_index=True,
            height=300
        )

except Exception as e:
    st.error(f"❌ Error loading financials: {str(e)}")
    logger.error(f"Error loading financials: {e}")

# ============================================================
# SECTION 4: CHARTS
# ============================================================

st.markdown("---")
st.subheader("4. Trends & Analysis")

try:
    financials_df = db.get_financials_history(selected_ticker, quarters=16)
    
    if financials_df is not None and len(financials_df) > 0:
        col1, col2 = st.columns(2)
        
        # Chart 1: Revenue & EBITDA trend
        with col1:
            fig_revenue = px.line(
                financials_df,
                x='period', # Constructed in db query or needs construction here
                y=['przychody', 'ebitda'],
                title="Revenue & EBITDA Trend",
                labels={'przychody': 'Revenue', 'ebitda': 'EBITDA', 'period': 'Period'},
            )
            fig_revenue.update_layout(
                hovermode='x unified',
                height=400,
                template='plotly_dark'
            )
            st.plotly_chart(fig_revenue, use_container_width=True)
        
        # Chart 2: Net Income trend
        with col2:
            fig_income = px.bar(
                financials_df,
                x='period',
                y='zysk_netto',
                title="Net Income by Quarter",
                color='zysk_netto',
                color_continuous_scale='RdYlGn',
                labels={'period': 'Period', 'zysk_netto': 'Net Income'}
            )
            fig_income.update_layout(
                hovermode='x unified',
                height=400,
                template='plotly_dark',
                showlegend=False
            )
            st.plotly_chart(fig_income, use_container_width=True)

except Exception as e:
    st.error(f"❌ Error creating charts: {str(e)}")
    logger.error(f"Error creating charts: {e}")

# ============================================================
# SECTION 5: FOOTER
# ============================================================

st.markdown("---")
st.markdown("""
<div style='text-align: center; color: gray; font-size: 12px;'>
    <p>FinTech Analysis Tool | Data source: dane_finansowe.xlsx | Last updated: {}</p>
</div>
""".format(datetime.now().strftime("%Y-%m-%d %H:%M")), unsafe_allow_html=True)