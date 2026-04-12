import pandas as pd
from datetime import datetime, timedelta
import streamlit as st
import plotly.express as px
from fpdf import FPDF
import json

from supabase import create_client, Client

# --- SUPABASE SETUP ---
URL = "https://cwjoayqbjlerbilbtdom.supabase.co"
KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImN3am9heXFiamxlcmJpbGJ0ZG9tIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzMzMDMzMDMsImV4cCI6MjA4ODg3OTMwM30.vXNhwjYRXrxh98qUzFMIUbvPMzNPjg7B-ves9Vmyveg"
supabase: Client = create_client(URL, KEY)

def init_db():
    pass

# --- DB HELPERS ---
def update_stock(category, item_type, unit, qty_change):
    if unit in UNIT_CONVERSIONS:
        qty_in_pati = convert_to_base_unit(qty_change, unit)
        store_unit = "pati"
    else:
        qty_in_pati = qty_change
        store_unit = unit

    res = supabase.table("stock_summary").select("current_stock")\
        .match({"category": category, "item_type": item_type, "unit": store_unit})\
        .execute()

    if res.data:
        new_total = res.data[0]['current_stock'] + qty_in_pati
        supabase.table("stock_summary").update({"current_stock": new_total})\
            .match({"category": category, "item_type": item_type, "unit": store_unit}).execute()
    else:
        supabase.table("stock_summary").insert({
            "category": category, "item_type": item_type,
            "unit": store_unit, "current_stock": qty_in_pati
        }).execute()

def insert_transaction(data):
    data_dict = {
        "date": data[0], "category": data[1], "item_type": data[2],
        "unit": data[3], "quantity": data[4], "rate": data[5],
        "amount": data[6], "transaction_type": data[7], "cash_credit": data[8],
        "party_name": data[9], "vehicle_name": data[10], "site_name": data[11], "remarks": data[12],
        "mobile_number": data[13],
        "is_deleted": False
    }
    supabase.table("transactions").insert(data_dict).execute()
    update_stock(data[1], data[2], data[3], data[4])

def insert_expense(data):
    data_dict = {"date": data[0], "expense_type": data[1], "amount": data[2], "remarks": data[3], "is_deleted": False}
    supabase.table("expenses").insert(data_dict).execute()

def insert_payment(data):
    data_dict = {"date": data[0], "party_name": data[1], "payment_type": data[2], "amount": data[3], "remarks": data[4], "is_deleted": False}
    supabase.table("payments").insert(data_dict).execute()

def delete_records(table, record_ids):
    for rid in record_ids:
        if table == "transactions":
            row = supabase.table("transactions").select("category, item_type, unit, quantity").eq("id", rid).execute()
            if row.data:
                item = row.data[0]
                update_stock(item['category'], item['item_type'], item['unit'], -item['quantity'])
        supabase.table(table).update({"is_deleted": True}).eq("id", rid).execute()

def restore_records(table, record_ids):
    for rid in record_ids:
        if table == "transactions":
            row = supabase.table("transactions").select("category, item_type, unit, quantity").eq("id", rid).execute()
            if row.data:
                item = row.data[0]
                update_stock(item['category'], item['item_type'], item['unit'], item['quantity'])
        supabase.table(table).update({"is_deleted": False}).eq("id", rid).execute()

def insert_pending_order(data):
    data_dict = {
        "date": data[0], "party_name": data[1], "site_name": data[2],
        "vehicle_name": data[3], "category": data[4], "item_type": data[5],
        "unit": data[6], "quantity": data[7], "rate": data[8],
        "amount": data[9], "cash_credit": data[10], "remarks": data[11],
        "status": "pending", "is_deleted": False
    }
    supabase.table("pending_orders").insert(data_dict).execute()

def load_pending_orders():
    response = supabase.table("pending_orders").select("*")\
        .eq("is_deleted", False).eq("status", "pending")\
        .order("id", desc=True).execute()
    df = pd.DataFrame(response.data)
    if df.empty:
        return pd.DataFrame(columns=["id", "date", "party_name", "site_name",
                                     "vehicle_name", "category", "item_type",
                                     "unit", "quantity", "rate", "amount",
                                     "cash_credit", "remarks", "status", "is_deleted"])
    df['date_dt'] = pd.to_datetime(df['date'], errors='coerce')
    return df

def complete_pending_order(order_id, vehicle_name):
    row = supabase.table("pending_orders").select("*").eq("id", order_id).execute()
    if row.data:
        order = row.data[0]
        now = datetime.now().strftime("%Y-%m-%d %H:%M")
        insert_transaction((
            now, order['category'], order['item_type'], order['unit'],
            -abs(order['quantity']),
            order['rate'], order['amount'], "sale",
            order['cash_credit'], order['party_name'],
            vehicle_name, order['site_name'], order['remarks'], ""
        ))
        supabase.table("pending_orders").update({"status": "completed"})\
            .eq("id", order_id).execute()

def delete_pending_order(order_id):
    supabase.table("pending_orders").update({"is_deleted": True})\
        .eq("id", order_id).execute()

def load_deleted_records(table):
    return supabase.table(table).select("*").eq("is_deleted", True).order("id", desc=True).execute().data

def load_transactions():
    response = supabase.table("transactions").select("*").eq("is_deleted", False).order("id", desc=True).execute()
    df = pd.DataFrame(response.data)
    if df.empty:
        return pd.DataFrame(columns=["id", "date", "date_dt", "category", "item_type", "unit",
                                     "quantity", "rate", "amount", "transaction_type",
                                     "cash_credit", "party_name", "vehicle_name", "site_name", "remarks", "mobile_number", "is_deleted"])
    df['date_dt'] = pd.to_datetime(df['date'], errors='coerce')
    df['date'] = df['date_dt'].dt.strftime("%Y-%m-%d %H:%M")
    return df

def load_expenses():
    response = supabase.table("expenses").select("*").eq("is_deleted", False).order("id", desc=True).execute()
    df = pd.DataFrame(response.data)
    if df.empty:
        return pd.DataFrame(columns=["id", "date", "expense_type", "amount", "remarks", "is_deleted"])
    df['date_dt'] = pd.to_datetime(df['date'], errors='coerce')
    return df

def load_payments():
    response = supabase.table("payments").select("*").eq("is_deleted", False).order("id", desc=True).execute()
    df = pd.DataFrame(response.data)
    if df.empty:
        return pd.DataFrame(columns=["id", "date", "party_name", "payment_type", "amount", "remarks", "is_deleted"])
    df['date_dt'] = pd.to_datetime(df['date'], errors='coerce')
    return df

def generate_bill_pdf(bill_data, party_name, total_val):
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 20)
    pdf.cell(0, 12, "INVOICE", ln=True, align="C")
    pdf.ln(2)
    pdf.set_font("Helvetica", "B", 12)
    pdf.cell(0, 7, f"Client: {party_name}", ln=True)
    pdf.set_font("Helvetica", "", 11)
    pdf.cell(0, 7, f"Date: {datetime.now().strftime('%Y-%m-%d')}", ln=True)
    pdf.ln(5)
    pdf.set_fill_color(240, 240, 240)
    pdf.set_font("Helvetica", "B", 10)
    col_widths = [38, 38, 25, 20, 30, 35]
    headers = ["Date", "Item", "Qty", "Unit", "Rate (Rs)", "Amount (Rs)"]
    for i, h in enumerate(headers):
        pdf.cell(col_widths[i], 8, h, border=1, fill=True)
    pdf.ln()
    pdf.set_font("Helvetica", "", 10)
    for _, row in bill_data.iterrows():
        pdf.cell(col_widths[0], 7, str(row['date'])[:16], border=1)
        pdf.cell(col_widths[1], 7, str(row['item_type']), border=1)
        pdf.cell(col_widths[2], 7, str(int(abs(row['quantity']))), border=1)
        pdf.cell(col_widths[3], 7, str(row['unit']), border=1)
        pdf.cell(col_widths[4], 7, f"{row['rate']:,.2f}", border=1)
        pdf.cell(col_widths[5], 7, f"{abs(row['amount']):,.2f}", border=1)
        pdf.ln()
    pdf.ln(4)
    pdf.set_font("Helvetica", "B", 12)
    pdf.cell(0, 10, f"Total Amount: Rs {total_val:,.2f}", ln=True, align="R")
    pdf.ln(5)
    pdf.set_font("Helvetica", "I", 9)
    pdf.cell(0, 7, "Thank you for your business!", ln=True, align="C")
    return bytes(pdf.output())

# --- CONFIGURATION ---
INVENTORY_RULES = {
    "Cement": {
        "JK Strong": {"bag"},
        "JK Super": {"bag"},
        "Birla Super": {"bag"},
        "UltraTech": {"bag"},
        "Shree 43": {"bag"},
        "Shree 53": {"bag"}
    },
    "Stone/Crusher": {
        "Khadi": {"pati", "brass", "piaggo"},
        "Crush Sand": {"pati", "brass", "piaggo"},
        "Plaster Sand": {"pati", "brass", "piaggo"},
        "M Sand": {"pati", "brass", "piaggo"},
        "Grit": {"pati", "brass", "piaggo"},
        "JSB": {"pati", "brass", "piaggo"}
    },
    "Bricks": {
        "Cement Brick": {"pcs"},
        "Red Brick 4\"": {"pcs"},
        "Red Brick 6\"": {"pcs"}
    },
    "AAC Block": {
        "AAC 4\"": {"pcs"},
        "AAC 5\"": {"pcs"},
        "AAC 6\"": {"pcs"}
    },
    "Tile Chemical": {
        "Ascolite Fixobond Plus": {"bag"},
        "Ascolite Genx": {"bag"},
        "MYK 305": {"bag"},
        "MYK 315": {"bag"},
        "Rockfix 100": {"bag"},
        "Rockfix 200": {"bag"}
    },
    "Waterproofing Chemical": {
        "Dr. Fixit LW Plus 100 Ltr": {"can"},
        "Dr. Fixit URP 50 Ltr": {"can"},
        "WP+ 200": {"bottle"},
        "WP+ 200 1 Ltr": {"bottle"},
        "WP+ 200 5 Ltr": {"bottle"},
        "WP+ 200 10 Ltr": {"can"},
        "WP+ 200 20 Ltr": {"can"},
        "WP+ 200 50 Ltr": {"can"},
        "SBR 1 Kg": {"bottle"},
        "SBR 5 Kg": {"bottle"},
        "SBR 10 Kg": {"bottle"},
        "SBR 20 Kg": {"bottle"},
        "SBR 50 Kg": {"bottle"}
    },
    "Block Chemical": {
        "Rockstar BJM 40kg": {"bag"},
        "Rockstar BJM 30kg": {"bag"}
    },
    "Centring Material": {
        "Covered Blocks": {"pcs", "box"}
    },
    "Loose Cement": {
        "Birla White Cement 1 Kg": {"kg"},
        "Birla White Cement 5 Kg": {"kg"},
        "Birla White Cement 50 Kg": {"kg"},
        "Grey Cement 1 Kg": {"kg"},
        "Grey Cement 2 Kg": {"kg"},
        "Grey Cement 5 Kg": {"kg"},
        "Grey Cement 10 Kg": {"kg"}
    },
    "Sanla": {
        "Sanla": {"bag"}
    }
}

UNIT_CONVERSIONS = {
    "pati": 1,
    "piaggo": 40,
    "brass": 240
}

# Default sale rates by (category, item, unit) - user can override
DEFAULT_RATES = {
    # Cement (per bag)
    ("Cement", "JK Strong", "bag"): 330,
    ("Cement", "JK Super", "bag"): 310,
    ("Cement", "Birla Super", "bag"): 350,
    ("Cement", "UltraTech", "bag"): 320,
    ("Cement", "Shree 43", "bag"): 300,
    ("Cement", "Shree 53", "bag"): 300,
    
    # Stone/Crusher (brass rate)
    ("Stone/Crusher", "Crush Sand", "brass"): 3400,
    ("Stone/Crusher", "Khadi", "brass"): 3000,
    ("Stone/Crusher", "Plaster Sand", "brass"): 6500,
    ("Stone/Crusher", "M Sand", "brass"): 3400,
    ("Stone/Crusher", "Grit", "brass"): 3000,
    ("Stone/Crusher", "JSB", "brass"): 3000,
    # Stone/Crusher (piaggo rate)
    ("Stone/Crusher", "Crush Sand", "piaggo"): 1100,
    ("Stone/Crusher", "Khadi", "piaggo"): 1000,
    ("Stone/Crusher", "Plaster Sand", "piaggo"): 2000,
    ("Stone/Crusher", "M Sand", "piaggo"): 1100,
    ("Stone/Crusher", "Grit", "piaggo"): 1000,
    ("Stone/Crusher", "JSB", "piaggo"): 1000,
    # Stone/Crusher (pati rate)
    ("Stone/Crusher", "Crush Sand", "pati"): 30,
    ("Stone/Crusher", "Khadi", "pati"): 25,
    ("Stone/Crusher", "Plaster Sand", "pati"): 50,
    ("Stone/Crusher", "M Sand", "pati"): 30,
    ("Stone/Crusher", "Grit", "pati"): 30,
    ("Stone/Crusher", "JSB", "pati"): 25,
    
    # Bricks (per piece)
    ("Bricks", "Cement Brick", "pcs"): 9,
    ("Bricks", "Red Brick 4\"", "pcs"): 10,
    ("Bricks", "Red Brick 6\"", "pcs"): 14,
    
    # AAC Block (per piece)
    ("AAC Block", "AAC 4\"", "pcs"): 60,
    ("AAC Block", "AAC 5\"", "pcs"): 68,
    ("AAC Block", "AAC 6\"", "pcs"): 80,
    
    # Tile Chemical (per bag)
    ("Tile Chemical", "MYK 305", "bag"): 350,
    ("Tile Chemical", "MYK 315", "bag"): 580,
    ("Tile Chemical", "Ascolite Genx", "bag"): 320,
    ("Tile Chemical", "Ascolite Fixobond Plus", "bag"): 550,
    ("Tile Chemical", "Rockfix 100", "bag"): 350,
    ("Tile Chemical", "Rockfix 200", "bag"): 400,
    
    # Waterproofing Chemical
    ("Waterproofing Chemical", "WP+ 200 1 Ltr", "bottle"): 190,
    ("Waterproofing Chemical", "WP+ 200 5 Ltr", "bottle"): 800,
    ("Waterproofing Chemical", "WP+ 200 10 Ltr", "can"): 1200,
    ("Waterproofing Chemical", "WP+ 200 20 Ltr", "can"): 2200,
    ("Waterproofing Chemical", "SBR 1 Kg", "bottle"): 350,
    ("Waterproofing Chemical", "SBR 5 Kg", "bottle"): 1600,
    ("Waterproofing Chemical", "SBR 10 Kg", "bottle"): 3000,
    ("Waterproofing Chemical", "SBR 20 Kg", "bottle"): 5100,
    
    # Block Chemical (per bag)
    ("Block Chemical", "Rockstar BJM 40kg", "bag"): 400,
    ("Block Chemical", "Rockstar BJM 30kg", "bag"): 320,
    
    # Centring Material
    ("Centring Material", "Covered Blocks", "box"): 400,
    ("Centring Material", "Covered Blocks", "pcs"): 4,

    # Sanla (per bag)
    ("Sanla", "Sanla", "bag"): 100,
}

def convert_to_base_unit(quantity, unit):
    return quantity * UNIT_CONVERSIONS.get(unit, 1)

def convert_from_base_unit(quantity_in_pati, unit):
    return quantity_in_pati / UNIT_CONVERSIONS.get(unit, 1)

EXPENSE_TYPES = ["Staff Salary", "Diesel", "Maintenance", "Shop Rent", "Other"]
DASHBOARD_PASSWORD = "sunny123"

# --- APP CONFIG ---
st.set_page_config(page_title="Inventory Pro", layout="wide")

with open("styles.css", encoding="utf-8") as f:
    st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

# --- SESSION STATE ---
if 'cart' not in st.session_state:
    st.session_state.cart = []
if 'editing_index' not in st.session_state:
    st.session_state.editing_index = None
if 'edit_mode_transaction' not in st.session_state:
    st.session_state.edit_mode_transaction = None
if 'edit_mode_expense' not in st.session_state:
    st.session_state.edit_mode_expense = None
if 'edit_mode_payment' not in st.session_state:
    st.session_state.edit_mode_payment = None
if 'inline_save_counter' not in st.session_state:
    st.session_state.inline_save_counter = 0

# --- DATA LOADING ---
history_df = load_transactions()
expense_df = load_expenses()
payments_df = load_payments()
pending_df = load_pending_orders()

# --- SIDEBAR NAV ---
_nav_options = [
    "Business Dashboard", "New Transaction", "Add Expenses",
    "View History", "Stock", "Credit & Payments", "Pending Orders", "Bill Generator"
]
if 'nav_page' not in st.session_state:
    st.session_state.nav_page = _nav_options[0]

page = st.sidebar.radio(
    "Go to",
    _nav_options,
    index=_nav_options.index(st.session_state.nav_page) if st.session_state.nav_page in _nav_options else 0
)
st.session_state.nav_page = page

if not pending_df.empty:
    st.sidebar.warning(f"⏳ {len(pending_df)} order(s) pending delivery")

# =============================================================================
# PAGE: BUSINESS DASHBOARD
# =============================================================================
if page == "Business Dashboard":
    st.title("📈 Business Intelligence Dashboard")

    pwd_input = st.sidebar.text_input("Enter Dashboard Password", type="password")

    if pwd_input == DASHBOARD_PASSWORD:
        t_col1, t_col2 = st.columns([1, 3])
        with t_col1:
            time_range = st.selectbox("Select Timeframe", ["Last 7 Days", "Last 30 Days", "Last 90 Days", "Year to Date", "All Time"])

        now = datetime.now()
        if time_range == "Last 7 Days": start_date = now - timedelta(days=7)
        elif time_range == "Last 30 Days": start_date = now - timedelta(days=30)
        elif time_range == "Last 90 Days": start_date = now - timedelta(days=90)
        elif time_range == "Year to Date": start_date = datetime(now.year, 1, 1)
        else: start_date = datetime(2000, 1, 1)

        f_hist = history_df[history_df['date_dt'] >= start_date] if not history_df.empty else history_df
        f_exp = expense_df[expense_df['date_dt'] >= start_date] if not expense_df.empty else expense_df

        all_purchases = history_df[history_df['transaction_type'] == 'purchase']

        # Build avg cost per base unit — accumulate numerator/denominator across all unit groups
        # so that e.g. Khadi purchased in both 'brass' and 'piaggo' merges into a single avg rate.
        # Use qty*rate (not `amount`) to exclude transport surcharges from the cost basis.
        _rate_num = {}  # (cat, item_type) -> total cost (excl. transport)
        _rate_den = {}  # (cat, item_type) -> total qty in base unit
        for (cat, item_type, unit), grp in all_purchases.groupby(['category', 'item_type', 'unit']):
            for _, pr in grp.iterrows():
                qty_base = convert_to_base_unit(abs(pr['quantity']), unit)
                cost = abs(pr['quantity']) * pr['rate']  # qty * rate, no transport
                key = (cat, item_type)
                _rate_num[key] = _rate_num.get(key, 0.0) + cost
                _rate_den[key] = _rate_den.get(key, 0.0) + qty_base
        avg_rates = {
            k: _rate_num[k] / _rate_den[k]
            for k in _rate_num if _rate_den.get(k, 0) > 0
        }

        sales_in_period = f_hist[f_hist['transaction_type'] == 'sale']
        # Revenue = qty * rate only (exclude transport surcharges billed to client)
        total_sales = (sales_in_period['quantity'].abs() * sales_in_period['rate']).sum()
        total_operating_exp = f_exp['amount'].sum()

        cogs = 0.0
        skipped_items = []
        for _, row in sales_in_period.iterrows():
            key = (row['category'], row['item_type'])
            if key in avg_rates:
                qty_base = convert_to_base_unit(abs(row['quantity']), row['unit'])
                cogs += qty_base * avg_rates[key]
            else:
                skipped_items.append(f"{row['item_type']} ({row['unit']})")

        gross_profit = total_sales - cogs
        net_profit = gross_profit - total_operating_exp

        stock_response = supabase.table("stock_summary").select("*").execute()
        stock_value = 0.0
        if stock_response.data:
            for stock_row in stock_response.data:
                if stock_row['current_stock'] > 0:
                    key = (stock_row['category'], stock_row['item_type'])
                    if key in avg_rates:
                        stock_value += stock_row['current_stock'] * avg_rates[key]

        total_purchase_in_period = f_hist[f_hist['transaction_type'] == 'purchase']['amount'].sum()

        m1, m2, m3 = st.columns(3)
        m1.metric("Revenue (Sales)", f"₹{total_sales:,.2f}")
        m2.metric("Cost of Goods Sold", f"₹{cogs:,.2f}", help="Estimated cost based on average purchase rate")
        m3.metric("Operating Expenses", f"₹{total_operating_exp:,.2f}")

        st.divider()

        p1, p2, p3 = st.columns(3)
        p1.metric("Gross Profit", f"₹{gross_profit:,.2f}", help="Sales Revenue minus Cost of Goods Sold")
        p2.metric("Net Profit (After Expenses)", f"₹{net_profit:,.2f}", help="Gross Profit minus Operating Expenses")
        p3.metric("Current Stock Value", f"₹{stock_value:,.2f}", help="Value of unsold stock at average purchase rate")

        if total_purchase_in_period > 0:
            st.caption(f"ℹ️ New stock purchased in this period: ₹{total_purchase_in_period:,.2f}")

        st.divider()

        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Sales vs. Expenses Trend")
            if not f_hist.empty:
                sales_trend = f_hist[f_hist['transaction_type'] == 'sale'].groupby(f_hist['date_dt'].dt.date)['amount'].sum().reset_index()
                fig = px.line(sales_trend, x='date_dt', y='amount', title="Daily Revenue")
                st.plotly_chart(fig, width='stretch')

        with c2:
            st.subheader("Cash vs. Credit Sales Split")
            if not f_hist.empty:
                split = f_hist[f_hist['transaction_type'] == 'sale'].groupby('cash_credit')['amount'].sum().reset_index()
                fig2 = px.pie(split, values='amount', names='cash_credit', hole=0.4)
                st.plotly_chart(fig2, width='stretch')
    else:
        if pwd_input != "":
            st.error("❌ Incorrect Password. Access Denied.")
        else:
            st.warning("🔒 Please enter the password in the sidebar to view sensitive financial data.")

# =============================================================================
# PAGE: NEW TRANSACTION
# =============================================================================
elif page == "New Transaction":
    st.title("📝 New Transaction")

    # ── EDIT MODE BANNER ──────────────────────────────────────────────────────
    if st.session_state.edit_mode_transaction:
        edit_data = st.session_state.edit_mode_transaction
        st.warning(f"✏️ **Edit Mode** — Editing transaction ID `{edit_data['original_id']}` for **{edit_data['party_name']}**. Changes will overwrite the original when you save.")

        if not st.session_state.cart:
            st.session_state.cart = [edit_data.copy()]

        if st.button("❌ Cancel Edit", type="secondary"):
            st.session_state.edit_mode_transaction = None
            st.session_state.edit_mode_transaction = None
            st.session_state.cart = []
            st.rerun()

    # ── BILL HEADER ───────────────────────────────────────────────────────────
    with st.expander("👤 Bill Header Details (Party & Site)", expanded=True):
        h_col1, h_col2, h_col3, h_col4 = st.columns(4)
        with h_col1:
            existing_parties = sorted(history_df['party_name'].unique().tolist()) if not history_df.empty else []
            _edit_party = st.session_state.edit_mode_transaction.get('party_name', '') if st.session_state.edit_mode_transaction else ''
            _party_options = ["-- New Party --"] + existing_parties
            _party_default = _party_options.index(_edit_party) if _edit_party and _edit_party in _party_options else 0
            sel_party = st.selectbox("Party Name", _party_options, index=_party_default)
            party_name = st.text_input("Enter New Party").lower() if sel_party == "-- New Party --" else sel_party
        with h_col2:
            auto_site = ""
            if sel_party != "-- New Party --" and not history_df.empty:
                party_history = history_df[history_df['party_name'] == sel_party].dropna(subset=['site_name'])
                if not party_history.empty:
                    auto_site = party_history.iloc[0]['site_name']

            existing_sites = sorted(history_df['site_name'].dropna().unique().tolist()) if not history_df.empty else []
            _edit_site = st.session_state.edit_mode_transaction.get('site_name', '') if st.session_state.edit_mode_transaction else ''
            _site_options = ["-- New Site --"] + existing_sites

            if _edit_site and _edit_site in _site_options:
                _site_default = _site_options.index(_edit_site)
            elif sel_party != "-- New Party --" and auto_site and auto_site in _site_options:
                _site_default = _site_options.index(auto_site)
            else:
                _site_default = 0

            sel_site = st.selectbox("Site Location", _site_options, index=_site_default)

            site_name = st.text_input("Enter New Site").lower() if sel_site == "-- New Site --" else sel_site
        with h_col3:
            VEHICLES = ["MH12DT4738", "MH12LT9760", "MH12ET7413", "MH12MV4032"]
            vehicle = st.selectbox("Vehicle Number", ["-- No Vehicle --"] + VEHICLES)
            if vehicle == "-- No Vehicle --":
                vehicle = ""
        with h_col4:
            mobile_number = st.text_input("Mobile Number (Optional)", placeholder="e.g. 9876543210", max_chars=10)

    st.subheader("🛒 Add Items")

    t_type = st.radio(
        "Transaction Type",
        ["🟢  SALE  (Outgoing Stock)", "🔴  PURCHASE  (Incoming Stock)"],
        horizontal=True,
        help="SALE = you are selling to client | PURCHASE = you are buying from supplier"
    )
    is_sale = "SALE" in t_type
    t_type_val = "sale" if is_sale else "purchase"

    bg_color = "#f0fff4" if is_sale else "#fff5f5"
    border_color = "#38a169" if is_sale else "#e53e3e"
    label = "🟢 SALE — Stock going OUT to client" if is_sale else "🔴 PURCHASE — Stock coming IN from supplier"

    st.markdown(f"""
        <div style="background-color:{bg_color}; border-left: 5px solid {border_color};
                    padding: 8px 16px; border-radius: 6px; margin-bottom: 8px; font-weight: 600;">
            {label}
        </div>
    """, unsafe_allow_html=True)

    with st.container(border=True):
        i_col1, i_col2, i_col3, i_col4 = st.columns([2, 2, 1, 1])
        with i_col1:
            cat = st.selectbox("Category", list(INVENTORY_RULES.keys()))
            item = st.selectbox("Item Type", list(INVENTORY_RULES[cat].keys()))
        with i_col2:
            unit = st.selectbox("Unit", list(INVENTORY_RULES[cat][item]))
            pay_mode = st.selectbox("Payment Mode", ["cash", "credit"])
            if pay_mode == "credit":
                amount_received = st.number_input(
                    "Amount Received Now (₹)", min_value=0.0, step=100.0,
                    help="If customer paid partial amount now, enter it here. Leave 0 for full credit."
                )
            else:
                amount_received = 0.0
        with i_col3:
            st.markdown(f"**{'📦 Qty Selling' if is_sale else '📥 Qty Buying'}**")
            qty = st.number_input("Quantity", min_value=0.0, step=1.0, label_visibility="collapsed")
            st.markdown(f"**{'💰 Sale Rate/Unit' if is_sale else '🛒 Purchase Rate/Unit'}**")
            default_rate = float(DEFAULT_RATES.get((cat, item, unit), 0))
            rate = st.number_input("Rate per unit", min_value=0.0, step=1.0, value=default_rate, label_visibility="collapsed")
        with i_col4:
            st.markdown("**Total Amount**")
            total_display = qty * rate
            if is_sale:
                st.markdown(f"<h3 style='color:#38a169'>₹{total_display:,.0f}</h3>", unsafe_allow_html=True)
            else:
                st.markdown(f"<h3 style='color:#e53e3e'>₹{total_display:,.0f}</h3>", unsafe_allow_html=True)

        t_col1, t_col2 = st.columns(2)
        with t_col1:
            remarks = st.text_input("Remarks")
        with t_col2:
            transport_cost = st.number_input(
                "Transport Cost (₹)" if is_sale else "Transport Cost (₹) — usually 0 for purchases",
                min_value=0.0, step=10.0,
                help="Extra transport charge billed to client" if is_sale else "Leave 0 if supplier bears transport"
            )

        if st.button("➕ Add Item to Transaction", width='stretch', type="secondary"):
            if not party_name:
                st.error("Please specify a Party Name")
            else:
                signed_qty = qty if t_type_val == "purchase" else -qty
                total_amount = (qty * rate) + transport_cost
                remark_with_transport = remarks
                if transport_cost > 0:
                    remark_with_transport = f"{remarks} | Transport: ₹{transport_cost:,.0f}".strip(" |")
                st.session_state.cart.append({
                    "category": cat, "item_type": item, "unit": unit,
                    "quantity": signed_qty, "rate": rate, "amount": total_amount,
                    "transaction_type": t_type_val, "cash_credit": pay_mode,
                    "party_name": party_name, "vehicle_name": vehicle,
                    "site_name": site_name, "remarks": remark_with_transport,
                    "transport_cost": transport_cost, "mobile_number": mobile_number,
                    "amount_received": amount_received
                })
                st.toast("Item added!")

    if st.session_state.cart:
        st.divider()
        st.subheader("🧾 Current Bill Preview")

        if 'editing_index' not in st.session_state:
            st.session_state.editing_index = None

        total_amt = sum(abs(item['amount']) for item in st.session_state.cart)

        for i, item in enumerate(st.session_state.cart):
            with st.container(border=True):
                if st.session_state.editing_index == i:
                    st.markdown(f"**✏️ Editing Item {i+1}**")

                    # ── Bill Header fields ────────────────────────────────────
                    st.caption("👤 Bill Header")
                    h1, h2, h3, h4 = st.columns(4)
                    with h1:
                        _parties = sorted(history_df['party_name'].unique().tolist()) if not history_df.empty else []
                        _cur_party = item.get('party_name', '')
                        _party_opts = ["-- New Party --"] + _parties
                        _party_idx = _party_opts.index(_cur_party) if _cur_party in _party_opts else 0
                        _sel_party = st.selectbox("Party Name", _party_opts, index=_party_idx, key=f"ehp_{i}")
                        new_party = st.text_input("Enter New Party", key=f"ehpn_{i}").lower() if _sel_party == "-- New Party --" else _sel_party
                    with h2:
                        _sites = sorted(history_df['site_name'].dropna().unique().tolist()) if not history_df.empty else []
                        _cur_site = item.get('site_name', '')
                        _site_opts = ["-- New Site --"] + _sites
                        _site_idx = _site_opts.index(_cur_site) if _cur_site in _site_opts else 0
                        _sel_site = st.selectbox("Site", _site_opts, index=_site_idx, key=f"ehs_{i}")
                        new_site = st.text_input("Enter New Site", key=f"ehsn_{i}").lower() if _sel_site == "-- New Site --" else _sel_site
                    with h3:
                        _cur_veh = item.get('vehicle_name', '')
                        _veh_opts = ["-- No Vehicle --"] + VEHICLES
                        _veh_idx = _veh_opts.index(_cur_veh) if _cur_veh in _veh_opts else 0
                        _sel_veh = st.selectbox("Vehicle", _veh_opts, index=_veh_idx, key=f"ehv_{i}")
                        new_vehicle = "" if _sel_veh == "-- No Vehicle --" else _sel_veh
                    with h4:
                        new_mobile = st.text_input("Mobile", value=item.get('mobile_number', ''), max_chars=10, key=f"ehm_{i}")

                    st.divider()

                    # ── Item fields ───────────────────────────────────────────
                    st.caption("📦 Item Details")
                    e1, e2, e3 = st.columns(3)
                    with e1:
                        new_qty = st.number_input("Quantity", value=abs(item['quantity']), min_value=0.0, step=1.0, key=f"eq_{i}")
                        new_rate = st.number_input("Rate (₹)", value=item['rate'], min_value=0.0, step=1.0, key=f"er_{i}")
                    with e2:
                        new_type = st.radio("Transaction Type", ["sale", "purchase"],
                                            index=0 if item['transaction_type'] == 'sale' else 1,
                                            key=f"et_{i}", horizontal=True)
                        new_pay = st.radio("Payment Mode", ["cash", "credit"],
                                           index=0 if item['cash_credit'] == 'cash' else 1,
                                           key=f"ep_{i}", horizontal=True)
                    with e3:
                        new_remarks = st.text_input("Remarks", value=item.get('remarks', ''), key=f"erm_{i}")
                        st.metric("Updated Total", f"₹{new_qty * new_rate:,.2f}")

                    s1, s2 = st.columns(2)
                    if s1.button("✅ Save Changes", key=f"save_{i}", type="primary", width='stretch'):
                        signed_qty = -abs(new_qty) if new_type == "sale" else abs(new_qty)
                        st.session_state.cart[i].update({
                            "quantity": signed_qty,
                            "rate": new_rate,
                            "amount": new_qty * new_rate + st.session_state.cart[i].get('transport_cost', 0),
                            "transaction_type": new_type,
                            "cash_credit": new_pay,
                            "remarks": new_remarks,
                            "party_name": new_party,
                            "site_name": new_site,
                            "vehicle_name": new_vehicle,
                            "mobile_number": new_mobile,
                        })
                        st.session_state.editing_index = None
                        st.rerun()

                    if s2.button("❌ Cancel", key=f"cancel_{i}", width='stretch'):
                        st.session_state.editing_index = None
                        st.rerun()

                else:
                    is_sale_item = item['transaction_type'] == 'sale'
                    item_color = "#f0fff4" if is_sale_item else "#fff5f5"
                    item_border = "#38a169" if is_sale_item else "#e53e3e"
                    item_icon = "🟢" if is_sale_item else "🔴"
                    st.markdown(f"""
                        <div style="background:{item_color}; border-left:4px solid {item_border};
                                    padding:4px 10px; border-radius:4px; margin-bottom:4px; font-size:13px;">
                            {item_icon} {'SALE' if is_sale_item else 'PURCHASE'}
                        </div>
                    """, unsafe_allow_html=True)
                    c1, c2, c3, c4 = st.columns([3, 2, 2, 1])
                    with c1:
                        st.markdown(f"**{item['item_type']}** ({item['category']})")
                        st.caption(f"{item['unit']} · {item['cash_credit'].upper()}")
                    with c2:
                        st.markdown(f"Qty: **{abs(item['quantity'])}**")
                        st.markdown(f"Rate: **₹{item['rate']:,.2f}**")
                    with c3:
                        base_amt = abs(item['quantity']) * item['rate']
                        transport = item.get('transport_cost', 0)
                        amt_received = item.get('amount_received', 0)
                        st.markdown(f"**₹{abs(item['amount']):,.2f}**")
                        if transport > 0:
                            st.caption(f"Item: ₹{base_amt:,.0f} + Transport: ₹{transport:,.0f}")
                        if amt_received > 0:
                            st.caption(f"💵 Received: ₹{amt_received:,.0f} | Due: ₹{abs(item['amount']) - amt_received:,.0f}")
                        if item.get('remarks'):
                            st.caption(item['remarks'])
                    with c4:
                        if st.button("✏️", key=f"edit_{i}", help="Edit this item"):
                            st.session_state.editing_index = i
                            st.rerun()
                        if st.button("🗑️", key=f"del_{i}", help="Remove this item"):
                            st.session_state.cart.pop(i)
                            st.session_state.editing_index = None
                            st.rerun()

        st.divider()
        st.metric("Total Bill Value", f"₹{total_amt:,.2f}")

        col1, col2, col3 = st.columns(3)
        if col1.button("💾 Save Transaction", type="primary", width='stretch'):
            now = datetime.now().strftime("%Y-%m-%d %H:%M")
            
            # If in edit mode, delete the original first (adjusts stock)
            if st.session_state.edit_mode_transaction:
                delete_records("transactions", [st.session_state.edit_mode_transaction['original_id']])
            
            for entry in st.session_state.cart:
                insert_transaction((now, entry['category'], entry['item_type'], entry['unit'],
                                    entry['quantity'], entry['rate'], abs(entry['amount']),
                                    entry['transaction_type'], entry['cash_credit'],
                                    entry['party_name'], entry['vehicle_name'], entry['site_name'],
                                    entry['remarks'], entry.get('mobile_number', '')))
                # If partial payment received on credit transaction, record it
                amt_received = entry.get('amount_received', 0)
                if entry['cash_credit'] == 'credit' and amt_received > 0:
                    payment_type = "Inward" if entry['transaction_type'] == 'sale' else "Outward"
                    insert_payment((now, entry['party_name'], payment_type, amt_received,
                                    f"Partial payment on {entry['item_type']} transaction"))
            
            st.session_state.cart = []
            st.session_state.editing_index = None
            msg = "✅ Transaction updated!" if st.session_state.edit_mode_transaction else "Transaction recorded successfully!"
            st.session_state.edit_mode_transaction = None
            st.success(msg)
            st.rerun()

        if col2.button("⏳ Add to Pending Orders", width='stretch', type="secondary"):
            now = datetime.now().strftime("%Y-%m-%d %H:%M")
            for entry in st.session_state.cart:
                if entry['transaction_type'] == 'sale':
                    insert_pending_order((
                        now, entry['party_name'], entry['site_name'],
                        entry['vehicle_name'], entry['category'], entry['item_type'],
                        entry['unit'], abs(entry['quantity']), entry['rate'],
                        entry['amount'], entry['cash_credit'], entry['remarks']
                    ))
            st.session_state.cart = []
            st.session_state.editing_index = None
            st.success("Added to Pending Orders! Stock will update when delivered.")
            st.rerun()

        if col3.button("🗑️ Clear All", width='stretch'):
            st.session_state.cart = []
            st.session_state.editing_index = None
            st.rerun()

# =============================================================================
# PAGE: ADD EXPENSES
# =============================================================================
elif page == "Add Expenses":
    st.title("💸 Record Daily Expenses")
    
    # Check if editing
    edit_expense = st.session_state.edit_mode_expense
    if edit_expense:
        st.warning(f"✏️ **Edit Mode** — Editing expense ID `{edit_expense['original_id']}`. Changes will overwrite the original.")
        if st.button("❌ Cancel Edit"):
            st.session_state.edit_mode_expense = None
            st.rerun()
    
    with st.container(border=True):
        e_col1, e_col2 = st.columns(2)
        with e_col1:
            _exp_type_default = EXPENSE_TYPES.index(edit_expense['expense_type']) if edit_expense and edit_expense['expense_type'] in EXPENSE_TYPES else 0
            exp_type = st.selectbox("Expense Type", EXPENSE_TYPES, index=_exp_type_default)
            exp_amt = st.number_input("Amount (₹)", min_value=0.0, step=10.0, value=float(edit_expense['amount']) if edit_expense else 0.0)
        with e_col2:
            exp_date = st.date_input("Date", value=datetime.now().date())
            exp_rem = st.text_input("Description / Remarks", value=edit_expense.get('remarks', '') if edit_expense else '')

        if st.button("💾 Save Expense", type="primary", width='stretch'):
            if exp_amt > 0:
                if edit_expense:
                    # Update existing expense
                    supabase.table("expenses").update({
                        "expense_type": exp_type,
                        "amount": exp_amt,
                        "remarks": exp_rem
                    }).eq("id", edit_expense['original_id']).execute()
                    st.session_state.edit_mode_expense = None
                    st.success(f"✅ Expense updated successfully!")
                else:
                    insert_expense((exp_date.strftime("%Y-%m-%d %H:%M"), exp_type, exp_amt, exp_rem))
                    st.success(f"Successfully recorded ₹{exp_amt} for {exp_type}")
                st.rerun()

# =============================================================================
# PAGE: VIEW HISTORY
# =============================================================================
elif page == "View History":
    st.title("📜 History Ledger")

    f_col1, f_col2, f_col3 = st.columns([2, 1, 1])
    with f_col1:
        search = st.text_input("🔍 Search globally")
    with f_col2:
        party_list = ["All Parties"]
        if not history_df.empty:
            party_list += sorted(history_df['party_name'].dropna().unique().tolist())
        selected_party = st.selectbox("Filter by Party", party_list)
    with f_col3:
        time_filter = st.selectbox("Filter by Time", [
            "All Time", "Today", "Yesterday",
            "Last 7 Days", "Last 30 Days", "This Month", "Last Month"
        ])

    tab_sale, tab_purchase, tab_exp, tab_pay, tab_all = st.tabs([
        "💰 Sales", "🛒 Purchases", "💸 Expenses", "💳 Payments", "📑 All Transactions"
    ])

    filtered_history = history_df.copy()

    if not filtered_history.empty:
        now = datetime.now()
        today = now.date()
        if time_filter == "Today":
            filtered_history = filtered_history[filtered_history['date_dt'].dt.date == today]
        elif time_filter == "Yesterday":
            filtered_history = filtered_history[filtered_history['date_dt'].dt.date == today - timedelta(days=1)]
        elif time_filter == "Last 7 Days":
            filtered_history = filtered_history[filtered_history['date_dt'].dt.date >= today - timedelta(days=7)]
        elif time_filter == "Last 30 Days":
            filtered_history = filtered_history[filtered_history['date_dt'].dt.date >= today - timedelta(days=30)]
        elif time_filter == "This Month":
            filtered_history = filtered_history[
                (filtered_history['date_dt'].dt.month == now.month) &
                (filtered_history['date_dt'].dt.year == now.year)
            ]
        elif time_filter == "Last Month":
            last_month = (now.replace(day=1) - timedelta(days=1))
            filtered_history = filtered_history[
                (filtered_history['date_dt'].dt.month == last_month.month) &
                (filtered_history['date_dt'].dt.year == last_month.year)
            ]

    if selected_party != "All Parties":
        filtered_history = filtered_history[filtered_history['party_name'] == selected_party]

    if search:
        filtered_history = filtered_history[
            filtered_history.apply(lambda row: search.lower() in row.astype(str).str.lower().values, axis=1)
        ]

    HIDE_COLS = ["created_at", "is_deleted", "date_dt"]

    # ── Flat option lists for SelectboxColumn (derived from INVENTORY_RULES) ──
    ALL_CATEGORIES = list(INVENTORY_RULES.keys())
    ALL_ITEMS = sorted({itm for cat_items in INVENTORY_RULES.values() for itm in cat_items})
    ALL_UNITS = sorted({u for cat_items in INVENTORY_RULES.values()
                        for item_units in cat_items.values() for u in item_units})

    # Editor key resets on filter change OR after a successful save
    _ekey = f"{time_filter}_{selected_party}_{search}_{st.session_state.inline_save_counter}"

    # ── Shared column config for both Sales and Purchases editors ─────────────
    TXN_COL_CONFIG = {
        "id":               st.column_config.NumberColumn("ID",           disabled=True),
        "date":             st.column_config.TextColumn("Date",           disabled=True),
        "mobile_number":    st.column_config.TextColumn("Mobile",         disabled=True),
        "amount":           st.column_config.NumberColumn("Amount (₹)",   disabled=True, format="₹%.2f"),
        "category":         st.column_config.SelectboxColumn("Category",      options=ALL_CATEGORIES, required=True),
        "item_type":        st.column_config.SelectboxColumn("Item Type",     options=ALL_ITEMS,      required=True),
        "unit":             st.column_config.SelectboxColumn("Unit",          options=ALL_UNITS,      required=True),
        "transaction_type": st.column_config.SelectboxColumn("Type",          options=["sale", "purchase"], required=True),
        "quantity":         st.column_config.NumberColumn("Quantity",    min_value=0.0, step=1.0),
        "rate":             st.column_config.NumberColumn("Rate (₹)",    min_value=0.0, step=1.0, format="₹%.2f"),
        "party_name":       st.column_config.TextColumn("Party Name"),
        "site_name":        st.column_config.TextColumn("Site"),
        "vehicle_name":     st.column_config.TextColumn("Vehicle"),
        "cash_credit":      st.column_config.SelectboxColumn("Payment",       options=["cash", "credit"], required=True),
        "remarks":          st.column_config.TextColumn("Remarks"),
    }

    # ── Helper: validate + save changed rows from inline editor ───────────────
    def _save_inline_edits(edited_df, original_df):
        """Returns (saved_count, error_list). Modifies stock + Supabase in place."""
        errors = []
        saved = 0

        orig_indexed  = original_df.set_index('id')
        edited_indexed = edited_df.set_index('id')

        CHECK_COLS = ['quantity', 'rate', 'category', 'item_type', 'unit',
                      'transaction_type', 'party_name', 'site_name',
                      'vehicle_name', 'cash_credit', 'remarks']

        for row_id, erow in edited_indexed.iterrows():
            if row_id not in orig_indexed.index:
                continue
            orow = orig_indexed.loc[row_id]

            # Detect if anything actually changed
            changed = any(
                str(erow.get(c, '')) != str(orow.get(c, ''))
                for c in CHECK_COLS if c in erow.index
            )
            if not changed:
                continue

            # ── Validate ──────────────────────────────────────────────────────
            try:
                new_qty  = abs(float(erow['quantity']))
                new_rate = float(erow['rate'])
            except (ValueError, TypeError):
                errors.append(f"ID {row_id}: Quantity and Rate must be numbers.")
                continue

            new_cat    = str(erow['category'])
            new_item   = str(erow['item_type'])
            new_unit   = str(erow['unit'])
            new_txn    = str(erow['transaction_type'])
            new_party  = str(erow.get('party_name', '')).strip()
            new_site   = str(erow.get('site_name',  ''))
            new_veh    = str(erow.get('vehicle_name', ''))
            new_cc     = str(erow.get('cash_credit', 'cash'))
            new_rem    = str(erow.get('remarks', ''))

            if new_qty <= 0:
                errors.append(f"ID {row_id}: Quantity must be > 0.")
                continue
            if new_rate < 0:
                errors.append(f"ID {row_id}: Rate cannot be negative.")
                continue
            if not new_party:
                errors.append(f"ID {row_id}: Party name cannot be empty.")
                continue
            if new_cat not in INVENTORY_RULES:
                errors.append(f"ID {row_id}: '{new_cat}' is not a valid category.")
                continue
            if new_item not in INVENTORY_RULES[new_cat]:
                errors.append(f"ID {row_id}: '{new_item}' is not valid for category '{new_cat}'.")
                continue
            if new_unit not in INVENTORY_RULES[new_cat][new_item]:
                errors.append(f"ID {row_id}: '{new_unit}' is not a valid unit for '{new_item}'.")
                continue
            if new_txn not in ('sale', 'purchase'):
                errors.append(f"ID {row_id}: Transaction type must be 'sale' or 'purchase'.")
                continue
            if new_cc not in ('cash', 'credit'):
                errors.append(f"ID {row_id}: Payment mode must be 'cash' or 'credit'.")
                continue

            # ── Fetch authoritative original row from DB ───────────────────────
            db_resp = supabase.table("transactions").select(
                "quantity, category, item_type, unit"
            ).eq("id", int(row_id)).execute()
            if not db_resp.data:
                errors.append(f"ID {row_id}: Record not found in database.")
                continue
            db_orig = db_resp.data[0]
            old_qty_signed = float(db_orig['quantity'])
            old_cat  = db_orig['category']
            old_item = db_orig['item_type']
            old_unit = db_orig['unit']

            # ── Stock reconciliation ───────────────────────────────────────────
            new_signed_qty = -abs(new_qty) if new_txn == 'sale' else abs(new_qty)
            # 1. Reverse the old contribution
            update_stock(old_cat, old_item, old_unit, -old_qty_signed)
            # 2. Apply the new contribution
            update_stock(new_cat, new_item, new_unit, new_signed_qty)

            # ── Persist ───────────────────────────────────────────────────────
            new_amount = round(abs(new_qty) * new_rate, 2)
            supabase.table("transactions").update({
                "quantity":         new_signed_qty,
                "rate":             new_rate,
                "amount":           new_amount,
                "category":         new_cat,
                "item_type":        new_item,
                "unit":             new_unit,
                "transaction_type": new_txn,
                "party_name":       new_party,
                "site_name":        new_site,
                "vehicle_name":     new_veh,
                "cash_credit":      new_cc,
                "remarks":          new_rem,
            }).eq("id", int(row_id)).execute()
            saved += 1

        return saved, errors

    # ── Helper: validate + save changed payment rows ──────────────────────────
    def _save_payment_edits(edited_df, original_df, cmp_cols):
        """Returns (saved_count, error_list). Updates the payments table in Supabase."""
        errors, saved = [], 0
        orig_idx   = original_df.set_index('id')
        edited_idx = edited_df.set_index('id')

        for pid, erow in edited_idx.iterrows():
            if pid not in orig_idx.index:
                continue
            orow = orig_idx.loc[pid]
            changed = any(str(erow.get(c, '')) != str(orow.get(c, '')) for c in cmp_cols)
            if not changed:
                continue
            # Validate
            new_party = str(erow.get('party_name', '')).strip()
            new_ptype = str(erow.get('payment_type', ''))
            try:
                new_amt = float(erow['amount'])
            except (ValueError, TypeError):
                errors.append(f"ID {pid}: Amount must be a number.")
                continue
            if not new_party:
                errors.append(f"ID {pid}: Party name cannot be empty.")
                continue
            if new_ptype not in ('Inward', 'Outward'):
                errors.append(f"ID {pid}: Direction must be 'Inward' or 'Outward'.")
                continue
            if new_amt <= 0:
                errors.append(f"ID {pid}: Amount must be > 0.")
                continue
            supabase.table("payments").update({
                "party_name":   new_party,
                "payment_type": new_ptype,
                "amount":       round(new_amt, 2),
                "remarks":      str(erow.get('remarks', '')),
            }).eq("id", int(pid)).execute()
            saved += 1

        return saved, errors

    # ── Helper: prepare a df for the editor (drop hidden cols, correct qty sign) ─
    def _prep_editor_df(df, abs_qty=True):
        out = df.drop(columns=[c for c in HIDE_COLS if c in df.columns]).copy()
        if abs_qty:
            out['quantity'] = out['quantity'].abs()
        # Ensure consistent column order
        ordered = ['id', 'date', 'category', 'item_type', 'unit', 'quantity', 'rate',
                   'amount', 'transaction_type', 'party_name', 'site_name',
                   'vehicle_name', 'cash_credit', 'remarks', 'mobile_number']
        out = out[[c for c in ordered if c in out.columns]]
        return out

    # ═══════════════════════════════════  SALES  ═══════════════════════════════
    with tab_sale:
        sales_raw = filtered_history[filtered_history['transaction_type'] == 'sale'].copy()
        if not sales_raw.empty:
            orig_sales = _prep_editor_df(sales_raw, abs_qty=True)

            st.caption("✏️ Click any cell to edit. Press **💾 Save changes** when done.")
            edited_sales = st.data_editor(
                orig_sales,
                key=f"sales_ed_{_ekey}",
                use_container_width=True,
                hide_index=True,
                column_config=TXN_COL_CONFIG,
                num_rows="fixed",
            )

            # Live-update the Amount preview column so user sees impact immediately
            edited_sales = edited_sales.copy()
            edited_sales['amount'] = (edited_sales['quantity'].abs() * edited_sales['rate']).round(2)

            # Detect if any row was actually modified
            cmp_cols = [c for c in ['quantity','rate','category','item_type','unit',
                                    'transaction_type','party_name','site_name',
                                    'vehicle_name','cash_credit','remarks']
                        if c in edited_sales.columns]
            n_changed = (edited_sales[cmp_cols].astype(str) != orig_sales[cmp_cols].astype(str)).any(axis=1).sum()

            if n_changed > 0:
                st.info(f"📝 {n_changed} row(s) modified — review then save.")
                if st.button(f"💾 Save {n_changed} change(s)", type="primary", key="save_sales_btn"):
                    saved, errs = _save_inline_edits(edited_sales, orig_sales)
                    for e in errs:
                        st.error(e)
                    if saved > 0:
                        st.success(f"✅ {saved} row(s) saved successfully!")
                        st.session_state.inline_save_counter += 1
                        st.rerun()
        else:
            st.info("No sales found.")

    # ══════════════════════════════════  PURCHASES  ════════════════════════════
    with tab_purchase:
        purch_raw = filtered_history[filtered_history['transaction_type'] == 'purchase'].copy()
        if not purch_raw.empty:
            orig_purch = _prep_editor_df(purch_raw, abs_qty=False)

            st.caption("✏️ Click any cell to edit. Press **💾 Save changes** when done.")
            edited_purch = st.data_editor(
                orig_purch,
                key=f"purch_ed_{_ekey}",
                use_container_width=True,
                hide_index=True,
                column_config=TXN_COL_CONFIG,
                num_rows="fixed",
            )

            edited_purch = edited_purch.copy()
            edited_purch['amount'] = (edited_purch['quantity'].abs() * edited_purch['rate']).round(2)

            cmp_cols = [c for c in ['quantity','rate','category','item_type','unit',
                                    'transaction_type','party_name','site_name',
                                    'vehicle_name','cash_credit','remarks']
                        if c in edited_purch.columns]
            n_changed_p = (edited_purch[cmp_cols].astype(str) != orig_purch[cmp_cols].astype(str)).any(axis=1).sum()

            if n_changed_p > 0:
                st.info(f"📝 {n_changed_p} row(s) modified — review then save.")
                if st.button(f"💾 Save {n_changed_p} change(s)", type="primary", key="save_purch_btn"):
                    saved_p, errs_p = _save_inline_edits(edited_purch, orig_purch)
                    for e in errs_p:
                        st.error(e)
                    if saved_p > 0:
                        st.success(f"✅ {saved_p} row(s) saved successfully!")
                        st.session_state.inline_save_counter += 1
                        st.rerun()
        else:
            st.info("No purchases found.")

    # ═══════════════════════════════════  EXPENSES  ════════════════════════════
    with tab_exp:
        if not expense_df.empty:
            orig_exp = expense_df.drop(columns=[c for c in HIDE_COLS + ['date_dt'] if c in expense_df.columns]).copy()
            # Consistent column order
            _exp_cols = ['id', 'date', 'expense_type', 'amount', 'remarks']
            orig_exp = orig_exp[[c for c in _exp_cols if c in orig_exp.columns]]

            EXP_COL_CONFIG = {
                "id":           st.column_config.NumberColumn("ID",           disabled=True),
                "date":         st.column_config.TextColumn("Date",           disabled=True),
                "expense_type": st.column_config.SelectboxColumn("Expense Type", options=EXPENSE_TYPES, required=True),
                "amount":       st.column_config.NumberColumn("Amount (₹)",   min_value=0.0, step=10.0, format="₹%.2f"),
                "remarks":      st.column_config.TextColumn("Remarks"),
            }

            st.caption("✏️ Click any cell to edit. Press **💾 Save changes** when done.")
            edited_exp = st.data_editor(
                orig_exp,
                key=f"exp_ed_{_ekey}",
                use_container_width=True,
                hide_index=True,
                column_config=EXP_COL_CONFIG,
                num_rows="fixed",
            )

            _exp_cmp = ['expense_type', 'amount', 'remarks']
            n_exp_changed = (
                edited_exp[_exp_cmp].astype(str) != orig_exp[_exp_cmp].astype(str)
            ).any(axis=1).sum()

            if n_exp_changed > 0:
                st.info(f"📝 {n_exp_changed} row(s) modified — review then save.")
                if st.button(f"💾 Save {n_exp_changed} change(s)", type="primary", key="save_exp_btn"):
                    saved_e, errs_e = 0, []
                    orig_exp_idx = orig_exp.set_index('id')
                    edited_exp_idx = edited_exp.set_index('id')

                    for eid, erow in edited_exp_idx.iterrows():
                        orow = orig_exp_idx.loc[eid]
                        changed = any(str(erow.get(c,'')) != str(orow.get(c,'')) for c in _exp_cmp)
                        if not changed:
                            continue
                        # Validate
                        if str(erow['expense_type']) not in EXPENSE_TYPES:
                            errs_e.append(f"ID {eid}: Invalid expense type.")
                            continue
                        try:
                            new_amt = float(erow['amount'])
                        except (ValueError, TypeError):
                            errs_e.append(f"ID {eid}: Amount must be a number.")
                            continue
                        if new_amt <= 0:
                            errs_e.append(f"ID {eid}: Amount must be > 0.")
                            continue
                        supabase.table("expenses").update({
                            "expense_type": str(erow['expense_type']),
                            "amount":       round(new_amt, 2),
                            "remarks":      str(erow.get('remarks', '')),
                        }).eq("id", int(eid)).execute()
                        saved_e += 1

                    for e in errs_e:
                        st.error(e)
                    if saved_e > 0:
                        st.success(f"✅ {saved_e} expense(s) saved!")
                        st.session_state.inline_save_counter += 1
                        st.rerun()
        else:
            st.info("No expenses recorded yet.")

    # ═══════════════════════════════════  PAYMENTS  ════════════════════════════
    with tab_pay:
        if not payments_df.empty:
            _all_pay_parties = sorted(payments_df['party_name'].dropna().unique().tolist())
            # Merge parties from transactions too for a richer dropdown
            if not history_df.empty:
                _all_pay_parties = sorted(set(_all_pay_parties) | set(history_df['party_name'].dropna().unique().tolist()))

            PAY_COL_CONFIG = {
                "id":           st.column_config.NumberColumn("ID",           disabled=True),
                "date":         st.column_config.TextColumn("Date",           disabled=True),
                "party_name":   st.column_config.SelectboxColumn("Party",     options=_all_pay_parties, required=True),
                "payment_type": st.column_config.SelectboxColumn("Direction", options=["Inward", "Outward"], required=True),
                "amount":       st.column_config.NumberColumn("Amount (₹)",   min_value=0.0, step=10.0, format="₹%.2f"),
                "remarks":      st.column_config.TextColumn("Remarks"),
            }

            orig_pay = payments_df.drop(columns=[c for c in HIDE_COLS + ['date_dt'] if c in payments_df.columns]).copy()
            _pay_cols = ['id', 'date', 'party_name', 'payment_type', 'amount', 'remarks']
            orig_pay = orig_pay[[c for c in _pay_cols if c in orig_pay.columns]]

            # ── Inward summary ────────────────────────────────────────────────
            st.markdown("### 🟢 Inward Payments (Received from Clients)")
            inward_orig = orig_pay[orig_pay['payment_type'] == 'Inward'].copy()
            if not inward_orig.empty:
                st.caption("✏️ Click any cell to edit. Press **💾 Save changes** when done.")
                edited_inward = st.data_editor(
                    inward_orig,
                    key=f"pay_in_ed_{_ekey}",
                    use_container_width=True,
                    hide_index=True,
                    column_config=PAY_COL_CONFIG,
                    num_rows="fixed",
                )
                st.caption(f"Total Received: ₹{inward_orig['amount'].sum():,.2f}")

                _pay_cmp = ['party_name', 'payment_type', 'amount', 'remarks']
                n_in_changed = (
                    edited_inward[_pay_cmp].astype(str) != inward_orig[_pay_cmp].astype(str)
                ).any(axis=1).sum()
                if n_in_changed > 0:
                    st.info(f"📝 {n_in_changed} row(s) modified.")
                    if st.button(f"💾 Save {n_in_changed} inward change(s)", type="primary", key="save_inward_btn"):
                        saved_i, errs_i = _save_payment_edits(edited_inward, inward_orig, _pay_cmp)
                        for e in errs_i: st.error(e)
                        if saved_i > 0:
                            st.success(f"✅ {saved_i} payment(s) saved!")
                            st.session_state.inline_save_counter += 1
                            st.rerun()
            else:
                st.info("No inward payments recorded yet.")

            st.divider()

            # ── Outward summary ───────────────────────────────────────────────
            st.markdown("### 🔴 Outward Payments (Paid to Suppliers)")
            outward_orig = orig_pay[orig_pay['payment_type'] == 'Outward'].copy()
            if not outward_orig.empty:
                st.caption("✏️ Click any cell to edit. Press **💾 Save changes** when done.")
                edited_outward = st.data_editor(
                    outward_orig,
                    key=f"pay_out_ed_{_ekey}",
                    use_container_width=True,
                    hide_index=True,
                    column_config=PAY_COL_CONFIG,
                    num_rows="fixed",
                )
                st.caption(f"Total Paid: ₹{outward_orig['amount'].sum():,.2f}")

                n_out_changed = (
                    edited_outward[_pay_cmp].astype(str) != outward_orig[_pay_cmp].astype(str)
                ).any(axis=1).sum()
                if n_out_changed > 0:
                    st.info(f"📝 {n_out_changed} row(s) modified.")
                    if st.button(f"💾 Save {n_out_changed} outward change(s)", type="primary", key="save_outward_btn"):
                        saved_o, errs_o = _save_payment_edits(edited_outward, outward_orig, _pay_cmp)
                        for e in errs_o: st.error(e)
                        if saved_o > 0:
                            st.success(f"✅ {saved_o} payment(s) saved!")
                            st.session_state.inline_save_counter += 1
                            st.rerun()
            else:
                st.info("No outward payments recorded yet.")
        else:
            st.info("No payments recorded yet.")

    with tab_all:
        st.dataframe(filtered_history.drop(columns=[c for c in HIDE_COLS if c in filtered_history.columns]), width='stretch', hide_index=True)

    # ── EDIT RECORDS ───────────────────────────────────────────────────────────
    st.divider()
    st.subheader("✏️ Edit Records")
    
    edit_type = st.radio("What do you want to edit?", ["Transaction", "Expense", "Payment"], horizontal=True)
    
    if edit_type == "Transaction":
        if history_df.empty:
            st.info("No transactions to edit.")
        else:
            edit_df = history_df.copy()
            edit_df['label'] = (
                edit_df['date'].astype(str).str[:10] + " | " +
                edit_df['party_name'] + " | " +
                edit_df['item_type'] + " | ₹" +
                edit_df['amount'].abs().astype(int).astype(str)
            )
            selected = st.selectbox("Select transaction", ["-- Select --"] + edit_df['label'].tolist(), key="edit_txn")
            
            if selected != "-- Select --" and st.button("✏️ Edit This Transaction", type="primary"):
                row = edit_df[edit_df['label'] == selected].iloc[0]
                st.session_state.edit_mode_transaction = {
                    "original_id": int(row['id']),
                    "category": row['category'],
                    "item_type": row['item_type'],
                    "unit": row['unit'],
                    "quantity": abs(row['quantity']),
                    "rate": row['rate'],
                    "amount": abs(row['amount']),
                    "transaction_type": row['transaction_type'],
                    "cash_credit": row['cash_credit'],
                    "party_name": row['party_name'],
                    "vehicle_name": row.get('vehicle_name', ''),
                    "site_name": row.get('site_name', ''),
                    "remarks": row.get('remarks', ''),
                    "transport_cost": 0,
                    "mobile_number": row.get('mobile_number', '')
                }
                st.session_state.nav_page = "New Transaction"
                st.session_state.cart = []
                st.rerun()
    
    elif edit_type == "Expense":
        if expense_df.empty:
            st.info("No expenses to edit.")
        else:
            edit_df = expense_df.copy()
            edit_df['label'] = (
                edit_df['date'].astype(str).str[:10] + " | " +
                edit_df['expense_type'] + " | ₹" +
                edit_df['amount'].astype(int).astype(str)
            )
            selected = st.selectbox("Select expense", ["-- Select --"] + edit_df['label'].tolist(), key="edit_exp")
            
            if selected != "-- Select --" and st.button("✏️ Edit This Expense", type="primary"):
                row = edit_df[edit_df['label'] == selected].iloc[0]
                st.session_state.edit_mode_expense = {
                    "original_id": int(row['id']),
                    "expense_type": row['expense_type'],
                    "amount": row['amount'],
                    "remarks": row.get('remarks', '')
                }
                st.session_state.nav_page = "Add Expenses"
                st.rerun()
    
    else:  # Payment
        if payments_df.empty:
            st.info("No payments to edit.")
        else:
            edit_df = payments_df.copy()
            edit_df['label'] = (
                edit_df['date'].astype(str).str[:10] + " | " +
                edit_df['party_name'] + " | " +
                edit_df['payment_type'] + " | ₹" +
                edit_df['amount'].astype(int).astype(str)
            )
            selected = st.selectbox("Select payment", ["-- Select --"] + edit_df['label'].tolist(), key="edit_pay")
            
            if selected != "-- Select --" and st.button("✏️ Edit This Payment", type="primary"):
                row = edit_df[edit_df['label'] == selected].iloc[0]
                st.session_state.edit_mode_payment = {
                    "original_id": int(row['id']),
                    "party_name": row['party_name'],
                    "payment_type": row['payment_type'],
                    "amount": row['amount'],
                    "remarks": row.get('remarks', '')
                }
                st.session_state.nav_page = "Credit & Payments"
                st.rerun()

    # ── DELETE TOOL ───────────────────────────────────────────────────────────
    st.divider()
    st.subheader("🗑️ Universal Delete Tool")

    with st.expander("Danger Zone - Remove Records"):
        target_table = st.selectbox(
            "1. Select Category",
            ["transactions", "expenses", "payments"],
            format_func=lambda x: x.capitalize()
        )

        if target_table == "transactions":
            df_for_delete = history_df.copy()
            df_for_delete['label'] = df_for_delete['id'].astype(str) + ": " + df_for_delete['party_name'] + " - " + df_for_delete['item_type'] + " (₹" + df_for_delete['amount'].astype(str) + ")"
        elif target_table == "expenses":
            df_for_delete = expense_df.copy()
            df_for_delete['label'] = df_for_delete['id'].astype(str) + ": " + df_for_delete['expense_type'] + " (₹" + df_for_delete['amount'].astype(str) + ")"
        else:
            df_for_delete = payments_df.copy()
            df_for_delete['label'] = df_for_delete['id'].astype(str) + ": " + df_for_delete['party_name'] + " [" + df_for_delete['payment_type'] + "] (₹" + df_for_delete['amount'].astype(str) + ")"

        selected_labels = st.multiselect(f"2. Select {target_table.capitalize()} to Delete", df_for_delete['label'].tolist())
        ids_to_delete = [int(label.split(":")[0]) for label in selected_labels]

        if ids_to_delete:
            st.warning(f"⚠️ You are about to permanently delete {len(ids_to_delete)} record(s). This cannot be undone.")
            if target_table == "transactions":
                st.info("Stock levels will be automatically adjusted.")

            if st.button(f"Confirm Delete {len(ids_to_delete)} Records", type="primary", width='stretch'):
                delete_records(target_table, ids_to_delete)
                st.success(f"Successfully removed selected records from {target_table}.")
                st.rerun()

        st.divider()
        st.markdown("**↩️ Undo / Restore Deleted Records**")

        deleted_data = load_deleted_records(target_table)

        if not deleted_data:
            st.info("No deleted records to restore.")
        else:
            deleted_df = pd.DataFrame(deleted_data)

            if target_table == "transactions":
                deleted_df['label'] = deleted_df['id'].astype(str) + ": " + deleted_df['party_name'] + " - " + deleted_df['item_type'] + " (₹" + deleted_df['amount'].astype(str) + ") [DELETED]"
            elif target_table == "expenses":
                deleted_df['label'] = deleted_df['id'].astype(str) + ": " + deleted_df['expense_type'] + " (₹" + deleted_df['amount'].astype(str) + ") [DELETED]"
            else:
                deleted_df['label'] = deleted_df['id'].astype(str) + ": " + deleted_df['party_name'] + " [" + deleted_df['payment_type'] + "] (₹" + deleted_df['amount'].astype(str) + ") [DELETED]"

            restore_labels = st.multiselect("Select records to restore", deleted_df['label'].tolist(), key="restore_select")
            ids_to_restore = [int(label.split(":")[0]) for label in restore_labels]

            if ids_to_restore:
                if st.button(f"↩️ Restore {len(ids_to_restore)} Record(s)", type="secondary", width='stretch'):
                    restore_records(target_table, ids_to_restore)
                    st.success(f"Successfully restored {len(ids_to_restore)} record(s)!")
                    st.rerun()

# =============================================================================
# PAGE: STOCK
# =============================================================================
elif page == "Stock":
    st.title("📦 Inventory Stock")

    stock_df = pd.DataFrame()

    s_col1, s_col2 = st.columns(2)
    with s_col1:
        stock_cat = st.selectbox("Filter Category", ["All"] + list(INVENTORY_RULES.keys()), key="stock_cat_sel")
    with s_col2:
        if stock_cat != "All":
            stock_item = st.selectbox("Filter Item Type", ["All"] + list(INVENTORY_RULES[stock_cat].keys()), key="stock_item_sel")
        else:
            stock_item = "All"

    response = supabase.table("stock_summary").select("*").execute()

    if response.data:
        stock_df = pd.DataFrame(response.data)

        if stock_cat != "All":
            stock_df = stock_df[stock_df['category'] == stock_cat]
        if stock_item != "All":
            stock_df = stock_df[stock_df['item_type'] == stock_item]

        if not stock_df.empty:
            display_stock = stock_df[['category', 'item_type', 'unit', 'current_stock']].copy()

            def format_stock(row):
                if row['unit'] == 'pati':
                    pati = row['current_stock']
                    brass = pati / 240
                    piaggo = pati / 40
                    return f"{pati:.0f} pati / {piaggo:.2f} piaggo / {brass:.2f} brass"
                return f"{row['current_stock']:.2f}"

            display_stock['Balance'] = display_stock.apply(format_stock, axis=1)

            st.dataframe(
                display_stock[['category', 'item_type', 'Balance']].rename(columns={
                    'category': 'Category',
                    'item_type': 'Item'
                }),
                width='stretch',
                hide_index=True
            )
        else:
            st.info("No items match the selected filters.")
    else:
        st.info("📦 Your inventory is currently empty. Add a 'Purchase' in New Transactions to see stock here!")

# =============================================================================
# PAGE: CREDIT & PAYMENTS
# =============================================================================
elif page == "Credit & Payments":
    st.title("💳 Credit & Payments")
    
    # Check if editing a payment
    edit_payment = st.session_state.edit_mode_payment
    if edit_payment:
        st.warning(f"✏️ **Edit Mode** — Editing payment ID `{edit_payment['original_id']}` for `{edit_payment['party_name']}`. Changes will overwrite the original.")
        
        with st.container(border=True):
            ep_col1, ep_col2 = st.columns(2)
            with ep_col1:
                ep_party = st.text_input("Party Name", value=edit_payment['party_name'])
                ep_type_idx = 0 if edit_payment['payment_type'] == 'Inward' else 1
                ep_type = st.radio("Payment Type", ["Inward", "Outward"], index=ep_type_idx, horizontal=True)
            with ep_col2:
                ep_amt = st.number_input("Amount (₹)", value=float(edit_payment['amount']), min_value=0.0)
                ep_rem = st.text_input("Remarks", value=edit_payment.get('remarks', ''))
            
            btn_col1, btn_col2 = st.columns(2)
            with btn_col1:
                if st.button("💾 Save Changes", type="primary"):
                    supabase.table("payments").update({
                        "party_name": ep_party,
                        "payment_type": ep_type,
                        "amount": ep_amt,
                        "remarks": ep_rem
                    }).eq("id", edit_payment['original_id']).execute()
                    st.session_state.edit_mode_payment = None
                    st.success("✅ Payment updated!")
                    st.rerun()
            with btn_col2:
                if st.button("❌ Cancel Edit"):
                    st.session_state.edit_mode_payment = None
                    st.rerun()
        
        st.divider()

    detailed_list = []
    outstanding_parties = []
    total_rec = 0
    total_pay = 0

    if not history_df.empty:
        full_credit_df = history_df[history_df['cash_credit'].str.lower() == 'credit']
        all_p_list = sorted(history_df['party_name'].unique().tolist())

        for p in all_p_list:
            b_sale = full_credit_df[(full_credit_df['party_name'] == p) & (full_credit_df['transaction_type'] == 'sale')]['amount'].sum()
            b_pur = full_credit_df[(full_credit_df['party_name'] == p) & (full_credit_df['transaction_type'] == 'purchase')]['amount'].sum()

            r_pay = payments_df[(payments_df['party_name'] == p) & (payments_df['payment_type'] == 'Inward')]['amount'].sum()
            p_pay = payments_df[(payments_df['party_name'] == p) & (payments_df['payment_type'] == 'Outward')]['amount'].sum()

            net_receivable = b_sale - r_pay
            net_payable = b_pur - p_pay

            if net_receivable > 0.01 or net_payable > 0.01:
                outstanding_parties.append(p)
                detailed_list.append({
                    "Party Name": p,
                    "Pending Receivable (Client)": max(0, net_receivable),
                    "Pending Payable (Supplier)": max(0, net_payable)
                })
                total_rec += max(0, net_receivable)
                total_pay += max(0, net_payable)

    with st.expander("💳 Record New Payment Settlement", expanded=False):
        if not outstanding_parties:
            st.info("No parties currently have an outstanding balance.")
        else:
            receivable_parties = []
            payable_parties = []
            for item in detailed_list:
                if item['Pending Receivable (Client)'] > 0.01:
                    receivable_parties.append(item['Party Name'])
                if item['Pending Payable (Supplier)'] > 0.01:
                    payable_parties.append(item['Party Name'])

            p_col1, p_col2, p_col3 = st.columns(3)

            with p_col1:
                pay_mode = st.radio(
                    "Payment Direction",
                    ["Received (from Client)", "Paid (to Dealer)"],
                    horizontal=True
                )

            with p_col2:
                if pay_mode == "Received (from Client)":
                    filtered_parties = receivable_parties
                    db_type = "Inward"
                    help_text = "Clients who owe you money"
                else:
                    filtered_parties = payable_parties
                    db_type = "Outward"
                    help_text = "Suppliers you owe money to"

                if filtered_parties:
                    pay_party = st.selectbox("Select Party", filtered_parties, help=help_text)
                    party_detail = next((d for d in detailed_list if d['Party Name'] == pay_party), None)
                    if party_detail:
                        if db_type == "Inward":
                            st.caption(f"Outstanding: ₹{party_detail['Pending Receivable (Client)']:,.2f}")
                        else:
                            st.caption(f"Outstanding: ₹{party_detail['Pending Payable (Supplier)']:,.2f}")
                else:
                    st.info("No parties found for this payment type.")
                    pay_party = None

            with p_col3:
                pay_amt = st.number_input("Amount (₹)", min_value=0.0)

            pay_rem = st.text_input("Payment Remarks")

            if pay_party and st.button("Save Payment Entry", type="primary"):
                if pay_amt > 0:
                    insert_payment((
                        datetime.now().strftime("%Y-%m-%d %H:%M"),
                        pay_party, db_type, pay_amt, pay_rem
                    ))
                    st.success(f"Payment of ₹{pay_amt:,.2f} recorded for {pay_party}!")
                    st.rerun()
                else:
                    st.warning("Please enter an amount greater than 0.")

    m1, m2, m3 = st.columns(3)
    m1.metric("Total Net Receivable", f"₹{total_rec:,.2f}")
    m2.metric("Total Net Payable", f"₹{total_pay:,.2f}")
    m3.metric("Net Market Balance", f"₹{total_rec - total_pay:,.2f}")

    st.divider()

    if detailed_list:
        det_df = pd.DataFrame(detailed_list)

        st.markdown("### 🟢 Client Receivables (Money to Come)")
        rec_df = det_df[det_df['Pending Receivable (Client)'] > 0][["Party Name", "Pending Receivable (Client)"]]
        if not rec_df.empty:
            st.dataframe(rec_df, width='stretch', hide_index=True)
        else:
            st.info("No receivables outstanding.")

        st.divider()

        st.markdown("### 🔴 Supplier Payables (Money to Pay)")
        pay_df = det_df[det_df['Pending Payable (Supplier)'] > 0][["Party Name", "Pending Payable (Supplier)"]]
        if not pay_df.empty:
            st.dataframe(pay_df, width='stretch', hide_index=True)
        else:
            st.info("No payables outstanding.")
    else:
        st.success("✅ All balances are clear!")

# =============================================================================
# PAGE: PENDING ORDERS
# =============================================================================
elif page == "Pending Orders":
    st.title("⏳ Pending Orders")

    if pending_df.empty:
        st.success("✅ No pending orders! All deliveries are complete.")
    else:
        st.info(f"📦 {len(pending_df)} order(s) awaiting delivery.")
        st.divider()

        for _, order in pending_df.iterrows():
            with st.container(border=True):
                c1, c2, c3 = st.columns([3, 2, 2])

                with c1:
                    st.markdown(f"**{order['party_name']}**")
                    st.caption(f"📍 {order['site_name']} &nbsp;|&nbsp; 🗓️ {str(order['date'])[:16]}")
                    st.markdown(f"**{order['item_type']}** ({order['category']})")
                    st.caption(f"Qty: {abs(order['quantity'])} {order['unit']} · Rate: ₹{order['rate']:,.0f} · {order['cash_credit'].upper()}")
                    if order.get('remarks'):
                        st.caption(f"📝 {order['remarks']}")

                with c2:
                    st.metric("Order Value", f"₹{abs(order['amount']):,.2f}")

                with c3:
                    st.markdown("**Mark as Delivered:**")
                    delivery_vehicle = st.text_input(
                        "Vehicle No.",
                        value=order.get('vehicle_name', ''),
                        key=f"veh_{order['id']}",
                        placeholder="e.g. GJ-01-XX-0000"
                    )
                    d1, d2 = st.columns(2)
                    if d1.button("✅ Delivered", key=f"done_{order['id']}", type="primary", width='stretch'):
                        complete_pending_order(order['id'], delivery_vehicle)
                        st.success("Order marked as delivered and added to transactions!")
                        st.rerun()
                    if d2.button("❌ Cancel", key=f"cancel_{order['id']}", width='stretch'):
                        delete_pending_order(order['id'])
                        st.warning("Pending order cancelled.")
                        st.rerun()

# =============================================================================
# PAGE: BILL GENERATOR
# =============================================================================
elif page == "Bill Generator":
    st.title("🧾 Smart Bill Generator")
    bill_data = pd.DataFrame()

    if not history_df.empty:
        mode = st.radio("Choose Mode:", ["Filter by Party & Date", "Enter Transaction IDs Manually"], horizontal=True)

        if mode == "Filter by Party & Date":
            col_f1, col_f2 = st.columns(2)
            with col_f1:
                parties = sorted(history_df['party_name'].unique().tolist())
                sel_p = st.selectbox("Select Party Name", ["-- Select Party --"] + parties)
                sites = ["All Sites"]
                if sel_p != "-- Select Party --":
                    sites += sorted(history_df[history_df['party_name'] == sel_p]['site_name'].unique().tolist())
                sel_s = st.selectbox("Select Site Name", sites)
            with col_f2:
                today = datetime.now()
                date_range = st.date_input("Select Date Range", value=(today - pd.Timedelta(days=10), today.date()))

            if sel_p != "-- Select Party --":
                bill_data = history_df[history_df['party_name'] == sel_p].copy()
                if sel_s != "All Sites":
                    bill_data = bill_data[bill_data['site_name'] == sel_s]
                if len(date_range) == 2:
                    bill_data = bill_data[
                        (bill_data['date_dt'].dt.date >= date_range[0]) &
                        (bill_data['date_dt'].dt.date <= date_range[1])
                    ]

        else:
            st.markdown('<div class="no-print">', unsafe_allow_html=True)
            with st.expander("🔍 Search Transaction IDs (Reference Table)", expanded=True):
                ref_search = st.text_input("Filter Reference Table (by Party, Item, etc.)")
                ref_df = history_df.copy()
                if ref_search:
                    ref_df = ref_df[ref_df.apply(lambda row: ref_search.lower() in row.astype(str).str.lower().values, axis=1)]
                st.dataframe(
                    ref_df[['id', 'date', 'party_name', 'item_type', 'quantity', 'amount', 'transaction_type']],
                    width='stretch',
                    hide_index=True,
                    height=250
                )
            ids = st.text_input("Enter Transaction IDs (e.g. 1, 5, 8)", placeholder="Enter IDs separated by commas")
            st.markdown('</div>', unsafe_allow_html=True)

            if ids:
                try:
                    id_list = [int(i.strip()) for i in ids.split(",") if i.strip().isdigit()]
                    bill_data = history_df[history_df['id'].isin(id_list)].copy()
                except Exception:
                    st.error("Invalid ID format. Please use numbers separated by commas.")

    if not bill_data.empty:
        st.divider()

        st.markdown('<div class="printable-bill">', unsafe_allow_html=True)
        st.markdown("# INVOICE")
        st.markdown(f"**Client:** {bill_data.iloc[0]['party_name']}")
        st.markdown(f"**Date:** {datetime.now().strftime('%Y-%m-%d')}")

        display_bill = bill_data[['date', 'item_type', 'quantity', 'unit', 'rate', 'amount']].copy()
        display_bill['quantity'] = display_bill['quantity'].abs()
        display_bill['amount'] = display_bill['amount'].abs()

        st.write(display_bill.to_html(index=False, classes='bill-table'), unsafe_allow_html=True)

        total_val = display_bill['amount'].sum()
        st.markdown(f"## Total Amount: ₹{total_val:,.2f}")
        st.markdown('</div>', unsafe_allow_html=True)

        st.divider()

        bill_lines = "\n".join([
            f"• {row['item_type']} | Qty: {int(abs(row['quantity']))} {row['unit']} | Rate: ₹{row['rate']:,.0f} | Amt: ₹{abs(row['amount']):,.0f}"
            for _, row in display_bill.iterrows()
        ])
        share_text = f"""🧾 *INVOICE*
Client: {bill_data.iloc[0]['party_name']}
Date: {datetime.now().strftime('%Y-%m-%d')}

{bill_lines}

*Total: ₹{total_val:,.2f}*

Sent via Inventory Pro"""

        pdf_bytes = generate_bill_pdf(display_bill, bill_data.iloc[0]['party_name'], total_val)
        fname = f"Invoice_{bill_data.iloc[0]['party_name'].replace(' ', '_')}_{datetime.now().strftime('%Y%m%d')}.pdf"

        col1, col2 = st.columns(2)

        with col1:
            st.download_button(
                label="📥 Download Invoice PDF",
                data=pdf_bytes,
                file_name=fname,
                mime="application/pdf",
                width='stretch',
                type="primary"
            )

        with col2:
            import urllib.parse
            encoded_text = urllib.parse.quote(share_text)
            whatsapp_url = f"https://web.whatsapp.com/send?text={encoded_text}"

            st.markdown(f"""
                <a href="{whatsapp_url}" target="_blank" rel="noopener noreferrer"
                   style="
                       display: block;
                       background-color: #25D366;
                       color: white;
                       text-align: center;
                       padding: 10px 24px;
                       font-size: 16px;
                       font-weight: 600;
                       border-radius: 8px;
                       text-decoration: none;
                       margin-top: 4px;
                       width: 100%;
                       box-sizing: border-box;
                   ">
                    📤 Share via WhatsApp
                </a>
            """, unsafe_allow_html=True)

        st.info("💡 On mobile: Share button opens native share sheet. On desktop: opens WhatsApp Web.", icon="📱")
        st.info("💡 Press **Ctrl + P** to print directly from browser.", icon="⌨️")
