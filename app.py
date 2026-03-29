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
        "Cement 4\"": {"pcs"},
        "Cement 6\"": {"pcs"},
        "Red Brick": {"pcs"}
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
    }
}

UNIT_CONVERSIONS = {
    "pati": 1,
    "piaggo": 40,
    "brass": 240
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

        avg_rates = {}
        for (cat, item_type, unit), grp in all_purchases.groupby(['category', 'item_type', 'unit']):
            total_qty_base = sum(convert_to_base_unit(abs(q), unit) for q in grp['quantity'])
            if total_qty_base > 0:
                avg_rates[(cat, item_type)] = grp['amount'].sum() / total_qty_base

        sales_in_period = f_hist[f_hist['transaction_type'] == 'sale']
        total_sales = sales_in_period['amount'].sum()
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
        st.warning(f"✏️ **Edit Mode** — Editing transaction ID `{edit_data['original_id']}` for **{edit_data['party_name']}**. Original will be deleted when you save.")

        if not st.session_state.cart:
            st.session_state.cart = [edit_data.copy()]
            delete_records("transactions", [edit_data['original_id']])
            st.info("Original record removed. Make your changes below and press Save.")

        if st.button("🚫 Cancel Edit (restore original)", type="secondary"):
            restore_records("transactions", [edit_data['original_id']])
            st.session_state.edit_mode_transaction = None
            st.session_state.cart = []
            st.rerun()

    # ── BILL HEADER ───────────────────────────────────────────────────────────
    with st.expander("👤 Bill Header Details (Party & Site)", expanded=True):
        h_col1, h_col2, h_col3, h_col4 = st.columns(4)
        with h_col1:
            existing_parties = sorted(history_df['party_name'].unique().tolist()) if not history_df.empty else []
            sel_party = st.selectbox("Party Name", ["-- New Party --"] + existing_parties)
            party_name = st.text_input("Enter New Party").lower() if sel_party == "-- New Party --" else sel_party
        with h_col2:
            auto_site = ""
            if sel_party != "-- New Party --" and not history_df.empty:
                party_history = history_df[history_df['party_name'] == sel_party].dropna(subset=['site_name'])
                if not party_history.empty:
                    auto_site = party_history.iloc[0]['site_name']

            existing_sites = sorted(history_df['site_name'].dropna().unique().tolist()) if not history_df.empty else []

            if sel_party != "-- New Party --" and auto_site:
                site_options = ["-- New Site --"] + existing_sites
                auto_idx = site_options.index(auto_site) if auto_site in site_options else 0
                sel_site = st.selectbox("Site Location", site_options, index=auto_idx)
            else:
                sel_site = st.selectbox("Site Location", ["-- New Site --"] + existing_sites)

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
        with i_col3:
            st.markdown(f"**{'📦 Qty Selling' if is_sale else '📥 Qty Buying'}**")
            qty = st.number_input("Quantity", min_value=0.0, step=1.0, label_visibility="collapsed")
            st.markdown(f"**{'💰 Sale Rate/Unit' if is_sale else '🛒 Purchase Rate/Unit'}**")
            rate = st.number_input("Rate per unit", min_value=0.0, step=1.0, label_visibility="collapsed")
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
                    "transport_cost": transport_cost, "mobile_number": mobile_number
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
                            "amount": new_qty * new_rate,
                            "transaction_type": new_type,
                            "cash_credit": new_pay,
                            "remarks": new_remarks
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
                        st.markdown(f"**₹{abs(item['amount']):,.2f}**")
                        if transport > 0:
                            st.caption(f"Item: ₹{base_amt:,.0f} + Transport: ₹{transport:,.0f}")
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
            for entry in st.session_state.cart:
                insert_transaction((now, entry['category'], entry['item_type'], entry['unit'],
                                    entry['quantity'], entry['rate'], entry['amount'],
                                    entry['transaction_type'], entry['cash_credit'],
                                    entry['party_name'], entry['vehicle_name'], entry['site_name'],
                                    entry['remarks'], entry.get('mobile_number', '')))
            st.session_state.cart = []
            st.session_state.editing_index = None
            st.session_state.edit_mode_transaction = None
            st.success("Transaction recorded successfully!")
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
    with st.container(border=True):
        e_col1, e_col2 = st.columns(2)
        with e_col1:
            exp_type = st.selectbox("Expense Type", EXPENSE_TYPES)
            exp_amt = st.number_input("Amount (₹)", min_value=0.0, step=10.0)
        with e_col2:
            exp_date = st.date_input("Date", value=datetime.now().date())
            exp_rem = st.text_input("Description / Remarks")

        if st.button("💾 Save Expense", type="primary", width='stretch'):
            if exp_amt > 0:
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

    with tab_sale:
        sales_df = filtered_history[filtered_history['transaction_type'] == 'sale'].copy()
        if not sales_df.empty:
            sales_df['quantity'] = sales_df['quantity'].abs()
            st.dataframe(sales_df.drop(columns=[c for c in HIDE_COLS if c in sales_df.columns]), width='stretch', hide_index=True)
        else:
            st.info("No sales found.")

    with tab_purchase:
        purchase_df = filtered_history[filtered_history['transaction_type'] == 'purchase']
        st.dataframe(purchase_df.drop(columns=[c for c in HIDE_COLS if c in purchase_df.columns]), width='stretch', hide_index=True)

    with tab_exp:
        st.dataframe(expense_df.drop(columns=[c for c in HIDE_COLS if c in expense_df.columns]), width='stretch', hide_index=True)

    with tab_pay:
        if not payments_df.empty:
            inward_df = payments_df[payments_df['payment_type'] == 'Inward'].drop(columns=[c for c in HIDE_COLS if c in payments_df.columns])
            outward_df = payments_df[payments_df['payment_type'] == 'Outward'].drop(columns=[c for c in HIDE_COLS if c in payments_df.columns])

            st.markdown("### 🟢 Inward Payments (Received from Clients)")
            if not inward_df.empty:
                st.dataframe(inward_df, width='stretch', hide_index=True)
                st.caption(f"Total Received: ₹{inward_df['amount'].sum():,.2f}")
            else:
                st.info("No inward payments recorded yet.")

            st.divider()

            st.markdown("### 🔴 Outward Payments (Paid to Suppliers)")
            if not outward_df.empty:
                st.dataframe(outward_df, width='stretch', hide_index=True)
                st.caption(f"Total Paid: ₹{outward_df['amount'].sum():,.2f}")
            else:
                st.info("No outward payments recorded yet.")
        else:
            st.info("No payments recorded yet.")

    with tab_all:
        st.dataframe(filtered_history.drop(columns=[c for c in HIDE_COLS if c in filtered_history.columns]), width='stretch', hide_index=True)

    # ── EDIT TRANSACTION ──────────────────────────────────────────────────────
    st.divider()
    st.subheader("✏️ Edit a Transaction")

    with st.expander("Select a transaction to edit"):
        if history_df.empty:
            st.info("No transactions found.")
        else:
            edit_df = history_df.copy()
            edit_df['label'] = (
                edit_df['id'].astype(str) + " | " +
                edit_df['date'].astype(str).str[:16] + " | " +
                edit_df['party_name'] + " — " +
                edit_df['item_type'] + " (" +
                edit_df['transaction_type'].str.upper() + ") ₹" +
                edit_df['amount'].abs().astype(int).astype(str)
            )
            selected_edit_label = st.selectbox(
                "Choose transaction",
                ["-- Select --"] + edit_df['label'].tolist(),
                key="edit_selector"
            )

            if selected_edit_label != "-- Select --":
                selected_id = int(selected_edit_label.split(" | ")[0])
                row = edit_df[edit_df['id'] == selected_id].iloc[0]

                with st.container(border=True):
                    c1, c2, c3 = st.columns(3)
                    c1.markdown(f"**Party:** {row['party_name']}")
                    c1.markdown(f"**Site:** {row.get('site_name', '—')}")
                    c2.markdown(f"**Item:** {row['item_type']} ({row['category']})")
                    c2.markdown(f"**Qty:** {abs(row['quantity'])} {row['unit']}")
                    c3.markdown(f"**Rate:** ₹{row['rate']:,.2f}")
                    c3.markdown(f"**Amount:** ₹{abs(row['amount']):,.2f}")

                st.warning("⚠️ Editing will delete the original record and let you re-save it with changes. Stock will be adjusted automatically.")

                if st.button("✏️ Load this transaction for editing", type="primary", width='stretch'):
                    st.session_state.edit_mode_transaction = {
                        "original_id": int(row['id']),
                        "category": row['category'],
                        "item_type": row['item_type'],
                        "unit": row['unit'],
                        "quantity": row['quantity'],
                        "rate": row['rate'],
                        "amount": row['amount'],
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
