import pandas as pd
import sqlite3
from datetime import datetime, timedelta
import streamlit as st
import plotly.express as px

# --- DATABASE SETUP ---
DB_PATH = "inventory.db"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    # Material Transactions Table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT, category TEXT, item_type TEXT, unit TEXT,
            quantity REAL, rate REAL, amount REAL,
            transaction_type TEXT, cash_credit TEXT,
            party_name TEXT, vehicle_name TEXT, site_name TEXT, remarks TEXT
        )
    """)
    # NEW: Stock Summary Table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stock_summary (
            category TEXT, 
            item_type TEXT, 
            unit TEXT, 
            current_stock REAL,
            PRIMARY KEY (category, item_type, unit)
        )
    """)
    # Expenses Table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS expenses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT, expense_type TEXT, amount REAL, remarks TEXT
        )
    """)
    # Payments Table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT, party_name TEXT, payment_type TEXT, 
            amount REAL, remarks TEXT
        )
    """)
    conn.commit()
    conn.close()

def get_connection():
    return sqlite3.connect(DB_PATH)

# --- DB HELPERS ---
def update_stock(category, item_type, unit, qty_change):
    """Updates the stock_summary table: adds if new, updates if exists."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO stock_summary (category, item_type, unit, current_stock)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(category, item_type, unit) 
        DO UPDATE SET current_stock = current_stock + excluded.current_stock
    """, (category, item_type, unit, qty_change))
    conn.commit()
    conn.close()

def insert_transaction(data):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("INSERT INTO transactions (date, category, item_type, unit, quantity, rate, amount, transaction_type, cash_credit, party_name, vehicle_name, site_name, remarks) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", data)
    conn.commit()
    conn.close()
    # TRIGGER STOCK UPDATE: quantity in data is at index 4
    # Note: Your logic stores sales as negative, so we can just add the value
    update_stock(data[1], data[2], data[3], data[4])

def insert_expense(data):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("INSERT INTO expenses (date, expense_type, amount, remarks) VALUES (?, ?, ?, ?)", data)
    conn.commit()
    conn.close()

def insert_payment(data):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("INSERT INTO payments (date, party_name, payment_type, amount, remarks) VALUES (?, ?, ?, ?, ?)", data)
    conn.commit()
    conn.close()

def delete_records(table, record_ids):
    conn = get_connection()
    cur = conn.cursor()
    
    for rid in record_ids:
        if table == "transactions":
            cur.execute("SELECT category, item_type, unit, quantity FROM transactions WHERE id = ?", (rid,))
            row = cur.fetchone()
            if row:
                # Reverse the stock change
                update_stock(row[0], row[1], row[2], -row[3])
                
        cur.execute(f"DELETE FROM {table} WHERE id = ?", (rid,))
    
    conn.commit()
    conn.close()

def load_transactions():
    conn = get_connection()
    df = pd.read_sql_query("SELECT * FROM transactions ORDER BY id DESC", conn)
    conn.close()
    if not df.empty:
        # 1. Convert to datetime object
        df['date_dt'] = pd.to_datetime(df['date'], errors='coerce')
        # 2. Create a clean string version for display (No seconds/ms)
        df['date'] = df['date_dt'].dt.strftime("%Y-%m-%d %H:%M")
    return df

def load_expenses():
    conn = get_connection()
    df = pd.read_sql_query("SELECT * FROM expenses ORDER BY id DESC", conn)
    conn.close()
    if not df.empty:
        df['date_dt'] = pd.to_datetime(df['date'], errors='coerce')
    return df

def load_payments():
    conn = get_connection()
    df = pd.read_sql_query("SELECT * FROM payments ORDER BY id DESC", conn)
    conn.close()
    if not df.empty:
        df['date_dt'] = pd.to_datetime(df['date'], errors='coerce')
    return df

# --- CONFIGURATION ---
INVENTORY_RULES = {
    "Cement": {"JK Strong": {"bag"}, "JK Super": {"bag"}, "Birla Super": {"bag"}, "UltraTech": {"bag"}},
    "Stone/Crusher": {"Khadi": {"pati", "brass"}, "Crush Sand": {"pati", "brass"}, "Plaster Sand": {"pati", "brass"}},
    "Bricks": {"Cement 4\"": {"pcs"}, "Cement 6\"": {"pcs"}, "Red Brick": {"pcs"}},
    "AAC Block": {"AAC 4\"": {"pcs", "cbm"}, "AAC 6\"": {"pcs", "cbm"}},
    "Chemicals": {"Tile Chemical": {"bag"}, "Waterproofing": {"litre", "kg"}}
}

EXPENSE_TYPES = ["Staff Salary", "Diesel", "Maintenance", "Shop Rent", "Other"]
DASHBOARD_PASSWORD = 'sunny123'

# --- APP CONFIG & PRINT CSS ---
st.set_page_config(page_title="Inventory Pro", layout="wide")

st.markdown("""
    <style>
    @media print {
        header, [data-testid="stSidebar"], .stButton, .stRadio, .stSelectbox, .stDateInput, .stTextInput, hr, .no-print {
            display: none !important;
        }
        .main .block-container {
            padding-top: 0rem !important;
        }
        .printable-bill {
            border: 1px solid #eee !important;
            padding: 20px !important;
        }
    }
    </style>
""", unsafe_allow_html=True)

init_db()

if 'cart' not in st.session_state:
    st.session_state.cart = []

# --- DATA LOADING (Fresh load every rerun) ---
history_df = load_transactions()
expense_df = load_expenses()
payments_df = load_payments()

# --- SIDEBAR NAV ---
st.sidebar.title("🚀 Navigation")
page = st.sidebar.radio("Go to", ["Business Dashboard", "New Transaction", "Add Expenses", "View History", "Stock & Credit", "Bill Generator"])

# --- PAGE: BUSINESS DASHBOARD ---
if page == "Business Dashboard":
    st.title("📈 Business Intelligence Dashboard")
    
    # Password Protection
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

        # Filtering for dashboard metrics only
        f_hist = history_df[history_df['date_dt'] >= start_date] if not history_df.empty else history_df
        f_exp = expense_df[expense_df['date_dt'] >= start_date] if not expense_df.empty else expense_df

        total_sales = f_hist[f_hist['transaction_type'] == 'sale']['amount'].sum()
        total_purchase = f_hist[f_hist['transaction_type'] == 'purchase']['amount'].sum()
        total_operating_exp = f_exp['amount'].sum()
        estimated_profit = total_sales - total_purchase - total_operating_exp

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Revenue (Sales)", f"₹{total_sales:,.2f}")
        m2.metric("Procurement (Purchase)", f"₹{total_purchase:,.2f}")
        m3.metric("Operating Expenses", f"₹{total_operating_exp:,.2f}")
        m4.metric("Estimated Net Profit", f"₹{estimated_profit:,.2f}")

        st.divider()
        
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Sales vs. Expenses Trend")
            if not f_hist.empty:
                sales_trend = f_hist[f_hist['transaction_type'] == 'sale'].groupby(f_hist['date_dt'].dt.date)['amount'].sum().reset_index()
                fig = px.line(sales_trend, x='date_dt', y='amount', title="Daily Revenue")
                st.plotly_chart(fig, use_container_width=True)
        
        with c2:
            st.subheader("Cash vs. Credit Sales Split")
            if not f_hist.empty:
                split = f_hist[f_hist['transaction_type'] == 'sale'].groupby('cash_credit')['amount'].sum().reset_index()
                fig2 = px.pie(split, values='amount', names='cash_credit', hole=0.4)
                st.plotly_chart(fig2, use_container_width=True)
    else:
        if pwd_input != "":
            st.error("❌ Incorrect Password. Access Denied.")
        else:
            st.warning("🔒 Please enter the password in the sidebar to view sensitive financial data.")

# --- PAGE: NEW TRANSACTION ---
elif page == "New Transaction":
    st.title("📝 New Transaction")
    
    with st.expander("👤 Bill Header Details (Party & Site)", expanded=True):
        h_col1, h_col2, h_col3 = st.columns(3)
        with h_col1:
            existing_parties = sorted(history_df['party_name'].unique().tolist()) if not history_df.empty else []
            sel_party = st.selectbox("Party Name", ["-- New Party --"] + existing_parties)
            party_name = st.text_input("Enter New Party") if sel_party == "-- New Party --" else sel_party
        with h_col2:
            existing_sites = sorted(history_df['site_name'].dropna().unique().tolist()) if not history_df.empty else []
            sel_site = st.selectbox("Site Location", ["-- New Site --"] + existing_sites)
            site_name = st.text_input("Enter New Site") if sel_site == "-- New Site --" else sel_site
        with h_col3:
            vehicle = st.text_input("Vehicle Number", placeholder="e.g. GJ-01-XX-0000")

    st.subheader("🛒 Add Items")
    with st.container(border=True):
        i_col1, i_col2, i_col3, i_col4 = st.columns([2, 2, 1, 1])
        with i_col1:
            cat = st.selectbox("Category", list(INVENTORY_RULES.keys()))
            item = st.selectbox("Item Type", list(INVENTORY_RULES[cat].keys()))
        with i_col2:
            unit = st.selectbox("Unit", list(INVENTORY_RULES[cat][item]))
            t_type = st.radio("Type", ["sale", "purchase"], horizontal=True)
        with i_col3:
            qty = st.number_input("Quantity", min_value=0.1, step=1.0)
            rate = st.number_input("Rate", min_value=0.0, step=1.0)
        with i_col4:
            pay_mode = st.selectbox("Payment", ["cash", "credit"])
            st.write(f"**Total:**")
            st.write(f"₹{qty * rate:,.2f}")

        remarks = st.text_input("Remarks")
        
        if st.button("➕ Add Item to Bill", use_container_width=True, type="secondary"):
            if not party_name:
                st.error("Please specify a Party Name")
            else:
                signed_qty = qty if t_type == "purchase" else -qty
                st.session_state.cart.append({
                    "category": cat, "item_type": item, "unit": unit,
                    "quantity": signed_qty, "rate": rate, "amount": qty * rate,
                    "transaction_type": t_type, "cash_credit": pay_mode,
                    "party_name": party_name, "vehicle_name": vehicle, 
                    "site_name": site_name, "remarks": remarks
                })
                st.toast("Item added!")

    if st.session_state.cart:
        st.divider()
        st.subheader("Current Bill Preview")
        cart_df = pd.DataFrame(st.session_state.cart)
        st.table(cart_df[["transaction_type", "item_type", "quantity", "unit", "rate", "amount"]])
        
        total_amt = cart_df['amount'].sum()
        st.metric("Total Bill Value", f"₹{total_amt:,.2f}")
        
        c1, c2 = st.columns([1, 5])
        if c1.button("💾 Save Bill", type="primary"):
            now = datetime.now().strftime("%Y-%m-%d %H:%M")
            for entry in st.session_state.cart:
                insert_transaction((now, entry['category'], entry['item_type'], entry['unit'], 
                                   entry['quantity'], entry['rate'], entry['amount'], 
                                   entry['transaction_type'], entry['cash_credit'], 
                                   entry['party_name'], entry['vehicle_name'], entry['site_name'], entry['remarks']))
            st.session_state.cart = []
            st.success("Transaction recorded successfully!")
            st.rerun()
        if c2.button("🗑️ Clear"):
            st.session_state.cart = []
            st.rerun()

# --- PAGE: ADD EXPENSES ---
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
            
        if st.button("💾 Save Expense", type="primary", use_container_width=True):
            if exp_amt > 0:
                insert_expense((exp_date.strftime("%Y-%m-%d %H:%M"), exp_type, exp_amt, exp_rem))
                st.success(f"Successfully recorded ₹{exp_amt} for {exp_type}")
                st.rerun()

# --- PAGE: VIEW HISTORY ---
elif page == "View History":
    st.title("📜 History Ledger")
    search = st.text_input("🔍 Search globally")
    
    tab_sale, tab_purchase, tab_exp, tab_pay, tab_all = st.tabs(["💰 Sales", "🛒 Purchases", "💸 Expenses", "💳 Payments", "📑 All Transactions"])

    filtered_history = history_df
    if search:
        filtered_history = history_df[history_df.apply(lambda row: search.lower() in row.astype(str).str.lower().values, axis=1)]

    with tab_sale:
        sales_df = filtered_history[filtered_history['transaction_type'] == 'sale'].copy()
        if not sales_df.empty:
            sales_df['quantity'] = sales_df['quantity'].abs()
            st.dataframe(sales_df, use_container_width=True, hide_index=True)

    with tab_purchase:
        purchase_df = filtered_history[filtered_history['transaction_type'] == 'purchase']
        st.dataframe(purchase_df, use_container_width=True, hide_index=True)

    with tab_exp:
        st.dataframe(expense_df, use_container_width=True, hide_index=True)
    
    with tab_pay:
        st.dataframe(payments_df, use_container_width=True, hide_index=True)

    with tab_all:
        st.dataframe(filtered_history, use_container_width=True, hide_index=True)

    st.divider()
    st.subheader("🗑️ Universal Delete Tool")
    
    with st.expander("Danger Zone - Remove Records"):
        target_table = st.selectbox(
            "1. Select Category", 
            ["transactions", "expenses", "payments"], 
            format_func=lambda x: x.capitalize()
        )

        # Fetch the relevant dataframe based on selection for the dropdown
        if target_table == "transactions":
            df_for_delete = history_df.copy()
            # Create a label for the dropdown so the user knows what they are deleting
            df_for_delete['label'] = df_for_delete['id'].astype(str) + ": " + df_for_delete['party_name'] + " - " + df_for_delete['item_type'] + " (₹" + df_for_delete['amount'].astype(str) + ")"
        elif target_table == "expenses":
            df_for_delete = expense_df.copy()
            df_for_delete['label'] = df_for_delete['id'].astype(str) + ": " + df_for_delete['expense_type'] + " (₹" + df_for_delete['amount'].astype(str) + ")"
        else:
            df_for_delete = payments_df.copy()
            df_for_delete['label'] = df_for_delete['id'].astype(str) + ": " + df_for_delete['party_name'] + " [" + df_for_delete['payment_type'] + "] (₹" + df_for_delete['amount'].astype(str) + ")"

        # 2. Select specific records via a multiselect
        selected_labels = st.multiselect(f"2. Select {target_table.capitalize()} to Delete", df_for_delete['label'].tolist())

        # Extract IDs from the selected labels
        ids_to_delete = [int(label.split(":")[0]) for label in selected_labels]

        if ids_to_delete:
            st.warning(f"⚠️ You are about to permanently delete {len(ids_to_delete)} record(s). This cannot be undone.")
            if target_table == "transactions":
                st.info("Stock levels will be automatically adjusted.")
            
            if st.button(f"Confirm Delete {len(ids_to_delete)} Records", type="primary", use_container_width=True):
                # We update your existing delete_records function (plural) 
                # which you already have defined at the top of your code
                delete_records(target_table, ids_to_delete)
                st.success(f"Successfully removed selected records from {target_table}.")
                st.rerun()

# --- PAGE: STOCK & CREDIT ---
elif page == "Stock & Credit":
    st.title("📊 Inventory & Financials")
    
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

    # RECORD PAYMENT UI
    with st.expander("💳 Record New Payment Settlement", expanded=False):
        if not outstanding_parties:
            st.info("No parties currently have an outstanding balance.")
        else:
            p_col1, p_col2, p_col3 = st.columns(3)
            with p_col1:
                pay_party = st.selectbox("Select Party with Balance", outstanding_parties)
            with p_col2:
                pay_type = st.radio("Type", ["Received (from Client)", "Paid (to Dealer)"])
                db_type = "Inward" if "Received" in pay_type else "Outward"
            with p_col3:
                pay_amt = st.number_input("Amount (₹)", min_value=0.0)
            
            pay_rem = st.text_input("Payment Remarks")
            if st.button("Save Payment Entry", type="primary"):
                if pay_amt > 0:
                    insert_payment((datetime.now().strftime("%Y-%m-%d %H:%M"), pay_party, db_type, pay_amt, pay_rem))
                    st.success(f"Payment recorded for {pay_party}!")
                    st.rerun()

    tab_fin, tab_stock = st.tabs(["💳 Credit & Balances", "📦 Available Stock"])
    
    with tab_fin:
        m1, m2, m3 = st.columns(3)
        m1.metric("Total Net Receivable", f"₹{total_rec:,.2f}")
        m2.metric("Total Net Payable", f"₹{total_pay:,.2f}")
        m3.metric("Net Market Balance", f"₹{total_rec - total_pay:,.2f}")
        
        st.divider()
        if detailed_list:
            det_df = pd.DataFrame(detailed_list)
            st.markdown("### 🟢 Client Receivables (Money to Come)")
            st.dataframe(det_df[det_df['Pending Receivable (Client)'] > 0][["Party Name", "Pending Receivable (Client)"]], use_container_width=True, hide_index=True)
            st.markdown("### 🔴 Supplier Payables (Money to Pay)")
            st.dataframe(det_df[det_df['Pending Payable (Supplier)'] > 0][["Party Name", "Pending Payable (Supplier)"]], use_container_width=True, hide_index=True)
        else:
            st.success("All balances are clear!")


    with tab_stock:
        st.subheader("📦 Live Inventory Levels")
        
        # UI Filters
        s_col1, s_col2 = st.columns(2)
        with s_col1:
            stock_cat = st.selectbox("Filter Category", ["All"] + list(INVENTORY_RULES.keys()), key="stock_cat_sel")
        with s_col2:
            if stock_cat != "All":
                stock_item = st.selectbox("Filter Item Type", ["All"] + list(INVENTORY_RULES[stock_cat].keys()), key="stock_item_sel")
            else:
                stock_item = "All"

        # Load from the new optimized table (Very fast)
        conn = get_connection()
        stock_df = pd.read_sql_query("SELECT * FROM stock_summary", conn)
        conn.close()

        if not stock_df.empty:
            # Apply filters in Pandas
            if stock_cat != "All":
                stock_df = stock_df[stock_df['category'] == stock_cat]
            if stock_item != "All":
                stock_df = stock_df[stock_df['item_type'] == stock_item]
            
            # Formatting for display
            st.dataframe(
                stock_df.rename(columns={
                    'category': 'Category', 
                    'item_type': 'Item', 
                    'unit': 'Unit', 
                    'current_stock': 'Balance'
                }),
                use_container_width=True, 
                hide_index=True
            )
        else:
            st.info("No stock data recorded yet. New transactions will populate this automatically.")

# --- PAGE: BILL GENERATOR ---
elif page == "Bill Generator":
    st.title("🧾 Smart Bill Generator")
    if not history_df.empty:
        mode = st.radio("Choose Mode:", ["Filter by Party & Date", "Enter Transaction IDs Manually"], horizontal=True)
        bill_data = pd.DataFrame() 

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
                if sel_s != "All Sites": bill_data = bill_data[bill_data['site_name'] == sel_s]
                if len(date_range) == 2:
                    bill_data = bill_data[(bill_data['date_dt'].dt.date >= date_range[0]) & (bill_data['date_dt'].dt.date <= date_range[1])]

        else:
            # MANUAL ID MODE WITH REFERENCE TABLE
            st.markdown('<div class="no-print">', unsafe_allow_html=True)
            with st.expander("🔍 Search Transaction IDs (Reference Table)", expanded=True):
                ref_search = st.text_input("Filter Reference Table (by Party, Item, etc.)")
                ref_df = history_df.copy()
                if ref_search:
                    ref_df = ref_df[ref_df.apply(lambda row: ref_search.lower() in row.astype(str).str.lower().values, axis=1)]
                
                # Show reference table so user can find IDs easily
                st.dataframe(
                    ref_df[['id', 'date', 'party_name', 'item_type', 'quantity', 'amount', 'transaction_type']], 
                    use_container_width=True, 
                    hide_index=True,
                    height=250
                )
            
            ids = st.text_input("Enter Transaction IDs (e.g. 1, 5, 8)", placeholder="Enter IDs separated by commas")
            st.markdown('</div>', unsafe_allow_html=True)
            
            if ids:
                try:
                    id_list = [int(i.strip()) for i in ids.split(",") if i.strip().isdigit()]
                    bill_data = history_df[history_df['id'].isin(id_list)].copy()
                except:
                    st.error("Invalid ID format. Please use numbers separated by commas.")

        if not bill_data.empty:
            st.divider()
            st.markdown('<div class="printable-bill">', unsafe_allow_html=True)
            with st.container(border=True):
                # Header info
                party_label = bill_data.iloc[0]['party_name']
                st.markdown(f"### Client: {party_label}")
                st.write(f"**Generated On:** {datetime.now().strftime('%Y-%m-%d %H:%M')}")
                
                # Formatting the table for display
                display_bill = bill_data[['date', 'item_type', 'quantity', 'unit', 'rate', 'amount']].copy()
                # Ensure values are absolute/positive for billing
                display_bill['quantity'] = display_bill['quantity'].abs()
                display_bill['amount'] = display_bill['amount'].abs()
                
                st.table(display_bill)
                
                # Total amount (always positive)
                total_val = display_bill['amount'].sum()
                st.markdown(f"## Total Bill Amount: ₹{total_val:,.2f}")
            st.markdown('</div>', unsafe_allow_html=True)

            st.info("💡 Press **Ctrl + P** to print.")
