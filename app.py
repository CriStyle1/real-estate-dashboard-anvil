import streamlit as st
from datetime import datetime, timedelta
from google_drive_module import GoogleSheet, TodoManager
import os
import pandas as pd
import pytz
from streamlit.components.v1 import html
from streamlit_scroll_to_top import scroll_to_here

# --- APARTMENT CODES FOR AUTOCOMPLETE ---
ALL_AP_CODES = [
    "WP60-082", "WP60-043", "WP60-028", "WP60-029", "WP60-051", "WP60-061", 
    "WP60-085", "WP60-096", "WP60-121", "WP49-009", "WP49-023", "WP49-041", 
    "WP49-059", "WP49-068", "WP14-019", "WP14-037", "WP14-062", "WP14-011", 
    "WP14-018", "WP14-025", "WP14-075", "WP14-080", "WP14-081", "WP14-084", 
    "WP14-064", "WP14-065", "WP12-018", "WP12-019", "WP12-039", "WP12-031-1", 
    "WP12-049", "WP12-023", "WP12-017", "WP12-031-2", "WP12-043", "WP12-014", 
    "WP12-040", "WP6-019", "WP6-020", "RE87-069", "RE60-017", "RE56-423", 
    "RE58-108", "CR48-024", "CR48-052", "CR42-030", "SA3941-014", "SA3941-010", 
    "SA56-024", "POL72-020", "POL72-033", "POL72-046", "POL72-048", "POL72-052", 
    "POL72-056", "POL72-065-1A", "POL72-069", "POL72-098", "POL72-112", "POL72-117", 
    "POL72-161", "POL72-006", "POL72-014", "POL72-015", "POL72-024", "POL72-026", 
    "POL72-045", "POL72-065-1B", "POL72-068", "POL72-090", "POL72-095", "POL72-105", 
    "POL72-107", "POL72-109", "POL72-119", "POL72-120", "POL72-132", "POL72-133", 
    "POL72-135", "POL72-136", "POL72-138", "POL72-153", "POL72-157", "POL72-158", 
    "POL72-057", "POL72-040", "POL72-211", "POL72-601", "POL72-203", "POL72-110", 
    "POL72-210", "POL72-301", "OR9-046", "OR30-008", "VI15-024", "TI81-022", 
    "TI81-025", "TI81-080", "TI81-018", "TI81-037", "TI81-051", "SO25-010", 
    "SO25-012", "TI85-006", "TI85-012", "TI85-403", "TI85-619", "TI85-602", 
    "TI19-055", "TI24-051", "TI35-032", "TI35-042", "TI35-103", "TI35-081", 
    "TI35-045", "TI35-011", "TI45-016", "TI65-035", "TI63-044", "TI20-053", 
    "TI17-021-1", "TI17-085", "TI17-175", "TI17-135-9", "TI17-136-9", "TI17-007", 
    "TI2-132", "TI2-156", "TI2-160", "DC371-045", "DC371-055", "IP102-001", 
    "IP102-022", "IP102-040", "IP88-029", "IP84-314", "IP84-618", "IP84-720", 
    "IP84-702", "IP84-808", "IP84-508", "IP84-009", "IP84-420", "IP86-708", 
    "IP86-507", "IP86-611", "IP86-210", "IP48-036", "IP48-062", "IP48-052", 
    "IP48-056", "IP40-024", "IP40-055", "IP40-059", "IP39-035", "IP19-001", 
    "IP19-007", "PA8-021", "PA8-013", "PA8-033", "PA8-010", "ST9-003", "ST9-046", 
    "TI17-174", "TI17-176", "TI17-187", "TI17-083", "TI17-018", "TI17-019", 
    "TI17-020", "TI17-021-11", "TI17-022", "TI17-023", "TI17-024", "TI17-025", 
    "TI17-026", "TI17-027", "TI17-028", "TI17-029", "TI17-030", "TI17-031", 
    "TI17-032", "TI17-033", "TI17-034", "TI17-120", "TI17-121", "TI17-122", 
    "TI17-123", "TI17-124", "TI17-125", "TI17-126", "TI17-127", "TI17-128", 
    "TI17-129", "TI17-130", "TI17-131", "TI17-132", "TI17-133", "TI17-134", 
    "TI17-135-11", "TI17-136-11", "TI35-023", "TI35-051", "TI35-053", "TI35-113", 
    "TI35-083", "TI35-098", "TI35-021", "TI35-126", "TI35-128", "IP3-012", 
    "IP3-020", "IP3-029", "IP3-037", "IP3-052", "IP3-045", "IP3-053", "IP3-021", 
    "AM58-019", "TI2-022", "TI35-062", "TI35-079", "TI35-096", "TI35-130", 
    "TI35-073", "TI35-106", "TI35-107", "IP86-612"
]

# Create credentials.json from the first secret
if not os.path.exists("credentials.json"):
    # Get the secret from the Streamlit secrets manager
    creds_json_str = st.secrets["google_credentials_json"]
    # Write the secret to a file named credentials.json
    with open("credentials.json", "w") as f:
        f.write(creds_json_str)

# Create token.json from the second secret
if not os.path.exists("token.json"):
    token_json_str = st.secrets["google_token_json"]
    with open("token.json", "w") as f:
        f.write(token_json_str)
# --- END OF AUTHENTICATION CODE ---

# --- Page Configuration with Custom Styling ---
st.set_page_config(
    page_title="Real Estate Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling with dark mode support
st.markdown(
    """
    <style>
    /* Remove padding from the main content container to allow full-width elements */
    .main .block-container {
        padding: 0 !important;
    }
    /* Add padding back to all direct children of the main container, except our sticky one */
    .main .block-container > div > [data-testid="stVerticalBlock"] > div:not(:has(div.control-strip-marker)) {
        padding: 2rem 1rem;
    }

    /* --- FINAL, CORRECT STICKY SELECTOR --- */
    div[data-testid="stVerticalBlock"] div:has(div.control-strip-marker) {
        position: sticky;
        top: 40px;
        background-color: #ffffff;
        z-index: 999;
        padding-top: 1px; /* Reduced padding */
        padding-bottom: 1px; /* Reduced padding */
        border-bottom: 0px solid #444; /* A subtle line to separate it */
    }

    /* --- Quick Stats Redesign --- */
    .stat-box { background-color: #f1f3f5; border-radius: 8px; padding: 10px 15px; text-align: center; }
    .stat-box .stat-label { font-size: 0.8em; color: #555; margin-bottom: 5px; }
    .stat-box .stat-value { font-size: 1.5em; font-weight: 600; color: #111; }
    .stat-box .stat-sub-value { font-size: 0.9em; font-weight: 600; color: #111; }

    /* --- NEW: Home Page Metric Box Styling --- */
    .metric-box {
        background-color: #f1f3f5;
        border-radius: 8px;
        padding: 15px;
        text-align: left;
        border-left: 5px solid #6ab187;
    }
    .metric-box .metric-label {
        font-size: 0.9em;
        color: #555;
        font-weight: 500;
    }
    .metric-box .metric-value {
        font-size: 2.2em;
        font-weight: 600;
        color: #111;
    }

    /* --- Dark Mode Adjustments --- */
    @media (prefers-color-scheme: dark) {
        .main .block-container { background-color: #0E1117; } /* Match Streamlit's dark bg */
        div[data-testid="stVerticalBlock"] div:has(div.control-strip-marker) {
            background-color: #0E1117; /* Match Streamlit's dark bg */
            border-bottom: 1px solid #40444b;
        }
        .stat-box { background-color: #40444b; }
        .stat-box .stat-label { color: #aaa; }
        .stat-box .stat-value, .stat-box .stat-sub-value { color: #fff; }
        .stHeader { color: #e0e0e0; border-bottom-color: #6ab187; }
        .stSubheader { color: #d1d1d1; }
        .stTextInput > div > div > input, .stDateInput > div > div > input { background-color: #40444b; color: #ffffff; border-color: #6ab187; }
        .stButton>button { background-color: #6ab187; color: white; }
        .stButton>button:hover { background-color: #5a9f7a; }
        .stExpander { background-color: #40444b; border-color: #5a5e63; color: #ffffff; }
        .stMarkdown { color: #e0e0e0; }
        div[data-testid="stRadio"] > label:hover { background-color: #40444b; color: #fff; }
        div[data-testid="stRadio"] > div[aria-checked="true"] > label { background-color: #5a9f7a; color: white !important; }
        .metric-box {
            background-color: #262730;
            border-left: 5px solid #5a9f7a;
        }
        .metric-box .metric-label {
            color: #aaa;
        }
        .metric-box .metric-value {
            color: #fff;
        }
    }

    /* --- Modern Sidebar Styling --- */
    div[data-testid="stRadio"] > label { display: block; padding: 10px 15px; margin-bottom: 5px; border-radius: 8px; transition: background-color 0.2s, color 0.2s; cursor: pointer; font-weight: 500; }
    div[data-testid="stRadio"] > label:hover { background-color: #e9ecef; color: #000; }
    div[data-testid="stRadio"] > div[aria-checked="true"] > label { background-color: #6ab187; color: white !important; }
    div[data-testid="stRadio"] input[type="radio"] { display: none; }

    /* --- General App Styling --- */
    .stHeader { color: #2c3e50; font-size: 2em; font-weight: 600; text-align: center; padding: 10px; border-bottom: 2px solid #6ab187; margin-bottom: 20px; }
    .stSubheader { color: #34495e; font-size: 1.3em; margin-top: 15px; font-weight: 500; }
    .stExpander { border: 1px solid #e9ecef; border-radius: 4px; padding: 10px; }
    .stTextInput > div > div > input, .stDateInput > div > div > input { border: 2px solid #6ab187; border-radius: 4px; padding: 6px; }
    .stButton>button { background-color: #6ab187; color: white; border: none; padding: 8px 16px; border-radius: 4px; transition: background-color 0.3s; }
    .stButton>button:hover { background-color: #5a9f7a; }
    div[data-testid="stVerticalBlock"] div[data-testid="stMarkdown"] strong > code { font-size: 1.1em !important; font-weight: 600 !important; }
    
    div[data-testid="stExpander"] h3, 
    div[data-testid="stExpander"] hr {
        margin-top: 0.5rem !important;
        margin-bottom: 0.5rem !important;
    }

    /* Enhanced scrollable tabs for small screens */
    div[data-testid="stTabs"] [role="tablist"] {
        overflow-x: auto;
        white-space: nowrap;
        display: flex;
        flex-wrap: nowrap;
        -webkit-overflow-scrolling: touch; /* Smooth scrolling on iOS */
        scrollbar-width: thin; /* Firefox */
        scrollbar-color: #6ab187 #40444b; /* Firefox - thumb and track color */
    }

    /* Webkit browsers (Chrome, Safari, Edge) scrollbar styling */
    div[data-testid="stTabs"] [role="tablist"]::-webkit-scrollbar {
        height: 8px;
    }

    div[data-testid="stTabs"] [role="tablist"]::-webkit-scrollbar-track {
        background: #f1f1f1;
        border-radius: 10px;
    }

    div[data-testid="stTabs"] [role="tablist"]::-webkit-scrollbar-thumb {
        background: #6ab187;
        border-radius: 10px;
    }

    div[data-testid="stTabs"] [role="tablist"]::-webkit-scrollbar-thumb:hover {
        background: #5a9f7a;
    }

    /* Dark mode scrollbar */
    @media (prefers-color-scheme: dark) {
        div[data-testid="stTabs"] [role="tablist"]::-webkit-scrollbar-track {
            background: #40444b;
        }
    }

    /* Ensure tab buttons don't shrink */
    div[data-testid="stTabs"] button[role="tab"] {
        flex-shrink: 0;
        min-width: fit-content;
    }

    /* --- NEW: Remove border from specific expanders --- */
    div[data-testid="stExpander"] {
        border: none !important;
        background-color: #212121 !important;
        padding: 0 !important;
    }
    div[data-testid="stExpander"] summary {
        font-size: 1.1em;
        font-weight: 500;
    }

    </style>
    """,
    unsafe_allow_html=True
)

# Load translations from external CSV
@st.cache_data
def load_translations(language):
    try:
        df = pd.read_csv("translations.csv", encoding="utf-8")
        if language in df.columns:
            return dict(zip(df['key'], df[language]))
        else:
            raise ValueError(f"Language '{language}' not supported")
    except FileNotFoundError:
        raise FileNotFoundError("translations.csv is missing")
    except pd.errors.ParserError as e:
        raise pd.errors.ParserError(f"Failed to parse translations.csv: {str(e)}")
    except Exception as e:
        raise Exception(f"Unexpected error: {str(e)}")

# --- Initialize Session State ---
if 'selected_ap_code' not in st.session_state:
    st.session_state.selected_ap_code = None
if 'action_result' not in st.session_state:
    st.session_state.action_result = None
if 'todo_list' not in st.session_state:
    st.session_state.todo_list = []    
if 'selected_agent' not in st.session_state:
    st.session_state.selected_agent = "Khrystyna Markin"
if 'todo_view' not in st.session_state:
    st.session_state.todo_view = "Full List" 
if 'util_ap_code' not in st.session_state:
    st.session_state.util_ap_code = None
if 'util_include_rent' not in st.session_state:
    st.session_state.util_include_rent = False
if 'util_last_result' not in st.session_state:
    st.session_state.util_last_result = None   

# --- Caching Managers ---
@st.cache_resource
def get_sheet_manager():
    print("--- INITIALIZING GOOGLE SHEET MANAGER (This should only run once!) ---")
    sheets_to_load = ["APARTMENTS", "UT_DATA", "EMAIL_LOG", "ZET", "STR", "MO_DATA", "CL_DATA"]
    sheet_manager = GoogleSheet(spreadsheet_name="APARTMENTS", sheet_names=sheets_to_load)
    sheet_manager.load_data()
    return sheet_manager

sheet_manager = get_sheet_manager()

@st.cache_resource
def get_todo_manager(_sheet_manager):
    print("--- INITIALIZING TODO MANAGER (This should only run once!) ---")
    return TodoManager(_sheet_manager)

todo_manager = get_todo_manager(sheet_manager)

# --- Auto-Sync Logic ---
if 'last_sync_time' not in st.session_state:
    st.session_state.last_sync_time = None

def run_exchange_rate_sync():
    print("--- [AUTO-SYNC] Running exchange rate sync... ---")
    success, message = sheet_manager.sync_exchange_rate(silent=True)
    if success:
        print("--- [AUTO-SYNC] Success: Exchange rate updated ---")
        st.session_state.last_sync_time = datetime.now(pytz.utc)
    else:
        print("--- [AUTO-SYNC] Failed: Could not update exchange rate ---")

now = datetime.now(pytz.utc)
if st.session_state.last_sync_time is None or (now - st.session_state.last_sync_time) > timedelta(minutes=30):
    run_exchange_rate_sync()

# --- Cached Wrappers for Expensive Operations ---
@st.cache_data(ttl=600)
def cached_calculate_utilities(ap_code, include_rent):
    return sheet_manager.calculate_and_format_utilities(ap_code, include_rent=include_rent)

@st.cache_data
def cached_master_report(ap_code):
    return sheet_manager.generate_master_report(ap_code)

@st.cache_data
def cached_email_log(ap_code):
    return sheet_manager.format_email_log(ap_code)

@st.cache_data
def cached_batch_report(codes_to_process):
    return sheet_manager.generate_batch_report(codes_to_process)

@st.cache_data
def cached_find_ap_by_email(email):
    return sheet_manager.find_ap_code_by_email(email)

@st.cache_data
def cached_find_email_by_ap_code(ap_code):
    return sheet_manager.find_email_by_ap_code(ap_code)

@st.cache_data
def cached_get_apartment_data(ap_code):
    return sheet_manager.get_apartment_data(ap_code)


# --- DEDICATED, HIGH-QUALITY FORMATTERS ---
def display_utility_summary(summary_string):
    if summary_string is None:
        st.error("Could not generate utility summary. There may not be enough data.")
        return
    st.subheader("Utility Calculation Summary")
    lines = summary_string.strip().split('\n')
    
    # The first line now contains the full title with the date range
    st.markdown(f"**{lines[0]}**")
    
    # The rest of the function displays the breakdown
    col1, col2 = st.columns(2)
    i = 0
    for line in lines[1:]:
        if ':' in line:
            parts = line.split(':', 1)
            key = parts[0].strip().replace('•', '').strip()
            value = parts[1].strip()
            target_col = col1 if i % 2 == 0 else col2
            with target_col:
                st.markdown(f"**{key}:**")
                st.code(value, language=None)
            i += 1
        elif "GRAND TOTAL" in line:
            st.markdown(f"**{line}**")
            
    with st.expander("Copy Raw Summary Text"):
        st.code(summary_string)

def display_master_report(report_string):
    sections = report_string.split('- - - - - - - - - - - - - - - - - - - -')
    for section in sections:
        if not section.strip(): continue
        lines = [line.strip() for line in section.strip().split('\n') if line.strip()]
        st.subheader(lines[0])
        data_dict = {}
        for line in lines[1:]:
            if ':' in line:
                parts = line.split(':', 1)
                key = parts[0].replace('•', '').strip()
                value = parts[1].strip()
                data_dict[key] = value
        if data_dict:
            num_items = len(data_dict)
            num_cols = 2 if num_items > 3 else 1
            cols = st.columns(num_cols)
            col_index = 0
            for key, value in data_dict.items():
                cols[col_index].markdown(f"**{key}:** {value}")
                col_index = (col_index + 1) % num_cols
        st.markdown("---")

def display_client_comm_result(result_string):
    result_string = result_string.replace("✅ **Success!**", "").replace("❌ **Data Mismatch!**", "").strip()
    lines = result_string.split('\n')
    for line in lines:
        line = line.replace('•', '').strip()
        if ':' in line:
            parts = line.split(':', 1)
            st.markdown(f"**{parts[0].strip()}:** `{parts[1].strip()}`")

def display_email_log(log_string):
    st.subheader(log_string.split('\n')[0])
    emails = log_string.split('--- Email')[1:]
    for email in emails:
        with st.container(border=True):
            lines = email.strip().split('\n')
            summary_lines = []
            in_summary = False
            for line in lines[1:]:
                if line.startswith("• Summary:"):
                    in_summary = True
                    continue
                if in_summary:
                    summary_lines.append(line.replace("```", ""))
                elif ':' in line:
                    parts = line.split(':', 1)
                    # THIS IS THE FIX: We operate on the elements of the list, not the list itself.
                    key = parts[0].replace('•', '').strip() # Get the first part for the key
                    value = parts[1].strip() # Get the second part for the value
                    if "Link" in key:
                        st.markdown(f"**{key}:** {value}")
                    else:
                        st.markdown(f"**{key}:** `{value}`")
            if summary_lines:
                st.markdown("**Summary:**")
                st.code("\n".join(summary_lines), language=None)

# --- SIDEBAR FOR NAVIGATION ---
st.sidebar.title("🏠 Navigation")
st.sidebar.markdown("---")
language = st.sidebar.selectbox("🌐 Язык / Language", ["en", "ru"], index=0)
lang_dict = load_translations(language)
menu_icons = {
    lang_dict['menu_home']: "🏠",
    lang_dict['menu_apartment_info']: "🕵️",
    "✅ To-Do List": "✅",
    lang_dict['menu_utilities']: "🖩",
    lang_dict['menu_reports']: "📜",
    lang_dict['menu_communication']: "📧",
    lang_dict['menu_settings']: "⚙️"
}

menu_choice = st.sidebar.radio(
    "Main menu", 
    list(menu_icons.keys()), # Use the keys from our new dictionary
    format_func=lambda x: f"{menu_icons[x]} {x.replace('✅ ', '').replace('🏠 ', '')}", # Format with the correct icon
    label_visibility="collapsed"
)
st.sidebar.markdown("---")
if st.sidebar.button(lang_dict['settings_refresh_data']): # Using translation key
    with st.spinner("Force reloading all data from Google Sheets..."):
        st.cache_data.clear() # Clear the app's function cache
        _, message = sheet_manager.reload_all_data()
        st.success(message)
    st.rerun()

if st.sidebar.button("💥 Restart App"):
    st.cache_resource.clear()
    st.cache_data.clear()
    st.session_state.clear()
    st.rerun()
st.sidebar.markdown("---")
st.sidebar.subheader("Data Status")
bucharest_tz = pytz.timezone("Europe/Bucharest")
sheet_ts = sheet_manager.sheet_data_timestamp
if sheet_ts:
    sheet_ts_bucharest = sheet_ts.astimezone(bucharest_tz)
    st.sidebar.caption(f"Sheets Last Updated: **{sheet_ts_bucharest.strftime('%H:%M:%S')}**")
else:
    st.sidebar.caption("Sheets Last Updated: **Never**")
st.sidebar.caption(f"Gas Tariff: **{sheet_manager.settings['gas_tariff']}**")
st.sidebar.caption(f"Electricity Tariff: **{sheet_manager.settings['electricity_tariff']}**")
st.sidebar.caption(f"EUR to RON Rate: **{sheet_manager.settings['eur_to_ron_rate']}**")

# --- MAIN PAGE CONTENT ---
st.markdown(f"<h1 class='stHeader'>{lang_dict['main_title']}</h1>", unsafe_allow_html=True)

# --- NEW: HOME PAGE ---
if menu_choice == lang_dict['menu_home']:
    st.markdown(f"<h2 class='stSubheader'>{lang_dict['home_header']}</h2>", unsafe_allow_html=True)
    st.markdown(lang_dict['home_subheader'])
    
    # --- COLLAPSIBLE TOTAL COMPANY METRICS ---
    with st.expander(lang_dict['home_company_metrics_expander']):
        total_metrics = sheet_manager.calculate_key_metrics()
        if total_metrics:
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.markdown(f"""
                <div class="metric-box">
                    <div class="metric-label">{lang_dict['home_metric_total_apartments']}</div>
                    <div class="metric-value">{total_metrics["total_apartments"]}</div>
                </div>
                """, unsafe_allow_html=True)
            with col2:
                st.markdown(f"""
                <div class="metric-box">
                    <div class="metric-label">{lang_dict['home_metric_active_contracts']}</div>
                    <div class="metric-value">{total_metrics["active_contracts"]}</div>
                </div>
                """, unsafe_allow_html=True)
            with col3:
                st.markdown(f"""
                <div class="metric-box">
                    <div class="metric-label">{lang_dict['home_metric_total_rent']}</div>
                    <div class="metric-value">€{total_metrics['total_monthly_rent']:,.0f}</div>
                </div>
                """, unsafe_allow_html=True)
            with col4:
                st.markdown(f"""
                <div class="metric-box">
                    <div class="metric-label">{lang_dict['home_metric_checkouts']}</div>
                    <div class="metric-value">{total_metrics["upcoming_checkouts"]}</div>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.warning(lang_dict['home_metrics_error'])

    # --- KHRYSTYNA'S PERSONAL METRICS ---
    st.subheader(lang_dict['home_personal_header'])
    khrystyna_metrics = sheet_manager.calculate_key_metrics(realtor_name="Khrystyna Markin")
    if khrystyna_metrics:
        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown(f"""
            <div class="metric-box">
                <div class="metric-label">{lang_dict['home_metric_your_contracts']}</div>
                <div class="metric-value">{khrystyna_metrics["active_contracts"]}</div>
            </div>
            """, unsafe_allow_html=True)
        with col2:
            st.markdown(f"""
            <div class="metric-box">
                <div class="metric-label">{lang_dict['home_metric_your_rent']}</div>
                <div class="metric-value">€{khrystyna_metrics['total_monthly_rent']:,.0f}</div>
            </div>
            """, unsafe_allow_html=True)
        with col3:
            st.markdown(f"""
            <div class="metric-box">
                <div class="metric-label">{lang_dict['home_metric_your_checkouts']}</div>
                <div class="metric-value">{khrystyna_metrics["upcoming_checkouts"]}</div>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.warning(lang_dict['home_personal_metrics_error'])

    # --- ACTION ITEMS FOR KHRYSTYNA ---
    st.markdown("---")
    st.subheader(lang_dict['home_action_items_header'])
    
    action_items = todo_manager.get_action_items_for_realtor("Khrystyna Markin", limit=5)
    
    if not action_items:
        st.success(lang_dict['home_no_action_items'])
    else:
        for item in action_items:
            icon = "🔴" if item['status'] == "Unpaid" else "🚪"
            st.markdown(
                f"{icon} **`{item['ap_code']}`** - {item['reason']}"
            )

elif menu_choice == "✅ To-Do List":
    st.markdown(f"<h2 class='stSubheader'>{lang_dict['todo_header']}</h2>", unsafe_allow_html=True)
    if 'scroll_to_top' not in st.session_state:
        st.session_state.scroll_to_top = False
    if st.session_state.scroll_to_top:
        scroll_to_here(0, key='top')  # Scroll to top of page
        st.session_state.scroll_to_top = False  # Reset flag
    # --- NEW: FUNCTION TO DETECT UNSAVED CHANGES ---
    def has_unsaved_changes():
        """Compares the session state with the backend state to detect changes."""
        for item in todo_manager.todo_list:
            item_key_base = f"{item['ap_code']}_{item.get('due_date')}"
            
            # Check notes
            session_note = st.session_state.get(f"note_{item_key_base}")
            if session_note is not None and session_note != item.get("note", ""):
                return True
            
            # Check checkboxes
            for cb_name in ["TELEGRAM", "EMAIL", "UT_DATA", "WRITE"]:
                session_checked = st.session_state.get(f"{cb_name}_{item_key_base}")
                if session_checked is not None and session_checked != item['checked'][cb_name]:
                    return True
        return False

    # --- STICKY CONTROL STRIP ---
    control_strip = st.container()
    with control_strip:
        st.markdown("""<div class='control-strip-marker'></div>""", unsafe_allow_html=True)
        
        # --- NEW: UNSAVED CHANGES WARNING (FRONTEND LOGIC) ---
        if has_unsaved_changes() or todo_manager.is_dirty:
            st.warning("💾 You have unsaved changes! Click 'Save All Changes' to persist them.", icon="⚠️")
        
        with st.expander("⚙️ Show Controls (Date Range, Save, Filter)", expanded=False):
            # --- Row 1: Date Range and Generate Button ---
            c1, c2, c3, c4 = st.columns(4) # MODIFIED: Changed to 4 columns
            today = datetime.now(pytz.timezone("Europe/Bucharest")).date()
            
            def on_date_change():
                st.session_state.todo_start_date = st.session_state.start_date_key
                st.session_state.todo_end_date = st.session_state.end_date_key

            with c1:
                st.date_input(
                    lang_dict['todo_from_date'], 
                    value=st.session_state.get('todo_start_date', today - timedelta(days=15)),
                    key='start_date_key',
                    on_change=on_date_change
                )
            with c2:
                st.date_input(
                    lang_dict['todo_to_date'], 
                    value=st.session_state.get('todo_end_date', today + timedelta(days=15)),
                    key='end_date_key',
                    on_change=on_date_change
                )
            with c3:
                st.write("") # Spacer for alignment
                if st.button(lang_dict['todo_generate_button'], use_container_width=True):
                    with st.spinner("Generating smart list..."):
                        start_date_to_use = st.session_state.get('todo_start_date', today - timedelta(days=15))
                        end_date_to_use = st.session_state.get('todo_end_date', today + timedelta(days=15))
                        todo_manager.generate_list(
                            start_date=start_date_to_use, 
                            end_date=end_date_to_use
                        )
            with c4:
                st.write("") # Spacer for alignment
                if st.button(lang_dict['todo_refresh_ut_button'], use_container_width=True):
                    with st.spinner("Refreshing UT_DATA..."):
                        success, message = sheet_manager.reload_specific_sheet("UT_DATA")
                        if success:
                            st.toast(message, icon="✅")
                        else:
                            st.toast(message, icon="❌")

                    st.success("To-Do list generated!")
                    st.rerun()

            st.markdown("<hr style='margin: 0.5rem 0;'>", unsafe_allow_html=True)
            c5, c6 = st.columns(2)
            with c5:
                if st.button("🔄 Reload List from Drive", use_container_width=True):
                    with st.spinner("Reloading todo list from Google Drive..."):
                        success, message = todo_manager.reload_from_drive()
                        if success:
                            st.toast(message, icon="✅")
                        else:
                            st.toast(message, icon="❌")
                    st.rerun()

            st.markdown("<hr style='margin: 0.5rem 0;'>", unsafe_allow_html=True)
            c1, c2 = st.columns(2)
            with c1:
                if st.button(lang_dict['todo_save_button'], use_container_width=True):
                    with st.spinner("Saving changes..."):
                        for item in todo_manager.todo_list:
                            item_key_base = f"{item['ap_code']}_{item.get('due_date')}"
                            note_text = st.session_state.get(f"note_{item_key_base}", item.get("note", ""))
                            todo_manager.update_note(item['ap_code'], note_text, item.get('due_date'))
                            for cb_name in ["TELEGRAM", "EMAIL", "UT_DATA", "WRITE"]:
                                is_checked = st.session_state.get(f"{cb_name}_{item_key_base}", item['checked'][cb_name])
                                todo_manager.update_checkbox(item['ap_code'], cb_name, is_checked, item.get('due_date'))
                        todo_manager._sort_list()
                        todo_manager.save_list_to_drive()
                    st.success("Changes saved!")
                    st.rerun()
            with c2:
                if st.session_state.get('selected_agent', 'All') != "All":
                    items = [item for item in todo_manager.todo_list if item.get('realtor') == st.session_state.selected_agent]
                else:
                    items = todo_manager.todo_list
                visible_codes = sorted(list(set([item['ap_code'] for item in items])))
                st.selectbox(
                    lang_dict['todo_filter_ap'], options=["---"] + visible_codes,
                    index=None, key="todo_search_query", placeholder=lang_dict['todo_search_placeholder']
                )

    # --- (The rest of the page layout and logic remains the same) ---
    c1, c2 = st.columns([1, 2])
    with c1:
        st.subheader(lang_dict['todo_filter_agent_header'])
        agents = ["All", "Khrystyna Markin", "Kristina Fedina"]
        
        def update_selected_agent():
            st.session_state.selected_agent = st.session_state.agent_radio_key

        st.radio(
            "Select an agent:", agents,
            index=agents.index(st.session_state.selected_agent),
            horizontal=True, 
            label_visibility="collapsed", 
            key="agent_radio_key",
            on_change=update_selected_agent
        )
        with c2:
            st.subheader("📊 Quick Stats")
            items_for_stats = todo_manager.todo_list
            if st.session_state.selected_agent != "All":
                items_for_stats = [item for item in items_for_stats if item.get('realtor') == st.session_state.selected_agent]
            
            
            unpaid_count = len([item for item in items_for_stats if item.get('status') == "Unpaid"])
            checkout_count = len([item for item in items_for_stats if item.get('status') == "Check-out"])
            
            oldest_unpaid_task = None
            unpaid_tasks = [item for item in items_for_stats if item.get('status') == "Unpaid" and item.get('due_date')]
            if unpaid_tasks:
                oldest_unpaid_task = min(unpaid_tasks, key=lambda x: x['due_date'])

            stat_cols = st.columns(3)
            with stat_cols[0]:
                st.markdown(f"""<div class="stat-box"><div class="stat-label">🔴 Unpaid Tasks</div><div class="stat-value">{unpaid_count}</div></div>""", unsafe_allow_html=True)
            with stat_cols[1]:
                st.markdown(f"""<div class="stat-box"><div class="stat-label">🚪 Upcoming Check-outs</div><div class="stat-value">{checkout_count}</div></div>""", unsafe_allow_html=True)
            with stat_cols[2]:
                if oldest_unpaid_task:
                    oldest_date = datetime.fromisoformat(oldest_unpaid_task['due_date']).strftime('%d-%b')
                    st.markdown(f"""<div class="stat-box"><div class="stat-label">⏳ Oldest Unpaid</div><div class="stat-sub-value">{oldest_unpaid_task['ap_code']}<br>({oldest_date})</div></div>""", unsafe_allow_html=True)
                else:
                    st.markdown(f"""<div class="stat-box"><div class="stat-label">⏳ Oldest Unpaid</div><div class="stat-sub-value">N/A</div></div>""", unsafe_allow_html=True)

    with st.expander(lang_dict['todo_manual_add_expander']):
        c1, c2, c3 = st.columns([2, 1, 1])
        with c1:
            manual_ap_code = st.selectbox(
                lang_dict['todo_manual_add_label'], 
                options=ALL_AP_CODES,
                index=None,
                placeholder="Select an AP Code",
                key="manual_add_code"
            )
        with c2:
            manual_due_date = st.date_input("Optional Due Date", value=None)
        
        with c3:
            st.write("") # Spacer for alignment
            if st.button(lang_dict['todo_manual_add_button'], use_container_width=True):
                if manual_ap_code:
                    # Pass both the code and the date to the backend function
                    success, message = todo_manager.add_manual_item(manual_ap_code, manual_due_date)
                    if success: 
                        st.success(message)
                        st.rerun()
                    else: 
                        st.warning(message)
                else:
                    st.warning("Please select an Apartment Code.")
    
    st.markdown("---")

    # --- Display Logic ---
    # (The rest of the display logic is unchanged and correct)
    todo_list = todo_manager.todo_list
    if not todo_list:
        st.info(lang_dict['todo_empty_list'])
    else:
        items_to_display = todo_list
        if st.session_state.selected_agent != "All":
            items_to_display = [item for item in items_to_display if item.get('realtor') == st.session_state.selected_agent]
        
        search_query = st.session_state.get("todo_search_query")
        if search_query and search_query != "---":
            items_to_display = [item for item in items_to_display if item['ap_code'] == search_query]
        
        if not items_to_display:
            st.info(lang_dict['todo_no_tasks_for_agent'].format(agent=st.session_state.selected_agent))
        else:
            # --- NEW: SEPARATE LISTS FOR 5 TABS ---
            all_completed_items = [item for item in items_to_display if all(item['checked'].values())]
            
            unpaid_items = [item for item in items_to_display if item.get('status') == "Unpaid" and not all(item['checked'].values())]
            checkout_items = [item for item in items_to_display if item.get('status') == "Check-out" and not all(item['checked'].values())]
            paid_items = [item for item in items_to_display if item.get('status') == "Paid" and not all(item['checked'].values())]
            manual_items = [item for item in items_to_display if item.get('manual') and not all(item['checked'].values())]

            def render_todo_item(item, is_trash=False):
                
                item_key_base = f"{item['ap_code']}_{item.get('due_date')}"
                with st.container(border=True):
                    # ... (rest of the render_todo_item function is the same)
                    col1, col2 = st.columns([3, 1])
                    with col1:
                        st.markdown(f"**`{item['ap_code']}`** - {item['reason']}")
                    with col2:
                        if is_trash:
                            if st.button("🔄 Restore", key=f"restore_{item_key_base}", use_container_width=True):
                                todo_manager.restore_item(item['ap_code'], item.get('due_date'))
                                st.rerun()
                        else:
                            if st.button(lang_dict['todo_item_remove'], key=f"remove_{item_key_base}", use_container_width=True):
                                todo_manager.remove_item(item['ap_code'], item.get('due_date'))
                                st.rerun()
                    date_cols = st.columns(2)
                    last_trans = item.get('last_transaction_date')
                    if last_trans:
                        date_str = datetime.fromisoformat(last_trans).strftime('%d-%b-%Y')
                        trans_type = item.get('last_transaction_type', '')
                        date_cols[0].caption(f"{lang_dict['todo_item_last_transaction']}: {date_str} ({trans_type})")
                    else:
                        date_cols[0].caption(f"{lang_dict['todo_item_last_transaction']}: N/A")
                    last_ut = item.get('last_ut_reading_date')
                    date_cols[1].caption(f"{lang_dict['todo_item_last_ut']}: {datetime.fromisoformat(last_ut).strftime('%d-%b-%Y') if last_ut else 'N/A'}")
                    checkbox_cols = st.columns(4)
                    checkboxes = ["TELEGRAM", "EMAIL", "UT_DATA", "WRITE"]
                    for i, cb_name in enumerate(checkboxes):
                        checkbox_cols[i].checkbox(cb_name, value=item['checked'][cb_name], key=f"{cb_name}_{item_key_base}")

                    # --- NOTIFICATION LOGIC: Check for utilities reminder email ---
                    if item.get('due_date'):
                        try:
                            due_date = datetime.fromisoformat(item['due_date'])
                            # Make due_date timezone-aware (assume Bucharest timezone)
                            if due_date.tzinfo is None:
                                bucharest_tz = pytz.timezone("Europe/Bucharest")
                                due_date = bucharest_tz.localize(due_date)
                            today = datetime.now(pytz.utc)
                            # Only check for warning if due date is within the next 5 days
                            if due_date <= today + timedelta(days=5):
                                reminder_date = due_date - timedelta(days=5)
                                reminder_date_str = reminder_date.strftime('%d/%m/%Y')
                                print(f"UTILITIES CHECK: Checking for utilities email for {item['ap_code']} | Due: {due_date.strftime('%d-%m-%Y')} | Reminder: {reminder_date_str}")
                                
                                # Check EMAIL_LOG for matching emails
                                email_log_data = sheet_manager.all_data.get('EMAIL_LOG', {}).get('data', [])
                                email_found = False
                                utilities_subjects = [
                                    "Напоминание о показаниях",
                                    "Показания счётчиков для оплаты",
                                    "Показания счётчиков для [client name]",
                                    "Показания счётчиков"
                                ]

                                # Find column indices using the same logic as format_email_log
                                try:
                                    header = sheet_manager.all_data.get('EMAIL_LOG', {}).get('header', [])
                                    header_upper = [str(h).strip().upper() for h in header]
                                    def find_col_by_keyword(keyword):
                                        for i, h in enumerate(header_upper):
                                            if keyword in h: return i
                                        raise ValueError(f"'{keyword}'")

                                    date_col = 0  # DATE is always first column
                                    code_col = find_col_by_keyword("AP CODE")
                                    subject_col = find_col_by_keyword("SUBJECT")
                                    summary_col = find_col_by_keyword("SUMMARY")
                                except (ValueError, IndexError):
                                    print(f"UTILITIES CHECK: Error finding EMAIL_LOG columns for {item['ap_code']}")
                                    date_col, code_col, subject_col, summary_col = 0, 1, 2, 3  # Fallback to default positions

                                for row in email_log_data:
                                    if len(row) > max(date_col, code_col, subject_col):  # Ensure row has enough columns
                                        email_date_str = row[date_col]  # DATE column
                                        try:
                                            # Try multiple date formats with more flexibility
                                            email_date = None
                                            for fmt in ['%d/%m/%Y', '%d-%m-%Y', '%d.%m.%Y']:
                                                try:
                                                    email_date = datetime.strptime(email_date_str, fmt)
                                                    break
                                                except ValueError:
                                                    continue
                                            if email_date:
                                                # Make email_date timezone-aware to match reminder_date
                                                if email_date.tzinfo is None:
                                                    email_date = bucharest_tz.localize(email_date)

                                                # Check if email was sent within 7 days BEFORE the due date
                                                days_before_due = (due_date - email_date).days
                                                if 0 <= days_before_due <= 7:  # Allow up to 7 days before due date
                                                    ap_codes_in_email = [c.strip().upper() for c in row[code_col].split(',')] if len(row) > code_col else []  # AP CODE
                                                    if item['ap_code'].upper() in ap_codes_in_email:
                                                        subject = row[subject_col] if len(row) > subject_col else ""  # SUBJECT column
                                                        summary = row[summary_col] if len(row) > summary_col else ""  # SUMMARY column

                                                        # Check both subject and summary for utilities keywords
                                                        text_to_check = (subject + " " + summary).lower()
                                                        if any(utility_subject.lower() in text_to_check for utility_subject in utilities_subjects):
                                                            email_found = True
                                                            print(f"UTILITIES CHECK: ✅ Found matching email for {item['ap_code']} with subject: '{subject}' on {email_date_str} ({days_before_due} days before due)")
                                                            break
                                        except ValueError:
                                            continue

                                if not email_found:
                                    print(f"UTILITIES CHECK: ❌ No utilities email found for {item['ap_code']} - showing warning")
                                    st.warning("⚠️ Client did not receive utilities reminder email 5 days before due date.")

                        except Exception as e:
                            print(f"DEBUG: Error checking email for {item['ap_code']}: {e}")

                    st.text_input(lang_dict['todo_item_notes'], value=item.get("note", ""), key=f"note_{item_key_base}")
                    with st.expander(lang_dict['todo_show_email']):
                        with st.spinner(lang_dict['todo_fetching_email']):
                            log = cached_email_log(item['ap_code'])
                            if log:
                                display_email_log(log)
                            else:
                                st.info(lang_dict['todo_no_email_log'].format(ap_code=item['ap_code']))
                    # Use a button to trigger master report generation instead of expander callback
                    # Include due_date in key to make it unique for each item
                    button_key = f"master_report_btn_{item['ap_code']}_{item.get('due_date', 'no_date')}"
                    hide_key = f"hide_master_report_{item['ap_code']}_{item.get('due_date', 'no_date')}"
                    
                    # Check if report is currently being displayed
                    report_displayed = st.session_state.get(f"report_displayed_{item['ap_code']}_{item.get('due_date', 'no_date')}", False)
                    
                    if report_displayed:
                        # Show hide button and the report in horizontal layout
                        col1, col2 = st.columns([3, 1])
                        with col2:
                            if st.button("🚫 Hide Report", key=hide_key):
                                st.session_state[f"report_displayed_{item['ap_code']}_{item.get('due_date', 'no_date')}"] = False
                                st.rerun()
                        with col1:
                            # Display the report
                            with st.spinner("Generating master report..."):
                                report = cached_master_report(item['ap_code'])
                                if report:
                                    display_master_report(report)
                                else:
                                    st.error(f"Could not generate master report for {item['ap_code']}.")
                    else:
                        # Show the show button
                        if st.button("📜 Show Master Report", key=button_key):
                            st.session_state[f"report_displayed_{item['ap_code']}_{item.get('due_date', 'no_date')}"] = True
                            st.rerun()
                    
                    if st.button(lang_dict['todo_item_mark_checked'], key=f"check_{item_key_base}"):
                        todo_manager.update_check_time(item['ap_code'], item.get('due_date'))
                        todo_manager.save_list_to_drive()
                        st.rerun()
                    check_time_str = item.get('check_time')
                    check_time_display = "Never"
                    if check_time_str:
                        try:
                            check_time_dt = datetime.fromisoformat(check_time_str).astimezone(pytz.timezone("Europe/Bucharest"))
                            check_time_display = check_time_dt.strftime('%d-%b %H:%M')
                        except:
                            check_time_display = "Invalid Date"
                    st.caption(f"{lang_dict['todo_item_last_checked']}: {check_time_display}")

            # --- NEW: 6-TAB LAYOUT ---
            tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
                lang_dict['todo_tab_unpaid'].format(count=len(unpaid_items)),
                lang_dict['todo_tab_checkouts'].format(count=len(checkout_items)),
                lang_dict['todo_tab_paid'].format(count=len(paid_items)),
                lang_dict['todo_tab_manual'].format(count=len(manual_items)),
                lang_dict['todo_completed_expander'].format(count=len(all_completed_items)), # New Completed Tab
                f"🗑️ Trash Bin ({len(todo_manager.trash_bin)})" # New Trash Bin Tab
            ])

            with tab1:
                st.subheader(lang_dict['todo_action_required'])
                if not unpaid_items: 
                    st.info(lang_dict['todo_unpaid_empty'])
                for item in unpaid_items:
                    render_todo_item(item)
            
            with tab2:
                if not checkout_items: st.info(lang_dict['todo_checkouts_empty'])
                for item in checkout_items:
                    render_todo_item(item)

            with tab3:
                if not paid_items: st.info(lang_dict['todo_paid_empty'])
                for item in paid_items:
                    render_todo_item(item)
            
            with tab4:
                if not manual_items: st.info(lang_dict['todo_manual_empty'])
                for item in manual_items:
                    render_todo_item(item)
            
            with tab5: # NEW COMPLETED TAB
                if not all_completed_items:
                    st.info(lang_dict['todo_completed_empty'])
                for item in all_completed_items:
                    render_todo_item(item)

            with tab6:  # NEW TRASH BIN TAB
                st.subheader("Trash Bin")
                if not todo_manager.trash_bin:
                    st.info("Trash bin is empty.")
                else:
                    for item in todo_manager.trash_bin:
                        render_todo_item(item, is_trash=True)

            if st.button("⬆️ Back to Top", use_container_width=True):
                st.session_state.scroll_to_top = True
                st.rerun()
            # ----------------------------------------------------------------------

            # st.markdown(
            #     """
            #     <style>
            #     .back-to-top-container::after {
            #         display: block !important; /* FORCE SHOW FOR TESTING */
            #         /* ... rest of styles ... */
            #     }
            #     </style>
            #     <div class="back-to-top-container" id="back-to-top-container">TEST</div>
            #     """,
            #     unsafe_allow_html=True
            # )

# --- Apartment & Realtor Info Page ---
elif menu_choice == lang_dict['menu_apartment_info']:
    st.markdown(f"<h2 class='stSubheader'>{lang_dict['apartment_header']}</h2>", unsafe_allow_html=True)

    col1, col2 = st.columns([1, 2])
    with col1:
        ap_code_input = st.selectbox(
            lang_dict['apartment_input_label'],
            options=ALL_AP_CODES,
            index=None, # Makes the default selection blank
            placeholder=lang_dict['apartment_input_placeholder'],
            key="ap_code_input_unique"
        )
        if st.button(lang_dict['apartment_button_get_details'], key="get_details_btn", use_container_width=True):
            st.session_state.selected_ap_code = ap_code_input
            st.session_state.action_result = None

    if st.session_state.selected_ap_code:
        apartment_data = cached_get_apartment_data(st.session_state.selected_ap_code)
        if apartment_data:
            st.markdown("---")
            with st.container():
                st.markdown(f"<h3 class='stSubheader'>{lang_dict['apartment_actions_subheader'].format(ap_code=st.session_state.selected_ap_code)}</h3>", unsafe_allow_html=True)
                action_cols = st.columns(4)
                if action_cols[0].button(lang_dict['apartment_button_calculate_utilities']):
                    _, summary = cached_calculate_utilities(st.session_state.selected_ap_code, True)
                    st.session_state.action_result = ("utility", summary)
                if action_cols[1].button(lang_dict['apartment_button_master_report']):
                    report = cached_master_report(st.session_state.selected_ap_code)
                    st.session_state.action_result = ("master_report", report)
                if action_cols[2].button(lang_dict['apartment_button_email_log']):
                    log = cached_email_log(st.session_state.selected_ap_code)
                    st.session_state.action_result = ("email_log", log)
            if st.session_state.action_result:
                action_type, result_data = st.session_state.action_result
                with st.expander(lang_dict['apartment_more_details'], expanded=True):
                    if action_type == "utility": display_utility_summary(result_data)
                    elif action_type == "master_report": display_master_report(result_data)
                    elif action_type == "email_log": display_email_log(result_data)
            st.markdown("---")
            with st.container():
                st.markdown(f"<h3 class='stSubheader'>{lang_dict['apartment_details_subheader']}</h3>", unsafe_allow_html=True)
                col1, col2 = st.columns(2)
                with col1:
                    with st.expander(lang_dict['apartment_client_location'], expanded=True):
                        first_name = apartment_data.get('FIRST NAME_CL', '')
                        family_name = apartment_data.get('FAMILY NAME_CL', '')
                        full_name = f"{first_name} {family_name}".strip()
                        if full_name:
                            st.markdown(f"**Client Name:** {full_name}")
                        address_parts = [
                            apartment_data.get('ADDRESS', ''),
                            f"Nr. {apartment_data.get('NR', '')}" if apartment_data.get('NR') else '',
                            f"Bl. {apartment_data.get('BL', '')}" if apartment_data.get('BL') else '',
                            f"Sc. {apartment_data.get('SC', '')}" if apartment_data.get('SC') else '',
                            f"Et. {apartment_data.get('ET', '')}" if apartment_data.get('ET') else ''
                        ]
                        full_address = ", ".join(part for part in address_parts if part)
                        st.markdown(f"**Address:** {full_address}")
                        st.markdown(f"**Realtor:** {apartment_data.get('REALTOR', 'N/A')}")
                        st.markdown(f"**Client Email:** {apartment_data.get('E-MAIL', 'N/A')}")
                        st.markdown(f"**Client Phone:** {apartment_data.get('PHONE_CL', 'N/A')}")
                        if apartment_data.get('TELEGRAM'):
                            st.markdown(f"**Telegram:** {apartment_data.get('TELEGRAM')}")
                with col2:
                    with st.expander(lang_dict['apartment_contract_rent'], expanded=True):
                        st.markdown(f"**Contract Start:** {apartment_data.get('START', 'N/A')}")
                        st.markdown(f"**Contract End:** {apartment_data.get('END', 'N/A')}")
                        st.markdown(f"**Rent:** `{apartment_data.get('ARENDA CLIENT', 'N/A')} EUR`")
                        st.markdown(f"**Deposit:** `{apartment_data.get('DEPOSIT', 'N/A')}`")
                             
                with st.expander(lang_dict['apartment_more_details']):
                    # --- NEW: Get and display the contract folder link ---
                    st.markdown("##### 📜 Contract Folder")
                    contract_info = sheet_manager.get_latest_contract_info(st.session_state.selected_ap_code)
                    if contract_info and contract_info.get("link") != "N/A":
                        st.markdown(f"**Folder:** [{contract_info['name']}]({contract_info['link']})")
                    else:
                        st.info("No contract folder found in Google Drive.")
                    st.markdown("---")
                    # --- END OF NEW SECTION ---

                    col3, col4, col5 = st.columns(3)
                    with col3:
                        st.markdown("##### Financials")
                        st.markdown(f"**Rent Suggestion:** {apartment_data.get('RENT SUGGESTION', 'N/A')}")
                        st.markdown(f"**Last Pay:** {apartment_data.get('LAST PAY', 'N/A')}")
                    with col4:
                        st.markdown("##### Utility Pricing")
                        st.markdown(f"**Electricity Price:** {apartment_data.get('ELECTRICITY PRICE', 'N/A')}")
                        st.markdown(f"**Gas Price:** {apartment_data.get('GAS PRICE', 'N/A')}")
                    with col5:
                        st.markdown("##### Status & Notes")
                        st.markdown(f"**Contract Status:** {apartment_data.get('CONTRACT STATUS_CL', 'N/A')}")
                        st.markdown(f"**Updated Date:** {apartment_data.get('UPDATED DATE', 'N/A')}")
                        st.markdown(f"**Notes:** {apartment_data.get('NOTES', 'N/A')}")
        else:
            st.error(lang_dict['apartment_not_found'].format(ap_code=st.session_state.selected_ap_code))
            st.session_state.selected_ap_code = None
    st.markdown("---")
    st.markdown(f"<h3 class='stSubheader'>List All Apartments for a Realtor</h3>", unsafe_allow_html=True)
    
    realtors = ["Khrystyna Markin", "Kristina Fedina"]
    selected_realtor = st.selectbox(
        "Select a Realtor:",
        options=realtors,
        index=None,
        placeholder="Choose a realtor to see their apartments"
    )

    if selected_realtor:
        with st.spinner(f"Fetching apartments for {selected_realtor}..."):
            ap_codes = sheet_manager.get_apartments_by_realtor(selected_realtor)
            if ap_codes:
                st.success(f"Found {len(ap_codes)} apartments for {selected_realtor}:")
                # Display in multiple columns for better readability
                num_columns = 4
                cols = st.columns(num_columns)
                for i, code in enumerate(sorted(ap_codes)):
                    cols[i % num_columns].markdown(f"- `{code}`")
            else:
                st.warning(f"No apartments found for {selected_realtor}.")
    st.markdown("---")


# --- Utilities Page ---
elif menu_choice == lang_dict['menu_utilities']:
    with st.container():
        st.markdown(f"<h2 class='stSubheader'>{lang_dict['utilities_header']}</h2>", unsafe_allow_html=True)
        
        # The widgets are now directly linked to session_state via their key
        st.selectbox(
            lang_dict['utilities_input_label'],
            options=ALL_AP_CODES,
            index=None,
            placeholder=lang_dict['utilities_input_placeholder'],
            key="util_ap_code" 
        )
        
        col1, col2 = st.columns(2)
        with col1:
            st.checkbox(lang_dict['utilities_checkbox_include_rent'], key="util_include_rent")
        
        button_clicked = col2.button(lang_dict['utilities_button_calculate'])

        if button_clicked:
            # When the button is clicked, we read the values from session_state
            if st.session_state.util_ap_code:
                diag, summary = cached_calculate_utilities(st.session_state.util_ap_code, st.session_state.util_include_rent)
                # We save the result to session_state as well
                st.session_state.util_last_result = (diag, summary)
            else:
                st.warning("Please enter an Apartment Code.")
                st.session_state.util_last_result = None # Clear old results

        # Now, we check if there's a saved result and display it on every rerun
        if st.session_state.util_last_result:
            diag, summary = st.session_state.util_last_result
            if summary:
                st.markdown("---")
                display_utility_summary(summary)
                with st.expander(lang_dict['apartment_more_details']):
                    display_master_report(diag)
                    st.code(summary + "\n\n" + diag)
            else: 
                st.error(diag)

# --- Generate Reports Page ---
elif menu_choice == lang_dict['menu_reports']:
    with st.container():
        st.markdown(f"<h2 class='stSubheader'>{lang_dict['reports_header']}</h2>", unsafe_allow_html=True)
        tab1, tab2 = st.tabs([lang_dict['reports_tab_master'], lang_dict['reports_tab_batch']])
        with tab1:
            with st.container():
                master_ap_code = st.selectbox(
                    lang_dict['reports_master_input_label'],
                    options=ALL_AP_CODES,
                    index=None,
                    placeholder=lang_dict['reports_master_input_placeholder'],
                    key="master_code"
                )
                if st.button(lang_dict['reports_master_button'], key="master_btn"):
                    if master_ap_code:
                        report = cached_master_report(master_ap_code)
                        display_master_report(report)
                        with st.expander(lang_dict['apartment_more_details']):
                            st.code(report)
                        st.download_button("📥 Download Report", report, f"Master_Report_{master_ap_code}.txt")
        with tab2:
            with st.container():
                codes_input = st.text_area(lang_dict['reports_batch_input_label'], placeholder=lang_dict['reports_batch_input_placeholder'], height=250)
                if st.button(lang_dict['reports_batch_button'], key="batch_btn"):
                    if codes_input:
                        codes_to_process = [code.strip() for code in codes_input.split('\n') if code.strip()]
                        if codes_to_process:
                            report = cached_batch_report(codes_to_process)
                            with st.expander(lang_dict['apartment_more_details'], expanded=True):
                                st.code(report)
                            st.download_button("📥 Download Report", report, f"Batch_Report_{datetime.now(pytz.utc).strftime('%Y-%m-%d_%H-%M')}.txt")

# --- Client Communication Page ---
elif menu_choice == lang_dict['menu_communication']:
    with st.container():
        st.markdown(f"<h2 class='stSubheader'>{lang_dict['communication_header']}</h2>", unsafe_allow_html=True)
        tab1, tab2 = st.tabs([lang_dict['communication_tab_find_ap'], lang_dict['communication_tab_find_email']])
        with tab1:
            with st.container():
                st.markdown(f"<h3 class='stSubheader'>{lang_dict['communication_find_ap_subheader']}</h3>", unsafe_allow_html=True)
                email = st.text_input(lang_dict['communication_find_ap_input_label'], placeholder=lang_dict['communication_find_ap_input_placeholder'], key="find_by_email_input")
                if st.button(lang_dict['communication_find_ap_button']):
                    if email:
                        result = cached_find_ap_by_email(email)
                        if "Success" in result: st.success("✅ Success!")
                        elif "Mismatch" in result: st.error("❌ Data Mismatch!")
                        else: st.info("ℹ️ Result:")
                        display_client_comm_result(result)
        with tab2:
            with st.container():
                st.markdown(f"<h3 class='stSubheader'>{lang_dict['communication_find_email_subheader']}</h3>", unsafe_allow_html=True)
                ap_code = st.selectbox(
                    lang_dict['communication_find_email_input_label'],
                    options=ALL_AP_CODES,
                    index=None,
                    placeholder=lang_dict['communication_find_email_input_placeholder'],
                    key="find_by_ap_code_input"
                )
                if st.button(lang_dict['communication_find_email_button']):
                    if ap_code:
                        result = cached_find_email_by_ap_code(ap_code)
                        if "Success" in result: st.success("✅ Success!")
                        else: st.info("ℹ️ Result:")
                        display_client_comm_result(result)

# --- System & Settings Page ---
elif menu_choice == lang_dict['menu_settings']:
    with st.container():
        st.markdown(f"<h2 class='stSubheader'>{lang_dict['settings_header']}</h2>", unsafe_allow_html=True)
        with st.container():
            st.markdown(f"<h3 class='stSubheader'>{lang_dict['settings_data_status']}</h3>", unsafe_allow_html=True)
            col1, col2 = st.columns(2)
            sheet_ts = sheet_manager.sheet_data_timestamp
            folder_ts = sheet_manager.folder_cache_timestamp
            with col1: st.metric(lang_dict['settings_sheets_loaded'], sheet_ts.strftime('%H:%M:%S') if sheet_ts else "Never")
            with col2: st.metric(lang_dict['settings_folders_cached'], folder_ts.strftime('%H:%M:%S') if folder_ts else "Never")
        st.markdown("---")
        with st.container():
            st.markdown(f"<h3 class='stSubheader'>{lang_dict['settings_current_settings']}</h3>", unsafe_allow_html=True)
            settings = sheet_manager.settings
            setting_cols = st.columns(len(settings))
            for i, (key, value) in enumerate(settings.items()):
                with setting_cols[i]:
                    st.metric(label=key, value=str(value))
        st.markdown("---")
        with st.container():
            st.markdown(f"<h3 class='stSubheader'>{lang_dict['settings_update_settings']}</h3>", unsafe_allow_html=True)
            update_col1, update_col2 = st.columns(2)
            with update_col1:
                key_to_change = st.selectbox(lang_dict['settings_select_setting'], options=list(settings.keys()))
            with update_col2:
                new_value = st.number_input(lang_dict['settings_enter_value'].format(key=key_to_change), value=float(settings[key_to_change]), format="%.4f")
            if st.button(lang_dict['settings_save_button']):
                sheet_manager.settings[key_to_change] = new_value
                sheet_manager.save_settings()
                st.success(f"✅ Setting '{key_to_change}' updated to {new_value}.")
                st.rerun()
        st.markdown("---")
        with st.container():
            st.markdown(f"<h3 class='stSubheader'>{lang_dict['settings_system_actions']}</h3>", unsafe_allow_html=True)
            action_col1, action_col2 = st.columns(2)
            if action_col1.button(lang_dict['settings_sync_rate']):
                with st.spinner("🔄 Syncing exchange rate..."):
                    success, message = sheet_manager.sync_exchange_rate()
                    if success: st.success(f"✅ {message}")
                    else: st.error(f"❌ {message}")
                st.rerun()
            if action_col2.button(lang_dict['settings_refresh_data']):
                with st.spinner("🔄 Reloading all data..."):
                    st.cache_resource.clear()
                    st.cache_data.clear()
                    st.success("✅ Data cache cleared.")
                    st.rerun()