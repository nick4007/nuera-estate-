import streamlit as st
import requests
import pandas as pd
import math
import plotly.express as px
import plotly.graph_objects as go
from typing import Dict, Any, List, Optional
import concurrent.futures
import json
import time

# ----------------------
# Page Configuration
# ----------------------
st.set_page_config(
    page_title="NeuraEstate - AI-Powered Real Estate Platform",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ----------------------
# Global CSS Styling
# ----------------------
st.markdown("""
<style>
    /* Import Poppins font */
    @import url('https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;500;600;700&display=swap');
    
    /* Global styles */
    .main {
        padding-top: 2rem;
    }
    
    .stApp {
        background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
    }
    
    /* Brand colors */
    :root {
        --primary: #80A1BA;
        --secondary: #91C4C3;
        --accent: #B4DEBD;
        --light: #FFF7DD;
        --dark: #2c3e50;
        --text: #34495e;
    }
    
    /* Typography */
    h1, h2, h3, h4, h5, h6 {
        font-family: 'Poppins', sans-serif !important;
        color: var(--dark) !important;
    }
    
    .stMarkdown {
        font-family: 'Poppins', sans-serif !important;
    }
    
    /* Home page styles */
    .ne-home-container {
        max-width: 1200px;
        margin: 0 auto;
        padding: 2rem;
    }
    
    .ne-hero {
        text-align: center;
        margin-bottom: 4rem;
        padding: 3rem 0;
        background: linear-gradient(135deg, var(--primary) 0%, var(--secondary) 100%);
        border-radius: 20px;
        color: white;
    }
    
    .ne-hero h1 {
        font-size: 3.5rem;
        font-weight: 700;
        margin-bottom: 1rem;
        color: white !important;
    }
    
    .ne-hero p {
        font-size: 1.3rem;
        margin-bottom: 2rem;
        opacity: 0.9;
    }
    
    .ne-login-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
        gap: 2rem;
        margin-top: 3rem;
    }
    
    .ne-login-card {
        background: white;
        border-radius: 15px;
        padding: 2.5rem;
        text-align: center;
        box-shadow: 0 10px 30px rgba(0,0,0,0.1);
        transition: transform 0.3s ease, box-shadow 0.3s ease;
        border: 2px solid transparent;
    }
    
    .ne-login-card:hover {
        transform: translateY(-5px);
        box-shadow: 0 20px 40px rgba(0,0,0,0.15);
        border-color: var(--primary);
    }
    
    .ne-login-icon {
        font-size: 4rem;
        margin-bottom: 1rem;
    }
    
    .ne-login-title {
        font-size: 1.5rem;
        font-weight: 600;
        margin-bottom: 1rem;
        color: var(--dark) !important;
    }
    
    .ne-login-desc {
        color: var(--text);
        margin-bottom: 2rem;
        line-height: 1.6;
    }
    
    .ne-login-btn {
        background: linear-gradient(135deg, var(--primary) 0%, var(--secondary) 100%);
        color: white;
        border: none;
        padding: 1rem 2rem;
        border-radius: 50px;
        font-size: 1.1rem;
        font-weight: 600;
        cursor: pointer;
        transition: all 0.3s ease;
        width: 100%;
        text-decoration: none;
        display: inline-block;
    }
    
    .ne-login-btn:hover {
        transform: translateY(-2px);
        box-shadow: 0 10px 20px rgba(128, 161, 186, 0.3);
        color: white;
        text-decoration: none;
    }
    
    /* Search page styles */
    .ne-search-card {
        background: white;
        border-radius: 15px;
        padding: 1.5rem;
        margin-bottom: 1.5rem;
        box-shadow: 0 5px 15px rgba(0,0,0,0.08);
        border-left: 4px solid var(--primary);
    }
    
    .ne-search-title {
        font-size: 1.3rem;
        font-weight: 600;
        color: var(--dark);
        margin-bottom: 0.5rem;
    }
    
    .ne-search-subtitle {
        color: var(--text);
        font-size: 0.9rem;
    }
    
    .ne-filters {
        background: white;
        border-radius: 15px;
        padding: 1.5rem;
        box-shadow: 0 5px 15px rgba(0,0,0,0.08);
    }
    
    .ne-performance {
        background: var(--light);
        border-radius: 10px;
        padding: 1rem;
        margin-top: 1rem;
        border: 1px solid var(--accent);
    }
    
    .ne-performance-title {
        font-weight: 600;
        color: var(--dark);
        margin-bottom: 0.5rem;
    }
    
    /* Property cards */
    .ne-property-grid {
        display: grid;
        grid-template-columns: repeat(auto-fill, minmax(350px, 1fr));
        gap: 1.5rem;
        margin-top: 2rem;
    }
    
    .ne-property-card {
        background: white;
        border-radius: 15px;
        overflow: hidden;
        box-shadow: 0 5px 15px rgba(0,0,0,0.08);
        transition: transform 0.3s ease, box-shadow 0.3s ease;
        border: 1px solid #e0e6ed;
    }
    
    .ne-property-card:hover {
        transform: translateY(-5px);
        box-shadow: 0 15px 30px rgba(0,0,0,0.15);
    }
    
    .ne-property-image {
        height: 200px;
        background: linear-gradient(135deg, var(--primary) 0%, var(--secondary) 100%);
        display: flex;
        align-items: center;
        justify-content: center;
        color: white;
        font-size: 3rem;
    }
    
    .ne-property-content {
        padding: 1.5rem;
    }
    
    .ne-property-title {
        font-size: 1.2rem;
        font-weight: 600;
        color: var(--dark);
        margin-bottom: 0.5rem;
        line-height: 1.4;
    }
    
    .ne-property-price {
        font-size: 1.5rem;
        font-weight: 700;
        color: var(--primary);
        margin-bottom: 1rem;
    }
    
    .ne-property-details {
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 0.5rem;
        margin-bottom: 1rem;
        font-size: 0.9rem;
        color: var(--text);
    }
    
    .ne-property-detail {
        display: flex;
        align-items: center;
        gap: 0.5rem;
    }
    
    .ne-property-detail-icon {
        font-size: 1rem;
    }
    
    .ne-valuation-badge {
        display: inline-block;
        padding: 0.3rem 0.8rem;
        border-radius: 20px;
        font-size: 0.8rem;
        font-weight: 600;
        margin-bottom: 1rem;
    }
    
    .ne-valuation-underpriced {
        background: #d4edda;
        color: #155724;
        border: 1px solid #c3e6cb;
    }
    
    .ne-valuation-overpriced {
        background: #f8d7da;
        color: #721c24;
        border: 1px solid #f5c6cb;
    }
    
    .ne-valuation-fair {
        background: #d1ecf1;
        color: #0c5460;
        border: 1px solid #bee5eb;
    }
    
    .ne-view-details-btn {
        background: linear-gradient(135deg, var(--primary) 0%, var(--secondary) 100%);
        color: white;
        border: none;
        padding: 0.8rem 1.5rem;
        border-radius: 25px;
        font-size: 0.9rem;
        font-weight: 600;
        cursor: pointer;
        transition: all 0.3s ease;
        width: 100%;
        text-decoration: none;
        display: inline-block;
        text-align: center;
    }
    
    .ne-view-details-btn:hover {
        transform: translateY(-2px);
        box-shadow: 0 5px 15px rgba(128, 161, 186, 0.3);
        color: white;
        text-decoration: none;
    }
    
    /* Summary cards */
    .ne-summary-card {
        background: white;
        border-radius: 15px;
        padding: 1.5rem;
        margin-bottom: 1rem;
        box-shadow: 0 5px 15px rgba(0,0,0,0.08);
        border-left: 4px solid var(--secondary);
    }
    
    .ne-summary-title {
        font-size: 1.1rem;
        font-weight: 600;
        color: var(--dark);
        margin-bottom: 1rem;
    }
    
    .ne-summary-grid {
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 1rem;
    }
    
    .ne-summary-item {
        text-align: center;
        padding: 1rem;
        background: var(--light);
        border-radius: 10px;
    }
    
    .ne-summary-label {
        font-size: 0.8rem;
        color: var(--text);
        margin-bottom: 0.5rem;
    }
    
    .ne-summary-value {
        font-size: 1.2rem;
        font-weight: 700;
        color: var(--primary);
    }
    
    /* Form styles */
    .ne-form-card {
        background: white;
        border-radius: 15px;
        padding: 2rem;
        box-shadow: 0 5px 15px rgba(0,0,0,0.08);
        margin-bottom: 2rem;
    }
    
    .ne-form-title {
        font-size: 1.5rem;
        font-weight: 600;
        color: var(--dark);
        margin-bottom: 1.5rem;
        text-align: center;
    }
    
    /* Admin dashboard */
    .ne-metric-card {
        background: white;
        border-radius: 15px;
        padding: 1.5rem;
        text-align: center;
        box-shadow: 0 5px 15px rgba(0,0,0,0.08);
        border-top: 4px solid var(--primary);
    }
    
    .ne-metric-value {
        font-size: 2.5rem;
        font-weight: 700;
        color: var(--primary);
        margin-bottom: 0.5rem;
    }
    
    .ne-metric-label {
        color: var(--text);
        font-size: 0.9rem;
        margin-bottom: 0.5rem;
    }
    
    .ne-metric-change {
        font-size: 0.8rem;
        font-weight: 600;
    }
    
    .ne-metric-positive {
        color: #28a745;
    }
    
    .ne-metric-negative {
        color: #dc3545;
    }
    
    /* Back button */
    .ne-back-btn {
        background: var(--light);
        color: var(--dark);
        border: 1px solid var(--accent);
        padding: 0.8rem 1.5rem;
        border-radius: 25px;
        font-size: 0.9rem;
        font-weight: 600;
        cursor: pointer;
        transition: all 0.3s ease;
        margin-bottom: 2rem;
        text-decoration: none;
        display: inline-block;
    }
    
    .ne-back-btn:hover {
        background: var(--accent);
        color: var(--dark);
        text-decoration: none;
    }
</style>
""", unsafe_allow_html=True)

# ----------------------
# Initialize Session State
# ----------------------
if "current_page" not in st.session_state:
    st.session_state.current_page = "home"
if "user_type" not in st.session_state:
    st.session_state.user_type = None

# ----------------------
# API Configuration
# ----------------------
def resolve_api_base():
    """Resolve API base URL with fallback chain"""
    # Try environment variable first
    import os
    api_base = os.getenv("API_BASE")
    if api_base:
        return api_base.rstrip("/")
    
    # Try secrets.toml
    try:
        import os
        if os.path.exists("secrets.toml"):
            return st.secrets.get("API_BASE", "http://127.0.0.1:8000").rstrip("/")
    except:
        pass
    
    # Default fallback
    return "http://127.0.0.1:8000"

API_BASE = resolve_api_base()

# ----------------------
# API Functions
# ----------------------
def fetch_listings_from_api(params: Dict[str, Any]) -> Dict[str, Any]:
    """Fetch listings from API with timeout"""
    try:
        response = requests.get(f"{API_BASE}/listings", params=params, timeout=6)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        return {"error": str(e), "total": 0, "page": 1, "per_page": 50, "items": []}

def fetch_summary() -> Dict[str, Any]:
    """Fetch market summary from API"""
    try:
        response = requests.get(f"{API_BASE}/summary", timeout=6)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        return {"error": str(e)}

def fetch_admin_stats() -> Dict[str, Any]:
    """Fetch admin stats (total properties, new listings today)."""
    try:
        response = requests.get(f"{API_BASE}/admin/stats", timeout=6)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        return {"error": str(e)}

def fetch_admin_analytics() -> Dict[str, Any]:
    try:
        r = requests.get(f"{API_BASE}/admin/analytics", timeout=8)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.RequestException as e:
        return {"error": str(e)}

def create_seller_listing(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Create a new seller listing."""
    try:
        response = requests.post(f"{API_BASE}/seller/listings", json=payload, timeout=8)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        return {"error": str(e)}

def list_seller_listings(limit: int = 10) -> List[Dict[str, Any]]:
    try:
        response = requests.get(f"{API_BASE}/seller/listings", params={"limit": limit}, timeout=6)
        response.raise_for_status()
        data = response.json()
        return data if isinstance(data, list) else []
    except requests.exceptions.RequestException:
        return []

def predict_valuation(property_data: Dict[str, Any]) -> Optional[str]:
    """Predict property valuation"""
    try:
        response = requests.post(f"{API_BASE}/predict", json=property_data, timeout=3)
        response.raise_for_status()
        result = response.json()
        return result.get("valuation", "N/A")
    except requests.exceptions.RequestException:
        return "N/A"

def compute_valuations_for_items(items: List[Dict[str, Any]], max_items: int) -> List[str]:
    """Compute valuations for items concurrently"""
    limited_items = items[:max_items]
    valuations = ["N/A"] * len(items)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_index = {
            executor.submit(predict_valuation, item): i 
            for i, item in enumerate(limited_items)
        }
        
        for future in concurrent.futures.as_completed(future_to_index):
            index = future_to_index[future]
            try:
                valuation = future.result()
                valuations[index] = valuation
            except Exception:
                valuations[index] = "N/A"
    
    return valuations


def fetch_listings_sample(max_items: int = 1000, page_size: int = 200) -> List[Dict[str, Any]]:
    """Fetch a sample of listings across pages (for admin charts).
    Stops early once max_items are collected or pages exhausted.
    """
    collected: List[Dict[str, Any]] = []
    first = fetch_listings_from_api({"page": 1, "per_page": page_size})
    if first.get("items"):
        collected.extend(first["items"])
    total = int(first.get("total", 0) or 0)
    total_pages = max(1, (total + page_size - 1) // page_size)
    for p in range(2, min(total_pages, 5) + 1):
        page = fetch_listings_from_api({"page": p, "per_page": page_size})
        items = page.get("items", [])
        if items:
            collected.extend(items)
        if len(collected) >= max_items:
            break
    return collected[:max_items]

# ----------------------
# Page Functions
# ----------------------
def show_home_page():
    """Display the home page with login options"""
    st.markdown("""
    <div class="ne-home-container">
        <div class="ne-hero">
            <h1>🏠 NeuraEstate</h1>
            <p>AI-Powered Real Estate Platform</p>
            <p>Find your perfect home with intelligent insights and valuations</p>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Handle login buttons
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("🔍 Login as Buyer", key="login_buyer_btn", use_container_width=True):
            st.session_state.user_type = "buyer"
            st.session_state.current_page = "buyer"
            st.experimental_rerun()
    
    with col2:
        if st.button("🏢 Login as Seller", key="login_seller_btn", use_container_width=True):
            st.session_state.user_type = "seller"
            st.session_state.current_page = "seller"
            st.experimental_rerun()
    
    with col3:
        if st.button("👨‍💼 Login as Admin", key="login_admin_btn", use_container_width=True):
            st.session_state.user_type = "admin"
            st.session_state.current_page = "admin"
            st.experimental_rerun()

def show_buyer_page():
    """Display the buyer page with property search"""
    if st.button("← Back to Home", key="back_home_btn"):
        st.session_state.current_page = "home"
        st.session_state.user_type = None
        st.experimental_rerun()
    
    show_property_search()

def show_seller_page():
    """Display the seller page with property listing form"""
    if st.button("← Back to Home", key="back_home_seller_btn"):
        st.session_state.current_page = "home"
        st.session_state.user_type = None
        st.experimental_rerun()
    
    st.markdown("### 🏢 Seller Dashboard")
    st.markdown("---")
    
    # Property listing form
    with st.container():
        st.markdown("""
        <div class="ne-form-card">
            <div class="ne-form-title">List Your Property</div>
        </div>
        """, unsafe_allow_html=True)
        
        # Initialize dedup state
        if "last_seller_payload_key" not in st.session_state:
            st.session_state.last_seller_payload_key = None

        with st.form("property_listing_form", clear_on_submit=True):
            col1, col2 = st.columns(2)
            
            with col1:
                bhk = st.selectbox("BHK", [1, 2, 3, 4, 5, 6], index=1)
                rooms = st.number_input("Number of Rooms", min_value=1, max_value=10, value=2, step=1)
                bathrooms = st.number_input("Number of Bathrooms", min_value=0.0, max_value=10.0, value=2.0, step=0.5)
            
            with col2:
                area_sqft = st.number_input("Area (sqft)", min_value=100.0, max_value=10000.0, value=1000.0, step=50.0)
                locality = st.selectbox("Locality", ["Mumbai", "Pune"])
                price = st.number_input("Price (INR)", min_value=100000.0, max_value=100000000.0, value=5000000.0, step=100000.0)
            
            title = st.text_input("Property Title", value=f"{bhk} BHK Apartment for Sale")
            description = st.text_area("Description", value="Beautiful property in prime location")
            
            submitted = st.form_submit_button("List Property", type="primary", use_container_width=True)
            
            if submitted:
                payload = {
                    "title": title or f"{bhk} BHK Apartment",
                    "price": float(price) if price else None,
                    "area_sqft": float(area_sqft) if area_sqft else None,
                    "bhk": int(bhk) if bhk else None,
                    "bathrooms": float(bathrooms) if bathrooms is not None else None,
                    "city": locality,
                }
                payload_key = json.dumps(payload, sort_keys=True)
                if st.session_state.last_seller_payload_key == payload_key:
                    st.info("This listing was just submitted; skipping duplicate.")
                else:
                    with st.spinner("Submitting listing…"):
                        res = create_seller_listing(payload)
                    if res.get("error"):
                        st.error("Failed to submit listing: " + res["error"])
                    else:
                        st.session_state.last_seller_payload_key = payload_key
                        st.success("Property listed successfully!")
                        st.experimental_rerun()

        # Recent seller listings
        st.markdown("#### Your Recent Listings")
        seller_items = list_seller_listings(limit=10)
        if seller_items:
            df = pd.DataFrame(seller_items)
            st.dataframe(df[["id","title","price","area_sqft","bhk","bathrooms","city","created_at"]], use_container_width=True)
        else:
            st.info("No recent listings yet.")

def show_admin_page():
    """Display the admin page with analytics"""
    if st.button("← Back to Home", key="back_home_admin_btn"):
        st.session_state.current_page = "home"
        st.session_state.user_type = None
        st.experimental_rerun()
    
    st.markdown("### 👨‍💼 Admin Dashboard")
    st.markdown("---")
    
    # Admin analytics (live)
    col1, col2 = st.columns(2)

    stats = fetch_admin_stats()
    total_val = stats.get("total_properties", 0) if not stats.get("error") else 0
    new_today_val = stats.get("new_listings_today", 0) if not stats.get("error") else 0

    with col1:
        st.markdown(f"""
        <div class=\"ne-metric-card\">
            <div class=\"ne-metric-value\">{total_val:,}</div>
            <div class=\"ne-metric-label\">Total Properties</div>
            <div class=\"ne-metric-change ne-metric-positive\">&nbsp;</div>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        st.markdown(f"""
        <div class=\"ne-metric-card\">
            <div class=\"ne-metric-value\">{new_today_val:,}</div>
            <div class=\"ne-metric-label\">New Listings (Today)</div>
            <div class=\"ne-metric-change ne-metric-positive\">&nbsp;</div>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("### Market Analytics")
    st.info("Analytics coming soon.")

def show_property_search():
    """Display the property search interface"""
    # Initialize session state for filters
    if "page" not in st.session_state:
        st.session_state.page = 1
    if "city" not in st.session_state:
        st.session_state.city = ""
    if "min_bhk" not in st.session_state:
        st.session_state.min_bhk = 0
    if "price_range" not in st.session_state:
        st.session_state.price_range = "Any"
    if "area_range" not in st.session_state:
        st.session_state.area_range = "Any"
    
    # Price range mapping
    price_ranges = {
        "Any": (0, 0),
        "Under 50 Lakhs": (0, 5000000),
        "50 Lakhs - 1 Cr": (5000000, 10000000),
        "1 Cr - 2 Cr": (10000000, 20000000),
        "2 Cr - 3 Cr": (20000000, 30000000),
        "3 Cr - 5 Cr": (30000000, 50000000),
        "Above 5 Cr": (50000000, 0)
    }
    
    # Area range mapping
    area_ranges = {
        "Any": (0, 0),
        "Under 1000 sqft": (0, 1000),
        "1000 - 2000 sqft": (1000, 2000),
        "2000 - 3000 sqft": (2000, 3000),
        "3000 - 4000 sqft": (3000, 4000),
        "4000 - 5000 sqft": (4000, 5000),
        "Above 5000 sqft": (5000, 0)
    }
    
    # Layout
    left_col, right_col = st.columns([1, 2])
    
    with left_col:
        # (Removed header card above filters to eliminate extra white bar)
        
        st.markdown("<div class='ne-filters'>", unsafe_allow_html=True)
        
        # Location
        st.markdown("**Location**")
        def _on_city_change():
            st.session_state.page = 1
        st.text_input(
            "City",
            key="city",
            value=st.session_state.city,
            placeholder="Enter city name",
            on_change=_on_city_change,
        )

        # Property details
        st.markdown("**Property Details**")
        def _on_bhk_change():
            st.session_state.page = 1
        st.slider(
            "Min BHK",
            0,
            6,
            key="min_bhk",
            value=st.session_state.min_bhk,
            on_change=_on_bhk_change,
        )

        # Price range
        st.markdown("**Price Range**")
        def _on_price_range_change():
            st.session_state.page = 1
        st.selectbox(
            "Price Range",
            list(price_ranges.keys()),
            index=list(price_ranges.keys()).index(st.session_state.price_range),
            key="price_range",
            on_change=_on_price_range_change,
        )

        # Area range
        st.markdown("**Area Range**")
        def _on_area_range_change():
            st.session_state.page = 1
        st.selectbox(
            "Area Range",
            list(area_ranges.keys()),
            index=list(area_ranges.keys()).index(st.session_state.area_range),
            key="area_range",
            on_change=_on_area_range_change,
        )
        
        def _reset_filters():
            st.session_state.page = 1
            st.session_state.city = ""
            st.session_state.min_bhk = 0
            st.session_state.price_range = "Any"
            st.session_state.area_range = "Any"
            st.experimental_rerun()

        st.button("Reset Filters", key="reset_filters_btn", use_container_width=True, on_click=_reset_filters)
        
        st.markdown("</div>", unsafe_allow_html=True)
        
        # Summary card
        st.markdown("""
        <div class="ne-summary-card">
            <div class="ne-summary-title">Market Summary</div>
        </div>
        """, unsafe_allow_html=True)
        
        summary = fetch_summary()
        if summary and not summary.get("error"):
            # Format numbers safely
            min_price = f"₹{summary.get('min_price'):,.0f}" if summary.get('min_price') is not None else "N/A"
            max_price = f"₹{summary.get('max_price'):,.0f}" if summary.get('max_price') is not None else "N/A"
            median_price = f"₹{summary.get('median_price'):,.0f}" if summary.get('median_price') is not None else "N/A"
            avg_price = f"₹{summary.get('avg_price_per_sqft'):,.0f}" if summary.get('avg_price_per_sqft') is not None else "N/A"
            
            st.markdown(f"""
            <div class="ne-summary-grid">
                <div class="ne-summary-item">
                    <div class="ne-summary-label">Min Price</div>
                    <div class="ne-summary-value">{min_price}</div>
                </div>
                <div class="ne-summary-item">
                    <div class="ne-summary-label">Max Price</div>
                    <div class="ne-summary-value">{max_price}</div>
                </div>
                <div class="ne-summary-item">
                    <div class="ne-summary-label">Median Price</div>
                    <div class="ne-summary-value">{median_price}</div>
                </div>
                <div class="ne-summary-item">
                    <div class="ne-summary-label">Avg Price/sqft</div>
                    <div class="ne-summary-value">{avg_price}</div>
                </div>
            </div>
            """, unsafe_allow_html=True)
        elif summary.get("error"):
            st.warning("Summary not available: " + str(summary.get("error")))
    
    with right_col:
        # Convert filter selections to API parameters
        min_price_val, max_price_val = price_ranges[st.session_state.price_range]
        min_area_val, max_area_val = area_ranges[st.session_state.area_range]
        
        # Build API params - hardcode per_page to 50
        params = {
            "page": int(st.session_state.page or 1),
            "per_page": 50,  # Fixed to 50 as requested
            "city": (st.session_state.city or "").strip(),
            "min_bhk": int(st.session_state.min_bhk or 0),
            "max_price": float(max_price_val) if max_price_val > 0 else 0.0,
            "min_area": float(min_area_val) if min_area_val > 0 else 0.0,
            # When "Any" is selected, do NOT cap max_area; pass 0.0 to disable the filter
            "max_area": float(max_area_val) if max_area_val > 0 else 0.0,
        }
        
        # Ensure page is always >= 1
        if params["page"] < 1:
            params["page"] = 1
            st.session_state.page = 1
        
        # Fetch listings
        with st.spinner("Loading listings…"):
            api_data = fetch_listings_from_api(params)
        
        if api_data.get("error"):
            st.error(f"API error: {api_data['error']}")
            total = 0
            items = []
        else:
            total = int(api_data.get("total", 0))
            items = api_data.get("items", [])
        
        # Pagination - fixed per_page to 50
        per_page = 50
        total_pages = max(1, math.ceil(total / per_page)) if per_page > 0 else 1
        
        # Ensure current page is within valid range
        if total_pages > 0:
            if st.session_state.page > total_pages:
                st.session_state.page = total_pages
            if st.session_state.page < 1:
                st.session_state.page = 1
        else:
            st.session_state.page = 1
        
        st.markdown(f"**Found {total} listings**")
        
        # Pagination controls
        col1, col2, col3 = st.columns([1, 1, 1])
        
        with col1:
            if st.button("← Previous", disabled=st.session_state.page <= 1, key="prev_btn"):
                if st.session_state.page > 1:
                    st.session_state.page -= 1
        
        with col2:
            st.markdown(f"**Page {st.session_state.page} of {total_pages}**")
        
        with col3:
            if st.button("Next →", disabled=st.session_state.page >= total_pages, key="next_btn"):
                if st.session_state.page < total_pages:
                    st.session_state.page += 1
                else:
                    st.warning("Already on the last page")
        
        # Display properties
        if items:
            # Compute valuations automatically
            with st.spinner("Computing valuations..."):
                valuations = compute_valuations_for_items(items, len(items))
            
            # Display property cards
            st.markdown('<div class="ne-property-grid">', unsafe_allow_html=True)
            
            for i, item in enumerate(items):
                valuation = valuations[i] if i < len(valuations) else "N/A"
                
                # Determine valuation badge - use a simple heuristic if API fails
                if valuation == "Underpriced":
                    badge_class = "ne-valuation-underpriced"
                    badge_text = "Underpriced"
                elif valuation == "Overpriced":
                    badge_class = "ne-valuation-overpriced"
                    badge_text = "Overpriced"
                elif valuation == "Fairly Priced":
                    badge_class = "ne-valuation-fair"
                    badge_text = "Fairly Priced"
                else:
                    # Fallback: simple heuristic based on price per sqft
                    price = item.get('price', 0)
                    area = item.get('area_sqft', 1)
                    if price > 0 and area > 0:
                        price_per_sqft = price / area
                        if price_per_sqft > 15000:  # High price per sqft
                            badge_class = "ne-valuation-overpriced"
                            badge_text = "Overpriced"
                        elif price_per_sqft < 8000:  # Low price per sqft
                            badge_class = "ne-valuation-underpriced"
                            badge_text = "Underpriced"
                        else:
                            badge_class = "ne-valuation-fair"
                            badge_text = "Fairly Priced"
                    else:
                        badge_class = "ne-valuation-fair"
                        badge_text = "N/A"
                
                # Property details
                price = f"₹{item.get('price'):,.0f}" if item.get('price') else "Price on request"
                area = f"{item.get('area_sqft') or 'N/A'} sqft"
                bhk = f"{item.get('bhk') or 'N/A'} BHK"
                bathrooms = f"{item.get('bathrooms') or 'N/A'} Bath"
                city = item.get('city') or 'N/A'
                title = (item.get('title') or 'Property')[:60] + "..." if len(item.get('title') or '') > 60 else (item.get('title') or 'Property')
                
                # Create property card
                card_html = f"""
                <div class="ne-property-card">
                    <div class="ne-property-image">🏠</div>
                    <div class="ne-property-content">
                        <div class="ne-property-title">{title}</div>
                        <div class="ne-property-price">{price}</div>
                        <div class="ne-valuation-badge {badge_class}">{badge_text}</div>
                        <div class="ne-property-details">
                            <div class="ne-property-detail">
                                <span class="ne-property-detail-icon">📐</span>
                                <span>{area}</span>
                            </div>
                            <div class="ne-property-detail">
                                <span class="ne-property-detail-icon">🏠</span>
                                <span>{bhk}</span>
                            </div>
                            <div class="ne-property-detail">
                                <span class="ne-property-detail-icon">🚿</span>
                                <span>{bathrooms}</span>
                            </div>
                            <div class="ne-property-detail">
                                <span class="ne-property-detail-icon">📍</span>
                                <span>{city}</span>
                            </div>
                        </div>
                        <a href="#" class="ne-view-details-btn">View Details</a>
                    </div>
                </div>
                """
                
                st.markdown(card_html, unsafe_allow_html=True)
            
            st.markdown('</div>', unsafe_allow_html=True)
        else:
            st.info("No listings found for the selected filters.")

# ----------------------
# Main Page Router
# ----------------------
def main():
    # Check if user selected a type from home page
    if st.session_state.user_type == "buyer":
        st.session_state.current_page = "buyer"
    elif st.session_state.user_type == "seller":
        st.session_state.current_page = "seller"
    elif st.session_state.user_type == "admin":
        st.session_state.current_page = "admin"
    
    # Route to appropriate page
    if st.session_state.current_page == "home":
        show_home_page()
    elif st.session_state.current_page == "buyer":
        show_buyer_page()
    elif st.session_state.current_page == "seller":
        show_seller_page()
    elif st.session_state.current_page == "admin":
        show_admin_page()

if __name__ == "__main__":
    main()
