"""
SalesTrendsDashboard — FastAPI Backend
======================================
Single source of truth for all sales analytics.
Deploys as a Zoho Catalyst Advanced I/O serverless function.

RULES FOR ANY AI AGENT WORKING ON THIS FILE:
- NEVER hardcode credentials. All secrets via os.environ.get() only.
- NEVER change the Catalyst handler pattern at the bottom of this file.
- NEVER add new frameworks or ORMs without updating requirements.txt.
- NEVER modify column names in COLUMN_MAPPING without updating the Excel source.
- Data loads once per warm instance (module-level singleton). Do not break this.
"""

import os
import io
import json
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Any

import pandas as pd
import numpy as np
import requests as http_requests
from fastapi import FastAPI, Query, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("salestrends")

# ============================================================================
# CONFIGURATION — all values from environment variables only
# ============================================================================

GITHUB_TOKEN  = os.environ.get("GITHUB_TOKEN", "")
GITHUB_REPO   = os.environ.get("GITHUB_REPO", "")          # e.g. shubhkrishna19/salestrendsdashboard
DATA_FILE     = os.environ.get("DATA_FILE", "data.xlsx")   # filename inside the repo or local path
SHEET_NAME    = os.environ.get("SHEET_NAME", "Final Sale Data")

# Column mapping: Excel headers → internal names
COLUMN_MAPPING = {
    "Final Order date": "Date",
    "Main Parties":     "Platform",
    "Group Name":       "Category",
    "Item Desc":        "Product",
    "Alias":            "SKU",
    "Sale (Qty.)":      "Sale_Qty",
    "Sale Return (Qty.)": "Return_Qty",
    "Sale (Amt.)":      "Sale_Amt",
    "Sale Return (Amt.)": "Return_Amt",
    "Tax Value":        "Tax",
    "Order ID":         "Order_ID",
    "Return Type":      "Return_Reason",
    "Valid/Invalid":    "Return_Valid",
}

PLATFORM_COLORS = {
    "Amazon":       "#FF9900",
    "Flipkart":     "#2874F0",
    "Pepperfry":    "#F16521",
    "Myntra":       "#FF3E6C",
    "Reliance":     "#005D32",
    "Shopify":      "#95BF47",
    "Urban Ladder": "#7B3F00",
    "Indiamart":    "#F36E1E",
    "Others":       "#747D8C",
}

# ============================================================================
# HELPERS
# ============================================================================

def fmt_inr(amount) -> str:
    """Format a number as Indian Rupee string (₹1.5Cr / ₹2.5L / ₹50K)."""
    if amount is None or (isinstance(amount, float) and np.isnan(amount)):
        return "₹0"
    a = abs(float(amount))
    if a >= 1_00_00_000:
        return f"₹{amount/1_00_00_000:.2f}Cr"
    if a >= 1_00_000:
        return f"₹{amount/1_00_000:.2f}L"
    if a >= 1_000:
        return f"₹{amount/1_000:.1f}K"
    return f"₹{amount:.0f}"

def safe_float(v) -> float:
    try:
        f = float(v)
        return 0.0 if np.isnan(f) else f
    except Exception:
        return 0.0

def _parse_filters(
    platform: Optional[str],
    category: Optional[str],
    start_date: Optional[str],
    end_date: Optional[str],
    product: Optional[str] = None,
) -> Dict:
    f: Dict[str, Any] = {}
    if platform:
        f["platform"] = [p.strip() for p in platform.split(",") if p.strip()]
    if category:
        f["category"] = [c.strip() for c in category.split(",") if c.strip()]
    if start_date:
        f["start_date"] = start_date
    if end_date:
        f["end_date"] = end_date
    if product and product.lower() not in ("all", ""):
        f["product"] = product
    return f

# ============================================================================
# DATA MANAGER — module-level singleton, loads once per warm instance
# ============================================================================

class DataManager:
    """Load, process, and cache the sales Excel data."""

    def __init__(self):
        self._raw_df: Optional[pd.DataFrame] = None
        self._df: Optional[pd.DataFrame] = None
        self._loaded_at: Optional[datetime] = None
        self._load()

    # ------------------------------------------------------------------
    # Loading
    # ------------------------------------------------------------------

    def _load(self):
        log.info("DataManager: starting data load...")
        df = None

        # 1. Try GitHub raw download (primary for Catalyst)
        if GITHUB_TOKEN and GITHUB_REPO:
            df = self._load_github()

        # 2. Fall back to local file (for local development)
        if df is None:
            df = self._load_local()

        if df is not None:
            self._raw_df = df
            self._df = self._process(df)
            self._loaded_at = datetime.utcnow()
            log.info(f"DataManager: loaded {len(self._df)} rows.")
        else:
            log.error("DataManager: no data source available!")

    def _load_github(self) -> Optional[pd.DataFrame]:
        """Download data.xlsx from GitHub using raw URL (handles large files)."""
        try:
            branch = "main"
            url = f"https://raw.githubusercontent.com/{GITHUB_REPO}/{branch}/{DATA_FILE}"
            headers = {"Authorization": f"token {GITHUB_TOKEN}"}
            log.info(f"Downloading from GitHub: {url}")
            
            # Rate limit handling - retry with exponential backoff
            max_retries = 3
            retry_delay = 5  # seconds
            for attempt in range(max_retries):
                resp = http_requests.get(url, headers=headers, timeout=120)
                
                if resp.status_code == 200:
                    df = pd.read_excel(io.BytesIO(resp.content), sheet_name=SHEET_NAME)
                    log.info(f"GitHub load success: {len(df)} rows")
                    return df
                elif resp.status_code == 429:
                    # Rate limited - wait and retry
                    log.warning(f"GitHub API rate limited. Attempt {attempt + 1}/{max_retries}. Waiting {retry_delay}s...")
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    resp.raise_for_status()
            
            # All retries exhausted
            log.error(f"GitHub API rate limit exceeded after {max_retries} attempts")
            return None
            
        except http_requests.exceptions.RequestException as e:
            log.warning(f"GitHub load failed: {e}")
            return None
        except Exception as e:
            log.warning(f"GitHub load failed: {e}")
            return None

    def _load_local(self) -> Optional[pd.DataFrame]:
        """Load from a local file path."""
        paths_to_try = [
            DATA_FILE,
            Path(__file__).parent / DATA_FILE,
            Path(__file__).parent.parent.parent / DATA_FILE,
        ]
        for p in paths_to_try:
            try:
                if Path(p).exists():
                    log.info(f"Loading local file: {p}")
                    df = pd.read_excel(p, sheet_name=SHEET_NAME)
                    log.info(f"Local load success: {len(df)} rows")
                    return df
            except Exception as e:
                log.warning(f"Local load failed ({p}): {e}")
        return None

    # ------------------------------------------------------------------
    # Processing
    # ------------------------------------------------------------------

    def _process(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df.columns = df.columns.str.strip()

        # Date column — keep only the primary date
        date_col = "Final Order date"
        drop_cols = [
            c for c in df.columns
            if c != date_col and (
                str(df[c].dtype).startswith("datetime64")
                or any(kw in c for kw in ("Date", "date", "Vch"))
            )
        ]
        df.drop(columns=drop_cols, errors="ignore", inplace=True)

        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

        # Numeric columns
        numeric = ["Sale (Qty.)", "Sale Return (Qty.)", "Sale (Amt.)", "Sale Return (Amt.)", "Tax Value"]
        for c in numeric:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

        # Derived columns
        df["Net_Revenue"] = df.get("Sale (Amt.)", 0) - df.get("Sale Return (Amt.)", 0)
        df["Net_Qty"]     = df.get("Sale (Qty.)", 0) - df.get("Sale Return (Qty.)", 0)

        # Friendly aliases
        df["Platform"] = df.get("Main Parties")
        df["Category"] = df.get("Group Name")
        df["Product"]  = df.get("Item Desc")
        df["SKU"]      = df.get("Alias")

        # Fiscal year (India: Apr–Mar)
        if date_col in df.columns:
            def _fy(d):
                if pd.isna(d):
                    return None
                return f"FY{d.year}-{str(d.year+1)[-2:]}" if d.month >= 4 \
                    else f"FY{d.year-1}-{str(d.year)[-2:]}"
            df["FY"] = df[date_col].apply(_fy)
            df["Month"] = df[date_col].dt.to_period("M").astype(str)

        # Categoricals for memory efficiency
        for c in ("Platform", "Category", "FY"):
            if c in df.columns:
                df[c] = df[c].astype("category")

        return df

    # ------------------------------------------------------------------
    # Filtering
    # ------------------------------------------------------------------

    @property
    def ready(self) -> bool:
        return self._df is not None and not self._df.empty

    def apply_filters(self, f: Dict) -> pd.DataFrame:
        if not self.ready:
            return pd.DataFrame()
        df = self._df.copy()
        date_col = "Final Order date"

        if f.get("platform"):
            df = df[df["Platform"].isin(f["platform"])]
        if f.get("category"):
            df = df[df["Category"].isin(f["category"])]
        if f.get("product"):
            df = df[df["Product"] == f["product"]]
        if f.get("start_date") and date_col in df.columns:
            df = df[df[date_col] >= pd.to_datetime(f["start_date"])]
        if f.get("end_date") and date_col in df.columns:
            df = df[df[date_col] <= pd.to_datetime(f["end_date"])]
        return df

    # ------------------------------------------------------------------
    # Aggregation helpers
    # ------------------------------------------------------------------

    def filter_options(self) -> Dict:
        if not self.ready:
            return {}
        d = self._df
        out: Dict[str, Any] = {
            "platforms":  sorted(d["Platform"].dropna().unique().tolist()),
            "categories": sorted(d["Category"].dropna().unique().tolist()),
        }
        date_col = "Final Order date"
        if date_col in d.columns:
            dates = d[date_col].dropna()
            if not dates.empty:
                out["date_range"] = {
                    "min": dates.min().strftime("%Y-%m-%d"),
                    "max": dates.max().strftime("%Y-%m-%d"),
                }
        return out

    def kpis(self, df: pd.DataFrame) -> Dict:
        if df.empty:
            return {k: 0 for k in ("total_revenue", "sales_volume", "total_orders", "aov",
                                   "total_returns", "return_rate")}
        rev   = safe_float(df["Net_Revenue"].sum())
        qty   = safe_float(df["Net_Qty"].sum())
        orders = len(df)
        aov   = rev / orders if orders else 0
        ret   = safe_float(df.get("Sale Return (Amt.)", pd.Series([0])).sum())
        rate  = (ret / rev * 100) if rev > 0 else 0
        return {
            "total_revenue":           rev,
            "total_revenue_formatted": fmt_inr(rev),
            "sales_volume":            safe_float(qty),
            "total_orders":            orders,
            "aov":                     aov,
            "aov_formatted":           fmt_inr(aov),
            "total_returns":           ret,
            "total_returns_formatted": fmt_inr(ret),
            "return_rate":             round(rate, 2),
        }

    def revenue_trend(self, df: pd.DataFrame) -> List[Dict]:
        date_col = "Final Order date"
        if df.empty or date_col not in df.columns:
            return []
        tmp = df.copy()
        date_range = (tmp[date_col].max() - tmp[date_col].min()).days
        freq = "D" if date_range <= 45 else ("W" if date_range <= 180 else "ME")
        tmp["_period"] = tmp[date_col].dt.to_period(
            "M" if freq == "ME" else ("W" if freq == "W" else "D")
        )
        t = tmp.groupby("_period")["Net_Revenue"].sum().reset_index()
        t["_period"] = t["_period"].astype(str)
        return [{"date": r["_period"], "revenue": safe_float(r["Net_Revenue"])} for _, r in t.iterrows()]

    def platform_data(self, df: pd.DataFrame) -> List[Dict]:
        if df.empty or "Platform" not in df.columns:
            return []
        g = df.groupby("Platform").agg(
            Revenue=("Net_Revenue", "sum"),
            Quantity=("Net_Qty", "sum"),
            Orders=("Order_ID", "count"),
            Returns=("Sale Return (Amt.)", "sum"),
        ).reset_index()
        g["AOV"]         = g["Revenue"] / g["Orders"].replace(0, np.nan)
        g["Return_Rate"] = (g["Returns"] / g["Revenue"].replace(0, np.nan) * 100).fillna(0)
        g["Color"]       = g["Platform"].map(PLATFORM_COLORS).fillna("#747D8C")
        g = g.sort_values("Revenue", ascending=False)
        return [
            {
                "platform":    str(r["Platform"]),
                "revenue":     safe_float(r["Revenue"]),
                "quantity":    safe_float(r["Quantity"]),
                "orders":      int(r["Orders"]),
                "returns":     safe_float(r["Returns"]),
                "aov":         safe_float(r["AOV"]),
                "return_rate": round(safe_float(r["Return_Rate"]), 2),
                "color":       r["Color"],
            }
            for _, r in g.iterrows()
        ]

    def category_data(self, df: pd.DataFrame) -> List[Dict]:
        if df.empty or "Category" not in df.columns:
            return []
        g = df.groupby("Category").agg(
            Revenue=("Net_Revenue", "sum"),
            Quantity=("Net_Qty", "sum"),
        ).reset_index().sort_values("Revenue", ascending=False)
        return [{"category": str(r["Category"]), "revenue": safe_float(r["Revenue"]),
                 "quantity": safe_float(r["Quantity"])} for _, r in g.iterrows()]

    def top_products(self, df: pd.DataFrame, n: int = 10) -> List[Dict]:
        if df.empty or "Product" not in df.columns:
            return []
        g = df.groupby("Product").agg(
            Revenue=("Net_Revenue", "sum"),
            Quantity=("Net_Qty", "sum"),
            Orders=("Order_ID", "count"),
        ).reset_index().sort_values("Revenue", ascending=False).head(n)
        return [{"product": str(r["Product"]), "revenue": safe_float(r["Revenue"]),
                 "quantity": safe_float(r["Quantity"]), "orders": int(r["Orders"])}
                for _, r in g.iterrows()]

    def top_products_by_volume(self, df: pd.DataFrame, n: int = 10) -> List[Dict]:
        if df.empty or "Product" not in df.columns:
            return []
        g = df.groupby("Product").agg(
            Revenue=("Net_Revenue", "sum"),
            Quantity=("Net_Qty", "sum"),
        ).reset_index().sort_values("Quantity", ascending=False).head(n)
        return [{"product": str(r["Product"]), "revenue": safe_float(r["Revenue"]),
                 "quantity": safe_float(r["Quantity"])} for _, r in g.iterrows()]

    def returns_by_platform(self, df: pd.DataFrame) -> List[Dict]:
        if df.empty or "Platform" not in df.columns:
            return []
        g = df.groupby("Platform").agg(
            Returns=("Sale Return (Amt.)", "sum"),
            Revenue=("Net_Revenue", "sum"),
            Return_Qty=("Sale Return (Qty.)", "sum"),
        ).reset_index()
        g["Rate"] = (g["Returns"] / g["Revenue"].replace(0, np.nan) * 100).fillna(0)
        g = g.sort_values("Returns", ascending=False)
        return [{"platform": str(r["Platform"]), "returns": safe_float(r["Returns"]),
                 "return_qty": safe_float(r["Return_Qty"]), "rate": round(safe_float(r["Rate"]), 2)}
                for _, r in g.iterrows()]

    def returns_trend(self, df: pd.DataFrame) -> List[Dict]:
        date_col = "Final Order date"
        if df.empty or date_col not in df.columns:
            return []
        tmp = df.copy()
        tmp["_month"] = tmp[date_col].dt.to_period("M")
        g = tmp.groupby("_month").agg(
            Revenue=("Net_Revenue", "sum"),
            Returns=("Sale Return (Amt.)", "sum"),
        ).reset_index()
        g["_month"] = g["_month"].astype(str)
        g["rate"] = (g["Returns"] / g["Revenue"].replace(0, np.nan) * 100).fillna(0)
        return [{"month": r["_month"], "returns": safe_float(r["Returns"]),
                 "revenue": safe_float(r["Revenue"]), "rate": round(safe_float(r["rate"]), 2)}
                for _, r in g.iterrows()]

    def returns_by_reason(self, df: pd.DataFrame) -> List[Dict]:
        col = "Return Type"
        if df.empty or col not in df.columns:
            return []
        g = df.groupby(col)["Sale Return (Amt.)"].sum().reset_index()
        g.columns = ["reason", "amount"]
        g = g.sort_values("amount", ascending=False)
        return [{"reason": str(r["reason"]), "amount": safe_float(r["amount"])} for _, r in g.iterrows()]

    def returns_validity(self, df: pd.DataFrame) -> List[Dict]:
        col = "Valid/Invalid"
        if df.empty or col not in df.columns:
            return []
        g = df.groupby(col)["Sale Return (Amt.)"].sum().reset_index()
        g.columns = ["validity", "amount"]
        return [{"validity": str(r["validity"]), "amount": safe_float(r["amount"])} for _, r in g.iterrows()]

    def operations_summary(self, df: pd.DataFrame) -> Dict:
        date_col = "Final Order date"
        if df.empty:
            return {}
        tax = safe_float(df["Tax Value"].sum()) if "Tax Value" in df.columns else 0
        orders_per_month: List[Dict] = []
        if date_col in df.columns:
            tmp = df.copy()
            tmp["_month"] = tmp[date_col].dt.to_period("M")
            g = tmp.groupby("_month").agg(
                Orders=("Order_ID", "count"),
                Revenue=("Net_Revenue", "sum"),
            ).reset_index()
            g["_month"] = g["_month"].astype(str)
            orders_per_month = [{"month": r["_month"], "orders": int(r["Orders"]),
                                  "revenue": safe_float(r["Revenue"])} for _, r in g.iterrows()]
        return {
            "tax_collected":           tax,
            "tax_collected_formatted": fmt_inr(tax),
            "orders_per_month":        orders_per_month,
        }

    def export_csv(self, df: pd.DataFrame) -> str:
        """Return CSV string of filtered data."""
        if df.empty:
            return ""
        export_cols = [c for c in ["Final Order date", "Platform", "Category", "Product",
                                    "SKU", "Net_Revenue", "Net_Qty", "Sale Return (Amt.)"]
                       if c in df.columns]
        return df[export_cols].to_csv(index=False)


# Module-level singleton — loads once on cold start, reused on warm invocations
_dm = DataManager()

# ============================================================================
# CACHE LAYER — in-memory cache for filtered results
# ============================================================================

class CacheManager:
    """Simple in-memory cache for API responses."""
    
    def __init__(self, ttl_seconds: int = 300):  # 5 minutes default TTL
        self._cache: Dict[str, tuple] = {}  # key -> (result, timestamp)
        self._ttl = ttl_seconds
    
    def _make_key(self, endpoint: str, params: Dict) -> str:
        """Generate cache key from endpoint and params."""
        sorted_params = sorted(params.items())
        return f"{endpoint}:{json.dumps(sorted_params)}"
    
    def get(self, endpoint: str, params: Dict) -> Optional[Any]:
        """Get cached result if still valid."""
        key = self._make_key(endpoint, params)
        if key in self._cache:
            result, timestamp = self._cache[key]
            if time.time() - timestamp < self._ttl:
                return result
            else:
                del self._cache[key]
        return None
    
    def set(self, endpoint: str, params: Dict, result: Any):
        """Cache a result."""
        key = self._make_key(endpoint, params)
        self._cache[key] = (result, time.time())
    
    def clear(self):
        """Clear all cache."""
        self._cache.clear()

_cache = CacheManager(ttl_seconds=300)

# ============================================================================
# FASTAPI APPLICATION
# ============================================================================

app = FastAPI(title="SalesTrendsDashboard", version="2.0.0", docs_url=None, redoc_url=None)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve the HTML dashboard
_HTML_PATH = Path(__file__).parent / "dashboard.html"

@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    try:
        html = _HTML_PATH.read_text(encoding="utf-8")
        return HTMLResponse(html)
    except FileNotFoundError:
        return HTMLResponse("<h1>dashboard.html missing from function directory</h1>", status_code=500)

# ------------------------------------------------------------------
# Status / Health
# ------------------------------------------------------------------

@app.get("/api/health")
async def health():
    return {
        "status": "ok",
        "data_loaded": _dm.ready,
        "rows": len(_dm._df) if _dm.ready else 0,
        "loaded_at": _dm._loaded_at.isoformat() if _dm._loaded_at else None,
        "cache_entries": len(_cache._cache),
        "cache_ttl_seconds": _cache._ttl,
    }

@app.post("/api/cache/clear")
async def clear_cache():
    """Manually clear the cache to force fresh data."""
    _cache.clear()
    return {"status": "ok", "message": "Cache cleared"}

@app.get("/api/filters")
async def get_filters():
    return _dm.filter_options()

# ------------------------------------------------------------------
# KPIs
# ------------------------------------------------------------------

@app.get("/api/kpis")
async def get_kpis(
    platform:   Optional[str] = None,
    category:   Optional[str] = None,
    start_date: Optional[str] = None,
    end_date:   Optional[str] = None,
    product:    Optional[str] = None,
):
    params = {"platform": platform, "category": category, "start_date": start_date, "end_date": end_date, "product": product}
    
    # Check cache first
    cached = _cache.get("kpis", params)
    if cached is not None:
        return cached
    
    df = _dm.apply_filters(_parse_filters(platform, category, start_date, end_date, product))
    result = _dm.kpis(df)
    
    # Cache the result
    _cache.set("kpis", params, result)
    return result

# ------------------------------------------------------------------
# Executive Summary
# ------------------------------------------------------------------

@app.get("/api/trend")
async def get_trend(
    platform: Optional[str]=None, category: Optional[str]=None,
    start_date: Optional[str]=None, end_date: Optional[str]=None,
    product: Optional[str]=None,
):
    params = {"platform": platform, "category": category, "start_date": start_date, "end_date": end_date, "product": product}
    
    cached = _cache.get("trend", params)
    if cached is not None:
        return cached
    
    df = _dm.apply_filters(_parse_filters(platform, category, start_date, end_date, product))
    result = _dm.revenue_trend(df)
    
    _cache.set("trend", params, result)
    return result

@app.get("/api/platforms")
async def get_platforms(
    platform: Optional[str]=None, category: Optional[str]=None,
    start_date: Optional[str]=None, end_date: Optional[str]=None,
    product: Optional[str]=None,
):
    params = {"platform": platform, "category": category, "start_date": start_date, "end_date": end_date, "product": product}
    
    cached = _cache.get("platforms", params)
    if cached is not None:
        return cached
    
    df = _dm.apply_filters(_parse_filters(platform, category, start_date, end_date, product))
    result = _dm.platform_data(df)
    
    _cache.set("platforms", params, result)
    return result

@app.get("/api/categories")
async def get_categories(
    platform: Optional[str]=None, category: Optional[str]=None,
    start_date: Optional[str]=None, end_date: Optional[str]=None,
    product: Optional[str]=None,
):
    df = _dm.apply_filters(_parse_filters(platform, category, start_date, end_date, product))
    return _dm.category_data(df)

# ------------------------------------------------------------------
# Sales Analysis
# ------------------------------------------------------------------

@app.get("/api/products")
async def get_products(
    n: int = Query(10, ge=5, le=50),
    platform: Optional[str]=None, category: Optional[str]=None,
    start_date: Optional[str]=None, end_date: Optional[str]=None,
    product: Optional[str]=None,
):
    df = _dm.apply_filters(_parse_filters(platform, category, start_date, end_date, product))
    return _dm.top_products(df, n)

@app.get("/api/products/volume")
async def get_products_volume(
    n: int = Query(10, ge=5, le=50),
    platform: Optional[str]=None, category: Optional[str]=None,
    start_date: Optional[str]=None, end_date: Optional[str]=None,
):
    df = _dm.apply_filters(_parse_filters(platform, category, start_date, end_date))
    return _dm.top_products_by_volume(df, n)

# ------------------------------------------------------------------
# Returns Analysis
# ------------------------------------------------------------------

@app.get("/api/returns/by-platform")
async def get_returns_platform(
    platform: Optional[str]=None, category: Optional[str]=None,
    start_date: Optional[str]=None, end_date: Optional[str]=None,
):
    df = _dm.apply_filters(_parse_filters(platform, category, start_date, end_date))
    return _dm.returns_by_platform(df)

@app.get("/api/returns/trend")
async def get_returns_trend(
    platform: Optional[str]=None, category: Optional[str]=None,
    start_date: Optional[str]=None, end_date: Optional[str]=None,
):
    df = _dm.apply_filters(_parse_filters(platform, category, start_date, end_date))
    return _dm.returns_trend(df)

@app.get("/api/returns/by-reason")
async def get_returns_reason(
    platform: Optional[str]=None, category: Optional[str]=None,
    start_date: Optional[str]=None, end_date: Optional[str]=None,
):
    df = _dm.apply_filters(_parse_filters(platform, category, start_date, end_date))
    return _dm.returns_by_reason(df)

@app.get("/api/returns/validity")
async def get_returns_validity(
    platform: Optional[str]=None, category: Optional[str]=None,
    start_date: Optional[str]=None, end_date: Optional[str]=None,
):
    df = _dm.apply_filters(_parse_filters(platform, category, start_date, end_date))
    return _dm.returns_validity(df)

# ------------------------------------------------------------------
# Operations
# ------------------------------------------------------------------

@app.get("/api/operations")
async def get_operations(
    platform: Optional[str]=None, category: Optional[str]=None,
    start_date: Optional[str]=None, end_date: Optional[str]=None,
):
    df = _dm.apply_filters(_parse_filters(platform, category, start_date, end_date))
    return _dm.operations_summary(df)

@app.get("/api/export")
async def export_csv(
    platform: Optional[str]=None, category: Optional[str]=None,
    start_date: Optional[str]=None, end_date: Optional[str]=None,
):
    from fastapi.responses import Response
    df = _dm.apply_filters(_parse_filters(platform, category, start_date, end_date))
    csv = _dm.export_csv(df)
    return Response(
        content=csv,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=sales_export.csv"}
    )

# ============================================================================
# CATALYST SERVERLESS HANDLER
# DO NOT MODIFY — this is the entry point Catalyst invokes.
# ============================================================================
try:
    from mangum import Mangum
    handler = Mangum(app, lifespan="off")
except ImportError:
    handler = None  # local dev without mangum
