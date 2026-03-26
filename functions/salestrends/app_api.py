"""
SalesTrendsDashboard - FastAPI backend.

This module owns the data loading, preprocessing, caching, and API surface for
the SalesTrends dashboard. It supports both raw workbook loading and a faster
snapshot-based runtime path for Vercel and future AppSail use.
"""

from __future__ import annotations

import io
import json
import logging
import os
import re
import tempfile
import time
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests as http_requests
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, Response

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("salestrends")


# ============================================================================
# CONFIGURATION
# ============================================================================

BASE_DIR = Path(__file__).parent
HTML_PATH = BASE_DIR / "dashboard.html"

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
GITHUB_REPO = os.environ.get("GITHUB_REPO", "")
DATA_FILE = os.environ.get("DATA_FILE", "data.xlsx")
SHEET_NAME = os.environ.get("SHEET_NAME", "Final Sale Data")
DATA_URL = os.environ.get("DATA_URL", "")
ORDER_HUB_BASE_URL = os.environ.get("ORDER_HUB_BASE_URL", "").strip()
SUMMARY_SHEET_NAME = os.environ.get("SUMMARY_SHEET_NAME", "Sales Analytics Dashboard")
SNAPSHOT_FILE = os.environ.get("SNAPSHOT_FILE", str(BASE_DIR / "data_snapshot.csv.gz"))
SNAPSHOT_META_FILE = os.environ.get("SNAPSHOT_META_FILE", str(BASE_DIR / "snapshot_meta.json"))

RAW_REQUIRED_COLUMNS = [
    "Final Order date",
    "Main Parties",
    "Group Name",
    "Item Desc",
    "Alias",
    "Sale (Qty.)",
    "Sale Return (Qty.)",
    "Sale (Amt.)",
    "Sale Return (Amt.)",
    "Tax Value",
    "Order ID",
    "Return Type",
    "Valid/Invalid",
]

NUMERIC_COLUMNS = [
    "Sale (Qty.)",
    "Sale Return (Qty.)",
    "Sale (Amt.)",
    "Sale Return (Amt.)",
    "Tax Value",
]

SNAPSHOT_COLUMNS = [
    "order_date",
    "platform_raw",
    "platform_label",
    "category",
    "product",
    "sku",
    "sale_qty",
    "return_qty_signed",
    "return_qty",
    "gross_sales",
    "return_value_signed",
    "return_value",
    "net_qty",
    "net_revenue",
    "tax",
    "order_id",
    "return_reason",
    "return_validity",
    "fy",
    "month",
    "weekday",
]

ORDER_HUB_ANALYTICS_PATH = "/api/analytics/salestrends-snapshot.csv"
ORDER_HUB_REQUIRED_COLUMNS = [
    "order_date",
    "platform_raw",
    "category",
    "product",
    "sku",
    "sale_qty",
    "return_qty_signed",
    "gross_sales",
    "return_value_signed",
    "tax",
    "order_id",
    "return_reason",
    "return_validity",
]

TREND_MODE_OPTIONS = ("auto", "daily", "weekly", "monthly")
TREND_MODE_LABELS = {
    "auto": "Auto",
    "daily": "Daily",
    "weekly": "Weekly",
    "monthly": "Monthly",
}
TREND_MODE_PERIODS = {
    "daily": "D",
    "weekly": "W",
    "monthly": "M",
}

SEARCH_DIMENSION_COLUMNS = [
    "product_search_text",
    "product_search_compact",
    "sku_search_text",
    "sku_search_compact",
    "sku_base",
    "sku_base_search_text",
    "sku_base_search_compact",
    "sku_extension",
]

PLATFORM_DISPLAY_NAMES = {
    "Amazon Online Sale": "Amazon",
    "Flipkart Online Sale": "Flipkart",
    "Pepperfry Private Limited": "Pepperfry",
    "Myntra Online Sale": "Myntra",
    "Reliance Retail Limited.": "Reliance",
    "Shopify Online Sale": "Shopify",
    "Other Offline Parties": "Offline",
    "Indiamart": "Indiamart",
}

PLATFORM_COLORS = {
    "Amazon Online Sale": "#F59E0B",
    "Flipkart Online Sale": "#2563EB",
    "Pepperfry Private Limited": "#EA580C",
    "Myntra Online Sale": "#DB2777",
    "Reliance Retail Limited.": "#166534",
    "Shopify Online Sale": "#16A34A",
    "Other Offline Parties": "#64748B",
    "Indiamart": "#DC2626",
}

NON_MERCH_PRODUCT_PATTERN = re.compile(
    r"\b(?:scrap|waste|minifix|polythene|carton|housing|discount)\b|damage\s*&\s*scrap",
    re.IGNORECASE,
)

SUMMARY_SECTION_KEYS = [
    "headline_cards",
    "monthly_fy_sales",
    "channel_performance_current",
    "budget_vs_achievement",
    "rto_monthly_current",
    "channel_growth",
    "insights",
]

FISCAL_MONTH_NAMES = [
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
    "January",
    "February",
    "March",
]

FISCAL_MONTH_INDEX = {name: index for index, name in enumerate(FISCAL_MONTH_NAMES)}

FY_SERIES_COLORS = [
    "#7F5539",
    "#4F772D",
    "#0D5C63",
    "#B35C2D",
    "#3C6E71",
    "#8D5524",
]


# ============================================================================
# HELPERS
# ============================================================================

def fmt_inr(amount: float) -> str:
    amount = safe_float(amount)
    absolute_amount = abs(amount)
    if absolute_amount >= 10_000_000:
        return f"\u20b9{amount / 10_000_000:.2f}Cr"
    if absolute_amount >= 100_000:
        return f"\u20b9{amount / 100_000:.2f}L"
    if absolute_amount >= 1_000:
        return f"\u20b9{amount / 1_000:.1f}K"
    return f"\u20b9{amount:,.0f}"


def safe_float(value: Any) -> float:
    try:
        number = float(value)
        return 0.0 if np.isnan(number) else number
    except Exception:
        return 0.0


def safe_int(value: Any) -> int:
    try:
        return int(value)
    except Exception:
        return 0


def clean_text(value: Any, fallback: str = "Unknown") -> str:
    if value is None:
        return fallback
    if isinstance(value, float) and np.isnan(value):
        return fallback
    text = str(value).strip()
    return text if text else fallback


def normalize_order_id(value: Any) -> Any:
    order_id = clean_text(value, "")
    return order_id if order_id else np.nan


def is_non_merch_product(value: Any) -> bool:
    return bool(NON_MERCH_PRODUCT_PATTERN.search(clean_text(value, "")))


def safe_divide(numerator: Any, denominator: Any) -> Optional[float]:
    denominator_value = safe_float(denominator)
    if denominator_value == 0:
        return None
    return safe_float(numerator) / denominator_value


def has_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, float) and np.isnan(value):
        return False
    return value != ""


def fiscal_year_for(date_value: pd.Timestamp) -> str:
    if pd.isna(date_value):
        return "Unknown"
    if date_value.month >= 4:
        return f"FY{date_value.year}-{str(date_value.year + 1)[-2:]}"
    return f"FY{date_value.year - 1}-{str(date_value.year)[-2:]}"


def fiscal_month_name(date_value: pd.Timestamp) -> str:
    if pd.isna(date_value):
        return "Unknown"
    return date_value.strftime("%B")


def format_signed_percentage(value: Optional[float], decimals: int = 1) -> str:
    if value is None:
        return "N/A"
    prefix = "+" if value > 0 else ""
    return f"{prefix}{value:.{decimals}f}%"


def pct(value: Any, decimals: int = 1) -> str:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return "N/A"
    return f"{safe_float(value):.{decimals}f}%"


def fy_label_to_key(value: str) -> str:
    match = re.fullmatch(r"FY(\d{4})-(\d{2})", clean_text(value, ""))
    if not match:
        return clean_text(value, "fy_unknown").lower().replace("-", "_").replace(" ", "_")
    return f"fy_{match.group(1)}_{match.group(2)}"


def fy_key_to_label(value: str) -> str:
    match = re.fullmatch(r"fy_(\d{4})_(\d{2})", clean_text(value, ""))
    if not match:
        return value.replace("_", " ").upper()
    return f"FY {match.group(1)[-2:]}-{match.group(2)}"


def fy_sort_key(value: str) -> Tuple[int, int]:
    label_match = re.fullmatch(r"FY(\d{4})-(\d{2})", clean_text(value, ""))
    if label_match:
        return (int(label_match.group(1)), int(label_match.group(2)))
    key_match = re.fullmatch(r"fy_(\d{4})_(\d{2})", clean_text(value, ""))
    if key_match:
        return (int(key_match.group(1)), int(key_match.group(2)))
    return (0, 0)


def normalize_google_drive_url(url: str) -> str:
    match = re.search(r"/d/([a-zA-Z0-9_-]+)", url)
    if match:
        return f"https://drive.google.com/uc?export=download&id={match.group(1)}"

    match = re.search(r"[?&]id=([a-zA-Z0-9_-]+)", url)
    if match:
        return f"https://drive.google.com/uc?export=download&id={match.group(1)}"

    return url


def normalize_google_sheet_url(url: str) -> str:
    match = re.search(r"/spreadsheets/d/([a-zA-Z0-9_-]+)", url)
    if not match:
        return url

    sheet_id = match.group(1)
    gid_match = re.search(r"[?&]gid=([0-9]+)", url)
    if gid_match:
        return f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx&gid={gid_match.group(1)}"
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx"


def normalize_sharepoint_url(url: str) -> str:
    if "sharepoint.com" not in url and "onedrive.live.com" not in url:
        return url
    if "download=1" in url:
        return url
    if "?" in url:
        return url + "&download=1"
    return url + "?download=1"


def normalize_data_url(url: str) -> str:
    if not url:
        return url
    if "docs.google.com/spreadsheets" in url:
        return normalize_google_sheet_url(url)
    if "drive.google.com" in url:
        return normalize_google_drive_url(url)
    if "sharepoint.com" in url or "onedrive.live.com" in url:
        return normalize_sharepoint_url(url)
    return url


def normalize_search_text(value: Any) -> str:
    text = clean_text(value, "").lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def compact_search_text(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", clean_text(value, "").lower())


def derive_sku_base(value: Any, known_skus: set[str]) -> str:
    sku = clean_text(value, "Unknown SKU")
    if sku == "Unknown SKU":
        return sku

    parts = [part.strip() for part in sku.split("-") if part.strip()]
    if len(parts) <= 2:
        return sku

    candidate = "-".join(parts[:-1])
    return candidate if candidate in known_skus else sku


def derive_sku_extension(value: Any, base_sku: Any) -> str:
    sku = clean_text(value, "")
    base = clean_text(base_sku, "")
    if not sku or not base or sku == base:
        return ""
    prefix = base + "-"
    return sku[len(prefix) :] if sku.startswith(prefix) else ""


def search_rank(value: Any, query: str, exact_bonus: int = 0) -> int:
    value_text = normalize_search_text(value)
    value_compact = compact_search_text(value)
    query_text = normalize_search_text(query)
    query_compact = compact_search_text(query)
    if not value_text and not value_compact:
        return 99
    if query_compact and value_compact == query_compact:
        return 0 + exact_bonus
    if query_text and value_text == query_text:
        return 0 + exact_bonus
    if query_compact and value_compact.startswith(query_compact):
        return 1 + exact_bonus
    if query_text and value_text.startswith(query_text):
        return 1 + exact_bonus
    if query_compact and query_compact in value_compact:
        return 2 + exact_bonus
    if query_text and query_text in value_text:
        return 2 + exact_bonus
    return 50 + exact_bonus


def read_text_cell(sheet: pd.DataFrame, row: int, col: int) -> str:
    if row >= len(sheet.index) or col >= len(sheet.columns):
        return ""
    value = sheet.iat[row, col]
    if pd.isna(value):
        return ""
    return str(value).strip()


def read_number_cell(sheet: pd.DataFrame, row: int, col: int) -> Optional[float]:
    if row >= len(sheet.index) or col >= len(sheet.columns):
        return None
    value = sheet.iat[row, col]
    if pd.isna(value):
        return None
    return safe_float(value)


def read_excel_quietly(*args: Any, **kwargs: Any) -> pd.DataFrame:
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message=r"Print area cannot be set to Defined name: .*",
            category=UserWarning,
        )
        return pd.read_excel(*args, **kwargs)


def build_cache_key(endpoint: str, params: Dict[str, Any], version: str) -> str:
    serialized = json.dumps(params, sort_keys=True, default=str)
    return f"{version}:{endpoint}:{serialized}"


def parse_filters(
    platform: Optional[str] = None,
    category: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    product: Optional[str] = None,
    product_query: Optional[str] = None,
    trend_mode: Optional[str] = None,
) -> Dict[str, Any]:
    filters: Dict[str, Any] = {}
    if platform:
        filters["platform"] = [item.strip() for item in platform.split(",") if item.strip()]
    if category:
        filters["category"] = [item.strip() for item in category.split(",") if item.strip()]
    if start_date:
        filters["start_date"] = start_date
    if end_date:
        filters["end_date"] = end_date
    if product and product.lower() not in {"all", ""}:
        filters["product"] = product.strip()
    if product_query and product_query.strip():
        filters["product_query"] = product_query.strip()
    if trend_mode:
        normalized_trend_mode = trend_mode.strip().lower()
        if normalized_trend_mode in TREND_MODE_OPTIONS:
            filters["trend_mode"] = normalized_trend_mode
    return filters


def parse_summary_sheet(sheet: pd.DataFrame) -> Dict[str, Any]:
    if sheet.empty:
        return {}

    headline_cards: List[Dict[str, str]] = []
    for label_row, value_row in ((5, 6), (8, 9), (11, 12)):
        for column in (1, 3, 5, 7):
            label = read_text_cell(sheet, label_row, column)
            value = read_text_cell(sheet, value_row, column)
            if label and value:
                headline_cards.append({"label": label, "value": value})

    monthly_fy_sales: List[Dict[str, Any]] = []
    for row in range(17, 30):
        month = read_text_cell(sheet, row, 1)
        if not month:
            continue
        monthly_fy_sales.append(
            {
                "month": month,
                "fy_2021_22": read_number_cell(sheet, row, 2),
                "fy_2022_23": read_number_cell(sheet, row, 3),
                "fy_2023_24": read_number_cell(sheet, row, 4),
                "fy_2024_25": read_number_cell(sheet, row, 5),
                "fy_2025_26": read_number_cell(sheet, row, 6),
                "budget_2025_26": read_number_cell(sheet, row, 7),
            }
        )

    channel_performance_current: List[Dict[str, Any]] = []
    for row in range(45, 54):
        channel = read_text_cell(sheet, row, 1)
        if not channel:
            continue
        channel_performance_current.append(
            {
                "channel": channel,
                "gross_sales": read_number_cell(sheet, row, 2),
                "returns": abs(read_number_cell(sheet, row, 3) or 0.0),
                "net_sales": read_number_cell(sheet, row, 4),
                "rto_rate": read_number_cell(sheet, row, 5),
                "revenue_share": read_number_cell(sheet, row, 6),
                "net_qty": read_number_cell(sheet, row, 7),
                "trend": read_text_cell(sheet, row, 8),
            }
        )

    budget_vs_achievement: List[Dict[str, Any]] = []
    for row in range(58, 71):
        month = read_text_cell(sheet, row, 1)
        if not month:
            continue
        budget_vs_achievement.append(
            {
                "month": month,
                "budget": read_number_cell(sheet, row, 2),
                "actual": read_number_cell(sheet, row, 3),
                "variance": read_number_cell(sheet, row, 4),
                "achievement_pct": read_number_cell(sheet, row, 5),
                "yoy_change": read_number_cell(sheet, row, 6),
                "cumulative_budget": read_number_cell(sheet, row, 7),
                "cumulative_actual": read_number_cell(sheet, row, 8),
            }
        )

    rto_monthly_current: List[Dict[str, Any]] = []
    for row in range(104, 115):
        month = read_text_cell(sheet, row, 1)
        if not month:
            continue
        rto_monthly_current.append(
            {
                "month": month,
                "amazon": read_number_cell(sheet, row, 2),
                "flipkart": read_number_cell(sheet, row, 3),
                "pepperfry": read_number_cell(sheet, row, 4),
                "reliance": read_number_cell(sheet, row, 5),
                "shopify": read_number_cell(sheet, row, 6),
                "myntra": read_number_cell(sheet, row, 7),
                "blended": read_number_cell(sheet, row, 8),
            }
        )

    channel_growth: List[Dict[str, Any]] = []
    for row in range(132, 140):
        channel = read_text_cell(sheet, row, 1)
        if not channel:
            continue
        channel_growth.append(
            {
                "channel": channel,
                "net_sales_fy_2024_25": read_number_cell(sheet, row, 2),
                "net_sales_fy_2025_26": read_number_cell(sheet, row, 3),
                "growth_value": read_number_cell(sheet, row, 4),
                "growth_pct": read_number_cell(sheet, row, 5),
                "share_fy_2024_25": read_number_cell(sheet, row, 6),
                "share_fy_2025_26": read_number_cell(sheet, row, 7),
                "share_change": read_number_cell(sheet, row, 8),
            }
        )

    insights: List[Dict[str, str]] = []
    for row in range(118, 128):
        title = read_text_cell(sheet, row, 1)
        body = read_text_cell(sheet, row, 2)
        if title and body:
            insights.append({"title": title, "body": body})

    return {
        "headline_cards": headline_cards,
        "monthly_fy_sales": monthly_fy_sales,
        "channel_performance_current": channel_performance_current,
        "budget_vs_achievement": budget_vs_achievement,
        "rto_monthly_current": rto_monthly_current,
        "channel_growth": channel_growth,
        "insights": insights,
    }


class CacheManager:
    def __init__(self, ttl_seconds: int = 300):
        self.ttl_seconds = ttl_seconds
        self._cache: Dict[str, Tuple[Any, float]] = {}

    def get(self, key: str) -> Optional[Any]:
        cached = self._cache.get(key)
        if not cached:
            return None
        result, created_at = cached
        if time.time() - created_at > self.ttl_seconds:
            del self._cache[key]
            return None
        return result

    def set(self, key: str, result: Any) -> None:
        self._cache[key] = (result, time.time())

    def clear(self) -> None:
        self._cache.clear()


class DataManager:
    def __init__(self) -> None:
        self._df: Optional[pd.DataFrame] = None
        self._loaded_at: Optional[datetime] = None
        self._source: str = "uninitialized"
        self._source_type: str = "none"
        self._summary_sheet: Dict[str, Any] = {}
        self._load_error: Optional[str] = None
        self._data_version: str = "unloaded"
        self._load()

    @property
    def ready(self) -> bool:
        return self._df is not None and not self._df.empty

    @property
    def version(self) -> str:
        return self._data_version

    def health(self) -> Dict[str, Any]:
        unique_orders = 0
        if self.ready:
            unique_orders = int(self._df["order_id"].dropna().nunique())

        return {
            "status": "ok" if self.ready else "error",
            "data_loaded": self.ready,
            "rows": len(self._df) if self.ready else 0,
            "unique_orders": unique_orders,
            "loaded_at": self._loaded_at.isoformat() if self._loaded_at else None,
            "source": self._source,
            "source_type": self._source_type,
            "error": self._load_error,
            "snapshot_available": any(path.exists() for path in self._snapshot_candidates()),
        }

    def filter_options(self) -> Dict[str, Any]:
        if not self.ready:
            return {}

        date_series = self._df["order_date"].dropna()
        out = {
            "platforms": [
                {
                    "value": platform,
                    "label": PLATFORM_DISPLAY_NAMES.get(platform, platform),
                    "color": PLATFORM_COLORS.get(platform, "#64748B"),
                }
                for platform in sorted(self._df["platform_raw"].dropna().unique().tolist())
            ],
            "categories": sorted(self._df["category"].dropna().unique().tolist()),
            "summary_sheet_available": bool(self.summary_sheet()),
            "data_source": {
                "source": self._source,
                "source_type": self._source_type,
            },
            "trend_modes": [
                {"value": mode, "label": TREND_MODE_LABELS[mode]}
                for mode in TREND_MODE_OPTIONS
            ],
            "default_trend_mode": "auto",
        }
        if not date_series.empty:
            out["date_range"] = {
                "min": date_series.min().strftime("%Y-%m-%d"),
                "max": date_series.max().strftime("%Y-%m-%d"),
            }
        return out

    def summary_sheet(self) -> Dict[str, Any]:
        source_summary = self._summary_sheet if isinstance(self._summary_sheet, dict) else {}
        if not self.ready and not source_summary:
            return {}

        fallback = self._computed_summary_sheet(self._df if self.ready else pd.DataFrame(columns=SNAPSHOT_COLUMNS))
        merged: Dict[str, Any] = {}
        for key in SUMMARY_SECTION_KEYS:
            source_value = source_summary.get(key)
            merged[key] = source_value if source_value else fallback.get(key, [])

        mode = "workbook" if any(source_summary.get(key) for key in SUMMARY_SECTION_KEYS) else "computed"
        merged["meta"] = self._summary_sheet_metadata(merged, mode)
        return merged

    def summary_sheet_for(self, filters: Dict[str, Any], df: Optional[pd.DataFrame] = None) -> Dict[str, Any]:
        if not self.ready:
            return {}
        scoped_df = df if df is not None else self.apply_filters(filters)
        if self._filters_change_scope(filters):
            summary = self._computed_summary_sheet(scoped_df)
            summary["meta"] = self._summary_sheet_metadata(summary, "computed")
            return summary
        return self.summary_sheet()

    def _filters_change_scope(self, filters: Dict[str, Any]) -> bool:
        if not self.ready:
            return False
        for key in ("platform", "category", "product", "product_query"):
            if clean_text(filters.get(key), ""):
                return True
        date_series = self._df["order_date"].dropna()
        if date_series.empty:
            return False
        full_start = date_series.min().strftime("%Y-%m-%d")
        full_end = date_series.max().strftime("%Y-%m-%d")
        start_date = clean_text(filters.get("start_date"), "")
        end_date = clean_text(filters.get("end_date"), "")
        if start_date and start_date != full_start:
            return True
        if end_date and end_date != full_end:
            return True
        return False

    def _summary_sheet_metadata(self, summary: Dict[str, Any], mode: str) -> Dict[str, Any]:
        fy_rows = [row for row in summary.get("monthly_fy_sales", []) if row.get("month") != "TOTAL"]
        sample_row = fy_rows[0] if fy_rows else (summary.get("monthly_fy_sales", [{}])[:1] or [{}])[0]
        fy_keys = sorted([key for key in sample_row.keys() if key.startswith("fy_")], key=fy_sort_key)
        fy_series = [
            {
                "key": key,
                "label": fy_key_to_label(key),
                "color": FY_SERIES_COLORS[index % len(FY_SERIES_COLORS)],
            }
            for index, key in enumerate(fy_keys)
        ]
        budget_available = any(
            has_value(row.get("budget"))
            for row in summary.get("budget_vs_achievement", [])
            if row.get("month") != "TOTAL"
        )
        dated_rows = self._df["order_date"].dropna() if self.ready else pd.Series(dtype="datetime64[ns]")
        date_range = None
        if not dated_rows.empty:
            date_range = {
                "min": dated_rows.min().strftime("%Y-%m-%d"),
                "max": dated_rows.max().strftime("%Y-%m-%d"),
            }

        if mode == "workbook":
            source_note = (
                "Using the workbook's Sales Analytics Dashboard sheet for strategic cards, FY rollups, "
                "and budget tracking."
            )
        else:
            source_note = (
                "Derived from the loaded sales rows because this source does not include the Sales Analytics "
                "Dashboard sheet. FY views reflect only the loaded date range, and budget targets are unavailable."
            )

        return {
            "mode": mode,
            "budget_available": budget_available,
            "fy_series": fy_series,
            "date_range": date_range,
            "source_note": source_note,
        }

    def _computed_summary_sheet(self, df: Optional[pd.DataFrame]) -> Dict[str, Any]:
        empty_payload = {key: [] for key in SUMMARY_SECTION_KEYS}
        if df is None or df.empty:
            return empty_payload

        dated_df = df[df["order_date"].notna() & df["fy"].astype(str).str.startswith("FY")].copy()
        scope_df = dated_df if not dated_df.empty else df.copy()
        available_fys = sorted(dated_df["fy"].dropna().unique().tolist(), key=fy_sort_key)
        current_fy = available_fys[-1] if available_fys else None
        previous_fy = available_fys[-2] if len(available_fys) > 1 else None
        current_df = dated_df[dated_df["fy"] == current_fy].copy() if current_fy else scope_df.copy()
        previous_df = dated_df[dated_df["fy"] == previous_fy].copy() if previous_fy else pd.DataFrame(columns=df.columns)

        return {
            "headline_cards": self._computed_headline_cards(scope_df, current_df, previous_df, current_fy, previous_fy),
            "monthly_fy_sales": self._computed_monthly_fy_sales(dated_df, available_fys),
            "channel_performance_current": self._computed_channel_performance_current(current_df, previous_df),
            "budget_vs_achievement": self._computed_budget_vs_achievement(current_df, previous_df),
            "rto_monthly_current": self._computed_rto_monthly_current(current_df),
            "channel_growth": self._computed_channel_growth(current_df, previous_df, current_fy, previous_fy),
            "insights": self._computed_strategic_insights(scope_df, current_df, previous_df, current_fy, previous_fy),
        }

    def _computed_headline_cards(
        self,
        scope_df: pd.DataFrame,
        current_df: pd.DataFrame,
        previous_df: pd.DataFrame,
        current_fy: Optional[str],
        previous_fy: Optional[str],
    ) -> List[Dict[str, str]]:
        scope = current_df if not current_df.empty else scope_df
        kpis = self.kpis(scope)
        platforms = self.platform_data(scope)
        categories = self.category_data(scope)
        trend = self.returns_trend(scope)
        scope_label = fy_key_to_label(fy_label_to_key(current_fy)) if current_fy else "Loaded Range"
        top_platform = platforms[0] if platforms else None
        eligible_platforms = [row for row in platforms if row["orders"] >= 25] or platforms
        highest_rto = max(eligible_platforms, key=lambda row: row["return_rate_value"]) if eligible_platforms else None
        top_category = categories[0] if categories else None
        best_month = max(trend, key=lambda row: row["net_revenue"]) if trend else None
        dated_rows = scope["order_date"].dropna()
        period_label = "N/A"
        if not dated_rows.empty:
            period_label = f"{dated_rows.min():%d %b %Y} to {dated_rows.max():%d %b %Y}"

        cards = [
            {"label": f"Total revenue ({scope_label})", "value": kpis["gross_sales_formatted"]},
            {"label": f"Returns value ({scope_label})", "value": kpis["return_value_formatted"]},
            {"label": f"Net revenue after returns ({scope_label})", "value": kpis["net_revenue_formatted"]},
            {"label": f"Unique orders ({scope_label})", "value": f"{kpis['unique_orders']:,}"},
            {"label": "Average order value", "value": kpis["aov_formatted"]},
            {"label": "Average selling price", "value": kpis["asp_formatted"]},
            {"label": "Return rate by value", "value": f"{kpis['return_rate_value']:.1f}%"},
            {"label": "Return rate by quantity", "value": f"{kpis['return_rate_qty']:.1f}%"},
            {
                "label": "Top revenue channel",
                "value": (
                    f"{top_platform['platform_label']} ({top_platform['share']:.1f}%)"
                    if top_platform
                    else "N/A"
                ),
            },
            {
                "label": "Highest value return risk",
                "value": (
                    f"{highest_rto['platform_label']} ({highest_rto['return_rate_value']:.1f}%)"
                    if highest_rto
                    else "N/A"
                ),
            },
            {
                "label": "Top category",
                "value": (
                    f"{top_category['category']} ({fmt_inr(top_category['net_revenue'])})"
                    if top_category
                    else "N/A"
                ),
            },
            {
                "label": "Best revenue month",
                "value": (
                    f"{best_month['month']} ({fmt_inr(best_month['net_revenue'])})"
                    if best_month
                    else "N/A"
                ),
            },
            {"label": "Loaded period", "value": period_label},
        ]

        if not previous_df.empty and previous_fy:
            previous_kpis = self.kpis(previous_df)
            previous_label = fy_key_to_label(fy_label_to_key(previous_fy))
            net_growth = safe_divide(
                kpis["net_revenue"] - previous_kpis["net_revenue"],
                previous_kpis["net_revenue"],
            )
            order_growth = safe_divide(
                kpis["unique_orders"] - previous_kpis["unique_orders"],
                previous_kpis["unique_orders"],
            )
            cards.extend(
                [
                    {
                        "label": f"Net growth ({scope_label} vs {previous_label})",
                        "value": format_signed_percentage(net_growth * 100 if net_growth is not None else None),
                    },
                    {
                        "label": f"Order growth ({scope_label} vs {previous_label})",
                        "value": format_signed_percentage(order_growth * 100 if order_growth is not None else None),
                    },
                ]
            )

        return cards

    def _computed_monthly_fy_sales(self, df: pd.DataFrame, available_fys: List[str]) -> List[Dict[str, Any]]:
        if df.empty or not available_fys:
            return []

        monthly_df = df.copy()
        monthly_df["fiscal_month_name"] = monthly_df["order_date"].apply(fiscal_month_name)
        grouped = (
            monthly_df.groupby(["fiscal_month_name", "fy"], observed=True)["net_revenue"]
            .sum()
            .to_dict()
        )
        totals = monthly_df.groupby("fy", observed=True)["net_revenue"].sum().to_dict()
        rows: List[Dict[str, Any]] = []

        for month in FISCAL_MONTH_NAMES:
            row: Dict[str, Any] = {"month": month}
            for fy in available_fys:
                row[fy_label_to_key(fy)] = safe_float(grouped.get((month, fy), 0.0))
            rows.append(row)

        total_row: Dict[str, Any] = {"month": "TOTAL"}
        for fy in available_fys:
            total_row[fy_label_to_key(fy)] = safe_float(totals.get(fy, 0.0))
        rows.append(total_row)
        return rows

    def _computed_channel_growth(
        self,
        current_df: pd.DataFrame,
        previous_df: pd.DataFrame,
        current_fy: Optional[str],
        previous_fy: Optional[str],
    ) -> List[Dict[str, Any]]:
        if current_df.empty:
            return []

        current_rows = {row["platform_label"]: row for row in self.platform_data(current_df)}
        previous_rows = {row["platform_label"]: row for row in self.platform_data(previous_df)}
        channel_labels = sorted(
            set(current_rows) | set(previous_rows),
            key=lambda label: current_rows.get(label, {"net_revenue": 0.0})["net_revenue"],
            reverse=True,
        )
        current_key = fy_label_to_key(current_fy) if current_fy else "fy_current"
        previous_key = fy_label_to_key(previous_fy) if previous_fy else "fy_previous"
        rows: List[Dict[str, Any]] = []

        for label in channel_labels:
            current_row = current_rows.get(label, {})
            previous_row = previous_rows.get(label, {})
            current_net = safe_float(current_row.get("net_revenue", 0.0))
            previous_net = safe_float(previous_row.get("net_revenue", 0.0))
            current_share = safe_float(current_row.get("share", 0.0)) / 100
            previous_share = safe_float(previous_row.get("share", 0.0)) / 100
            growth_ratio = safe_divide(current_net - previous_net, previous_net)
            rows.append(
                {
                    "channel": label,
                    f"net_sales_{previous_key}": previous_net,
                    f"net_sales_{current_key}": current_net,
                    "growth_value": current_net - previous_net,
                    "growth_pct": growth_ratio,
                    f"share_{previous_key}": previous_share,
                    f"share_{current_key}": current_share,
                    "share_change": current_share - previous_share,
                }
            )

        return rows

    def _computed_budget_vs_achievement(
        self,
        current_df: pd.DataFrame,
        previous_df: pd.DataFrame,
    ) -> List[Dict[str, Any]]:
        if current_df.empty:
            return []

        current_grouped = (
            current_df.assign(fiscal_month_name=current_df["order_date"].apply(fiscal_month_name))
            .groupby("fiscal_month_name", observed=True)["net_revenue"]
            .sum()
            .to_dict()
        )
        previous_grouped = (
            previous_df.assign(fiscal_month_name=previous_df["order_date"].apply(fiscal_month_name))
            .groupby("fiscal_month_name", observed=True)["net_revenue"]
            .sum()
            .to_dict()
            if not previous_df.empty
            else {}
        )

        running_actual = 0.0
        rows: List[Dict[str, Any]] = []
        for month in FISCAL_MONTH_NAMES:
            actual = safe_float(current_grouped.get(month, 0.0))
            previous_actual = safe_float(previous_grouped.get(month, 0.0))
            running_actual += actual
            yoy_change = safe_divide(actual - previous_actual, previous_actual)
            rows.append(
                {
                    "month": month,
                    "budget": None,
                    "actual": actual,
                    "variance": None,
                    "achievement_pct": None,
                    "yoy_change": yoy_change,
                    "cumulative_budget": None,
                    "cumulative_actual": running_actual,
                }
            )

        previous_total = safe_float(previous_df["net_revenue"].sum()) if not previous_df.empty else 0.0
        current_total = safe_float(current_df["net_revenue"].sum())
        rows.append(
            {
                "month": "TOTAL",
                "budget": None,
                "actual": current_total,
                "variance": None,
                "achievement_pct": None,
                "yoy_change": safe_divide(current_total - previous_total, previous_total),
                "cumulative_budget": None,
                "cumulative_actual": current_total,
            }
        )
        return rows

    def _computed_channel_performance_current(
        self,
        current_df: pd.DataFrame,
        previous_df: pd.DataFrame,
    ) -> List[Dict[str, Any]]:
        if current_df.empty:
            return []

        current_rows = self.platform_data(current_df)
        previous_rows = {row["platform_label"]: row for row in self.platform_data(previous_df)}
        rows: List[Dict[str, Any]] = []

        for row in current_rows:
            previous_net = safe_float(previous_rows.get(row["platform_label"], {}).get("net_revenue", 0.0))
            growth_ratio = safe_divide(row["net_revenue"] - previous_net, previous_net)
            if previous_net == 0 and row["net_revenue"] > 0:
                trend = "New"
            elif growth_ratio is None:
                trend = "Stable"
            elif growth_ratio > 0.03:
                trend = "Growing"
            elif growth_ratio < -0.03:
                trend = "Declining"
            else:
                trend = "Stable"

            rows.append(
                {
                    "channel": row["platform_label"],
                    "gross_sales": row["gross_sales"],
                    "returns": row["returns"],
                    "net_sales": row["net_revenue"],
                    "rto_rate": row["return_rate_value"] / 100,
                    "revenue_share": row["share"] / 100,
                    "net_qty": row["net_qty"],
                    "trend": trend,
                }
            )

        rows.append(
            {
                "channel": "TOTAL",
                "gross_sales": safe_float(current_df["gross_sales"].sum()),
                "returns": safe_float(current_df["return_value"].sum()),
                "net_sales": safe_float(current_df["net_revenue"].sum()),
                "rto_rate": safe_divide(
                    safe_float(current_df["return_value"].sum()),
                    safe_float(current_df["gross_sales"].sum()),
                )
                or 0.0,
                "revenue_share": 1.0,
                "net_qty": safe_float(current_df["net_qty"].sum()),
                "trend": "Aggregate",
            }
        )
        return rows

    def _computed_rto_monthly_current(self, current_df: pd.DataFrame) -> List[Dict[str, Any]]:
        if current_df.empty:
            return []

        focus_channels = {
            "Amazon": "amazon",
            "Flipkart": "flipkart",
            "Pepperfry": "pepperfry",
            "Reliance": "reliance",
            "Shopify": "shopify",
            "Myntra": "myntra",
        }
        monthly_df = current_df.copy()
        monthly_df["fiscal_month_name"] = monthly_df["order_date"].apply(fiscal_month_name)
        grouped = (
            monthly_df.groupby(["fiscal_month_name", "platform_label"], observed=True)
            .agg(
                gross_sales=("gross_sales", "sum"),
                returns=("return_value", "sum"),
            )
            .to_dict("index")
        )
        blended = (
            monthly_df.groupby("fiscal_month_name", observed=True)
            .agg(
                gross_sales=("gross_sales", "sum"),
                returns=("return_value", "sum"),
            )
            .to_dict("index")
        )
        rows: List[Dict[str, Any]] = []

        for month in FISCAL_MONTH_NAMES:
            row: Dict[str, Any] = {"month": month}
            for label, key in focus_channels.items():
                metrics = grouped.get((month, label), {})
                row[key] = safe_divide(metrics.get("returns", 0.0), metrics.get("gross_sales", 0.0)) or 0.0
            blended_metrics = blended.get(month, {})
            row["blended"] = (
                safe_divide(blended_metrics.get("returns", 0.0), blended_metrics.get("gross_sales", 0.0)) or 0.0
            )
            rows.append(row)

        return rows

    def _computed_strategic_insights(
        self,
        scope_df: pd.DataFrame,
        current_df: pd.DataFrame,
        previous_df: pd.DataFrame,
        current_fy: Optional[str],
        previous_fy: Optional[str],
    ) -> List[Dict[str, str]]:
        scope = current_df if not current_df.empty else scope_df
        if scope.empty:
            return []

        kpis = self.kpis(scope)
        dated_rows = scope["order_date"].dropna()
        coverage_metric = "N/A"
        if not dated_rows.empty:
            coverage_metric = f"{dated_rows.min():%d %b %Y} to {dated_rows.max():%d %b %Y}"

        insights: List[Dict[str, str]] = [
            {
                "title": "Derived FY layer",
                "metric": fy_key_to_label(fy_label_to_key(current_fy)) if current_fy else "Loaded range",
                "body": (
                    "This source does not include the workbook summary sheet, so the strategic cards and FY lens "
                    "below are computed directly from the loaded order rows."
                ),
                "note": (
                    "Budget values remain unavailable until the source includes the Sales Analytics Dashboard sheet."
                ),
            },
            {
                "title": "Coverage in view",
                "metric": coverage_metric,
                "body": (
                    f"The derived FY view currently spans {len(scope):,} normalized rows and "
                    f"{kpis['unique_orders']:,} unique orders."
                ),
            },
        ]

        if not previous_df.empty and previous_fy:
            previous_kpis = self.kpis(previous_df)
            previous_label = fy_key_to_label(fy_label_to_key(previous_fy))
            growth_ratio = safe_divide(
                kpis["net_revenue"] - previous_kpis["net_revenue"],
                previous_kpis["net_revenue"],
            )
            insights.append(
                {
                    "title": "Net movement vs prior FY",
                    "metric": format_signed_percentage(growth_ratio * 100 if growth_ratio is not None else None),
                    "body": (
                        f"Latest loaded FY net revenue is {kpis['net_revenue_formatted']} against "
                        f"{previous_kpis['net_revenue_formatted']} in {previous_label}."
                    ),
                }
            )

        insights.extend(self.dynamic_insights(scope)[:6])
        return insights[:8]

    def load_from_url(self, url: str) -> Dict[str, Any]:
        if not self._load(preferred_url=url, prefer_snapshot=False):
            raise ValueError(self._load_error or "Workbook reload failed.")
        return self.health()

    def load_order_hub_source(self) -> Dict[str, Any]:
        if not ORDER_HUB_BASE_URL:
            raise ValueError("OrderHub SSOT source is not configured for this deployment.")
        if not self._load(preferred_source_type="order_hub", prefer_snapshot=False):
            raise ValueError(self._load_error or "OrderHub SSOT reload failed.")
        return self.health()

    def refresh_current_source(self) -> Dict[str, Any]:
        if not self._load(prefer_snapshot=False):
            raise ValueError(self._load_error or "Current source refresh failed.")
        return self.health()

    def reset_to_default_source(self) -> Dict[str, Any]:
        if not self._load(prefer_snapshot=False, allow_current_url_fallback=False):
            raise ValueError(self._load_error or "Default source reload failed.")
        return self.health()

    def _current_state(self) -> Dict[str, Any]:
        return {
            "df": self._df,
            "loaded_at": self._loaded_at,
            "source": self._source,
            "source_type": self._source_type,
            "summary_sheet": self._summary_sheet,
            "data_version": self._data_version,
        }

    def _restore_state(self, state: Dict[str, Any]) -> None:
        self._df = state["df"]
        self._loaded_at = state["loaded_at"]
        self._source = state["source"]
        self._source_type = state["source_type"]
        self._summary_sheet = state["summary_sheet"]
        self._data_version = state["data_version"]

    def _load(
        self,
        preferred_url: Optional[str] = None,
        prefer_snapshot: bool = True,
        preferred_source_type: Optional[str] = None,
        allow_current_url_fallback: bool = True,
    ) -> bool:
        self._load_error = None
        previous_state = self._current_state()

        if prefer_snapshot and preferred_url is None and self._load_snapshot():
            log.info("Loaded dashboard data from snapshot.")
            return True

        loaders: List[Tuple[str, str, Any]] = []
        if preferred_url:
            loaders.append(("url", preferred_url, self._load_remote_excel))
        elif preferred_source_type == "order_hub":
            if ORDER_HUB_BASE_URL:
                loaders.append(("order_hub", ORDER_HUB_BASE_URL, self._load_order_hub_snapshot))
            else:
                self._load_error = "OrderHub SSOT source is not configured for this deployment."
                return False
        else:
            if ORDER_HUB_BASE_URL:
                loaders.append(("order_hub", ORDER_HUB_BASE_URL, self._load_order_hub_snapshot))

            if DATA_URL:
                loaders.append(("url", DATA_URL, self._load_remote_excel))
            elif allow_current_url_fallback and self._source_type == "url" and str(self._source).startswith(("http://", "https://")):
                loaders.append(("url", self._source, self._load_remote_excel))

            if GITHUB_TOKEN and GITHUB_REPO:
                loaders.append(("github", f"{GITHUB_REPO}/{DATA_FILE}", self._load_github))

            loaders.append(("local", DATA_FILE, self._load_local))

        last_error = "No usable data source found for SalesTrends."
        for source_type, source_value, loader in loaders:
            try:
                raw_df, summary_sheet = loader(source_value)
                processed = self._process_dataframe(raw_df)
                self._set_loaded_state(processed, source_value, source_type, summary_sheet)
                self._write_snapshot()
                return True
            except Exception as exc:
                last_error = str(exc)
                self._load_error = last_error
                log.warning("Failed to load source %s (%s): %s", source_type, source_value, exc)

        if previous_state["df"] is not None:
            self._restore_state(previous_state)
            self._load_error = last_error
            log.error("Reload failed; preserving the last healthy dataset: %s", last_error)
            return False

        self._df = None
        self._source = "unavailable"
        self._source_type = "none"
        self._loaded_at = None
        self._data_version = "unloaded"
        self._load_error = last_error
        log.error("No usable data source found for SalesTrends: %s", last_error)
        return False

    def _set_loaded_state(
        self,
        df: pd.DataFrame,
        source: str,
        source_type: str,
        summary_sheet: Optional[Dict[str, Any]],
    ) -> None:
        self._df = self._ensure_search_dimensions(df.copy())
        self._source = source
        self._source_type = source_type
        self._summary_sheet = summary_sheet or {}
        self._loaded_at = datetime.now(timezone.utc)
        self._data_version = self._loaded_at.strftime("%Y%m%d%H%M%S")
        self._load_error = None
        log.info("Loaded %s normalized rows from %s.", len(self._df), source)

    def _ensure_search_dimensions(self, df: pd.DataFrame) -> pd.DataFrame:
        if set(SEARCH_DIMENSION_COLUMNS).issubset(df.columns):
            return df

        enriched = df.copy()
        for column in ("product", "sku"):
            if column not in enriched.columns:
                enriched[column] = ""

        known_skus = {
            clean_text(value, "Unknown SKU")
            for value in enriched["sku"].dropna().astype(str).tolist()
        }
        sku_base = enriched["sku"].apply(lambda value: derive_sku_base(value, known_skus))
        enriched["sku_base"] = sku_base
        enriched["sku_extension"] = [
            derive_sku_extension(sku, base) for sku, base in zip(enriched["sku"], sku_base)
        ]
        enriched["product_search_text"] = enriched["product"].apply(normalize_search_text)
        enriched["product_search_compact"] = enriched["product"].apply(compact_search_text)
        enriched["sku_search_text"] = enriched["sku"].apply(normalize_search_text)
        enriched["sku_search_compact"] = enriched["sku"].apply(compact_search_text)
        enriched["sku_base_search_text"] = sku_base.apply(normalize_search_text)
        enriched["sku_base_search_compact"] = sku_base.apply(compact_search_text)
        return enriched

    def _product_query_mask(self, df: pd.DataFrame, query: str) -> pd.Series:
        search_df = self._ensure_search_dimensions(df)
        query_text = normalize_search_text(query)
        query_compact = compact_search_text(query)
        mask = pd.Series(False, index=search_df.index)

        if query_text:
            mask = mask | search_df["product_search_text"].str.contains(query_text, regex=False, na=False)
            mask = mask | search_df["sku_search_text"].str.contains(query_text, regex=False, na=False)
            mask = mask | search_df["sku_base_search_text"].str.contains(query_text, regex=False, na=False)
        if query_compact:
            mask = mask | search_df["product_search_compact"].str.contains(query_compact, regex=False, na=False)
            mask = mask | search_df["sku_search_compact"].str.contains(query_compact, regex=False, na=False)
            mask = mask | search_df["sku_base_search_compact"].str.contains(query_compact, regex=False, na=False)

        return mask

    def _snapshot_candidates(self) -> List[Path]:
        primary = Path(SNAPSHOT_FILE)
        temp_snapshot = Path(tempfile.gettempdir()) / "salestrends_data_snapshot.csv.gz"
        return [primary, temp_snapshot]

    def _meta_candidates(self) -> List[Path]:
        primary = Path(SNAPSHOT_META_FILE)
        temp_meta = Path(tempfile.gettempdir()) / "salestrends_snapshot_meta.json"
        return [primary, temp_meta]

    def _load_snapshot(self) -> bool:
        for snapshot_path, meta_path in zip(self._snapshot_candidates(), self._meta_candidates()):
            if not snapshot_path.exists() or not meta_path.exists():
                continue

            try:
                df = pd.read_csv(
                    snapshot_path,
                    compression="gzip",
                    parse_dates=["order_date"],
                    keep_default_na=False,
                )
                for numeric_column in (
                    "sale_qty",
                    "return_qty_signed",
                    "return_qty",
                    "gross_sales",
                    "return_value_signed",
                    "return_value",
                    "net_qty",
                    "net_revenue",
                    "tax",
                ):
                    if numeric_column in df.columns:
                        df[numeric_column] = pd.to_numeric(df[numeric_column], errors="coerce").fillna(0.0)
                for column in SNAPSHOT_COLUMNS:
                    if column not in df.columns:
                        df[column] = np.nan
                if "order_id" in df.columns:
                    df["order_id"] = df["order_id"].replace("", np.nan)

                meta_payload = json.loads(meta_path.read_text(encoding="utf-8"))
                source = meta_payload.get("source", snapshot_path.name)
                source_type = meta_payload.get("source_type", "snapshot")
                summary_data = meta_payload.get("summary_sheet", {})
                self._set_loaded_state(df[SNAPSHOT_COLUMNS], source, source_type, summary_data)
                return True
            except Exception as exc:
                log.warning("Failed to load snapshot %s: %s", snapshot_path, exc)
                self._load_error = str(exc)

        return False

    def _write_snapshot(self) -> None:
        if not self.ready:
            return

        meta_payload = {
            "source": self._source,
            "source_type": self._source_type,
            "loaded_at": self._loaded_at.isoformat() if self._loaded_at else None,
            "summary_sheet": self._summary_sheet,
        }

        for snapshot_path, meta_path in zip(self._snapshot_candidates(), self._meta_candidates()):
            try:
                snapshot_path.parent.mkdir(parents=True, exist_ok=True)
                meta_path.parent.mkdir(parents=True, exist_ok=True)
                self._df[SNAPSHOT_COLUMNS].to_csv(snapshot_path, index=False, compression="gzip")
                meta_path.write_text(json.dumps(meta_payload), encoding="utf-8")
                log.info("Wrote snapshot to %s", snapshot_path)
                return
            except Exception as exc:
                log.warning("Could not write snapshot to %s: %s", snapshot_path, exc)

    def _load_remote_excel(self, url: str) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        normalized_url = normalize_data_url(url)
        headers = {"User-Agent": "Bluewud-SalesTrends/1.0"}
        response = http_requests.get(normalized_url, headers=headers, timeout=180, allow_redirects=True)
        response.raise_for_status()

        content_type = response.headers.get("content-type", "").lower()
        if "text/html" in content_type and "application/vnd.openxmlformats" not in content_type:
            raise ValueError("URL resolved to HTML instead of an Excel file.")

        content = response.content
        return self._read_excel_bytes(content), self._read_summary_bytes(content)

    def _load_order_hub_snapshot(self, base_url: str) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        url = f"{base_url.rstrip('/')}{ORDER_HUB_ANALYTICS_PATH}"
        headers = {"User-Agent": "Bluewud-SalesTrends/1.0"}
        response = http_requests.get(url, headers=headers, timeout=180, allow_redirects=True)
        response.raise_for_status()

        content_type = response.headers.get("content-type", "").lower()
        if "text/html" in content_type:
            raise ValueError("OrderHub snapshot endpoint returned HTML instead of CSV.")

        df = pd.read_csv(io.BytesIO(response.content), parse_dates=["order_date"], keep_default_na=False)
        return self._normalize_order_hub_snapshot(df), {}

    def _normalize_order_hub_snapshot(self, df: pd.DataFrame) -> pd.DataFrame:
        frame = df.copy()
        frame.columns = [str(column).strip() for column in frame.columns]

        if frame.empty:
            raise ValueError("OrderHub analytics snapshot is empty.")

        missing = [column for column in ORDER_HUB_REQUIRED_COLUMNS if column not in frame.columns]
        if missing:
            raise ValueError(
                "OrderHub analytics snapshot is missing required columns: " + ", ".join(sorted(missing))
            )

        normalized = pd.DataFrame()
        normalized["order_date"] = pd.to_datetime(frame["order_date"], errors="coerce")
        normalized["platform_raw"] = frame["platform_raw"].apply(lambda value: clean_text(value, "Unknown Platform"))
        normalized["platform_label"] = normalized["platform_raw"].map(
            lambda value: PLATFORM_DISPLAY_NAMES.get(value, value)
        )
        normalized["category"] = frame["category"].apply(lambda value: clean_text(value, "Unknown Category"))
        normalized["product"] = frame["product"].apply(lambda value: clean_text(value, "Unknown Product"))
        normalized["sku"] = frame["sku"].apply(lambda value: clean_text(value, "Unknown SKU"))
        normalized["sale_qty"] = pd.to_numeric(frame["sale_qty"], errors="coerce").fillna(0.0)
        normalized["return_qty_signed"] = pd.to_numeric(frame["return_qty_signed"], errors="coerce").fillna(0.0)
        normalized["gross_sales"] = pd.to_numeric(frame["gross_sales"], errors="coerce").fillna(0.0)
        normalized["return_value_signed"] = pd.to_numeric(frame["return_value_signed"], errors="coerce").fillna(0.0)
        normalized["tax"] = pd.to_numeric(frame["tax"], errors="coerce").fillna(0.0)

        if (normalized["return_qty_signed"] > 0).any() or (normalized["return_value_signed"] > 0).any():
            raise ValueError(
                "OrderHub analytics snapshot must provide signed return_qty_signed and "
                "return_value_signed values (zero or negative)."
            )

        normalized["return_qty"] = normalized["return_qty_signed"].abs()
        normalized["return_value"] = normalized["return_value_signed"].abs()
        normalized["net_qty"] = normalized["sale_qty"] + normalized["return_qty_signed"]
        normalized["net_revenue"] = normalized["gross_sales"] + normalized["return_value_signed"]
        normalized["order_id"] = frame["order_id"].apply(normalize_order_id)
        normalized["return_reason"] = frame["return_reason"].apply(lambda value: clean_text(value, "Unspecified"))
        normalized["return_validity"] = frame["return_validity"].apply(lambda value: clean_text(value, "Unknown"))
        normalized["fy"] = normalized["order_date"].apply(fiscal_year_for)
        normalized["month"] = normalized["order_date"].dt.to_period("M").astype(str)
        normalized["weekday"] = normalized["order_date"].dt.day_name().fillna("Unknown")
        return normalized[SNAPSHOT_COLUMNS]

    def _load_github(self, _: str) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        url = f"https://raw.githubusercontent.com/{GITHUB_REPO}/main/{DATA_FILE}"
        headers = {"Authorization": f"token {GITHUB_TOKEN}"}

        retry_delay = 5
        for attempt in range(3):
            response = http_requests.get(url, headers=headers, timeout=180)
            if response.status_code == 200:
                content = response.content
                return self._read_excel_bytes(content), self._read_summary_bytes(content)
            if response.status_code == 429:
                time.sleep(retry_delay)
                retry_delay *= 2
                continue
            response.raise_for_status()

        raise ValueError("GitHub data load exhausted retries.")

    def _load_local(self, file_name: str) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        candidates = [
            Path(file_name),
            BASE_DIR / file_name,
            BASE_DIR.parent.parent / file_name,
        ]

        for candidate in candidates:
            if not candidate.exists():
                continue
            raw_df = read_excel_quietly(candidate, sheet_name=SHEET_NAME)
            summary_sheet = {}
            try:
                summary_sheet = parse_summary_sheet(
                    read_excel_quietly(candidate, sheet_name=SUMMARY_SHEET_NAME, header=None)
                )
            except Exception as exc:
                log.info("Summary sheet unavailable for %s: %s", candidate, exc)
            return raw_df, summary_sheet

        raise FileNotFoundError(f"Could not find local workbook {file_name}")

    def _read_excel_bytes(self, content: bytes) -> pd.DataFrame:
        return read_excel_quietly(io.BytesIO(content), sheet_name=SHEET_NAME)

    def _read_summary_bytes(self, content: bytes) -> Dict[str, Any]:
        try:
            sheet = read_excel_quietly(io.BytesIO(content), sheet_name=SUMMARY_SHEET_NAME, header=None)
            return parse_summary_sheet(sheet)
        except Exception as exc:
            log.info("Summary sheet parse skipped: %s", exc)
            return {}

    def _process_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        frame = df.copy()
        frame.columns = [str(column).strip() for column in frame.columns]

        for required in RAW_REQUIRED_COLUMNS:
            if required not in frame.columns:
                frame[required] = np.nan

        frame["Final Order date"] = pd.to_datetime(frame["Final Order date"], errors="coerce")

        for column in NUMERIC_COLUMNS:
            frame[column] = pd.to_numeric(frame[column], errors="coerce").fillna(0.0)

        normalized = pd.DataFrame()
        normalized["order_date"] = frame["Final Order date"]
        normalized["platform_raw"] = frame["Main Parties"].apply(lambda value: clean_text(value, "Unknown Platform"))
        normalized["platform_label"] = normalized["platform_raw"].map(
            lambda value: PLATFORM_DISPLAY_NAMES.get(value, value)
        )
        normalized["category"] = frame["Group Name"].apply(lambda value: clean_text(value, "Unknown Category"))
        normalized["product"] = frame["Item Desc"].apply(lambda value: clean_text(value, "Unknown Product"))
        normalized["sku"] = frame["Alias"].apply(lambda value: clean_text(value, "Unknown SKU"))
        normalized["sale_qty"] = frame["Sale (Qty.)"]
        normalized["return_qty_signed"] = frame["Sale Return (Qty.)"]
        normalized["return_qty"] = frame["Sale Return (Qty.)"].abs()
        normalized["gross_sales"] = frame["Sale (Amt.)"]
        normalized["return_value_signed"] = frame["Sale Return (Amt.)"]
        normalized["return_value"] = frame["Sale Return (Amt.)"].abs()
        normalized["net_qty"] = frame["Sale (Qty.)"] + frame["Sale Return (Qty.)"]
        normalized["net_revenue"] = frame["Sale (Amt.)"] + frame["Sale Return (Amt.)"]
        normalized["tax"] = frame["Tax Value"]
        normalized["order_id"] = frame["Order ID"].apply(normalize_order_id)
        normalized["return_reason"] = frame["Return Type"].apply(lambda value: clean_text(value, "Unspecified"))
        normalized["return_validity"] = frame["Valid/Invalid"].apply(lambda value: clean_text(value, "Unknown"))
        normalized["fy"] = normalized["order_date"].apply(fiscal_year_for)
        normalized["month"] = normalized["order_date"].dt.to_period("M").astype(str)
        normalized["weekday"] = normalized["order_date"].dt.day_name().fillna("Unknown")

        return normalized[SNAPSHOT_COLUMNS]

    def apply_filters(self, filters: Dict[str, Any]) -> pd.DataFrame:
        if not self.ready:
            return pd.DataFrame(columns=SNAPSHOT_COLUMNS)

        df = self._ensure_search_dimensions(self._df)

        if filters.get("platform"):
            df = df[df["platform_raw"].isin(filters["platform"])]
        if filters.get("category"):
            df = df[df["category"].isin(filters["category"])]
        if filters.get("product"):
            product_token = filters["product"].strip()
            product_mask = (
                (df["product"] == product_token)
                | (df["sku"] == product_token)
                | (df["sku_base"] == product_token)
            )
            if not product_mask.any():
                product_mask = self._product_query_mask(df, product_token)
            df = df[product_mask]
        if filters.get("product_query"):
            query = filters["product_query"]
            df = df[self._product_query_mask(df, query)]
        if filters.get("start_date"):
            df = df[df["order_date"] >= pd.to_datetime(filters["start_date"])]
        if filters.get("end_date"):
            df = df[df["order_date"] < pd.to_datetime(filters["end_date"]) + pd.Timedelta(days=1)]

        return df

    def search_products(self, filters: Dict[str, Any], query: str, limit: int = 20) -> List[str]:
        if not self.ready or not query.strip():
            return []
        search_filters = dict(filters)
        search_filters["product_query"] = query.strip()
        df = self.apply_filters(search_filters)
        if df.empty:
            return []

        candidates: Dict[str, int] = {}
        for series, bonus in (
            (df["sku_base"], 0),
            (df["sku"], 1),
            (df["product"], 2),
        ):
            for value in series.dropna().astype(str).unique().tolist():
                cleaned = clean_text(value, "")
                if not cleaned:
                    continue
                rank = search_rank(cleaned, query, exact_bonus=bonus)
                existing = candidates.get(cleaned)
                if existing is None or rank < existing:
                    candidates[cleaned] = rank

        return [
            value
            for value, _ in sorted(candidates.items(), key=lambda item: (item[1], len(item[0]), item[0].lower()))
        ][:limit]

    def kpis(self, df: pd.DataFrame) -> Dict[str, Any]:
        if df.empty:
            return {
                "gross_sales": 0.0,
                "gross_sales_formatted": fmt_inr(0),
                "net_revenue": 0.0,
                "net_revenue_formatted": fmt_inr(0),
                "unique_orders": 0,
                "sales_volume": 0.0,
                "sales_volume_net": 0.0,
                "return_qty": 0.0,
                "return_value": 0.0,
                "return_value_formatted": fmt_inr(0),
                "aov": 0.0,
                "aov_formatted": fmt_inr(0),
                "asp": 0.0,
                "asp_formatted": fmt_inr(0),
                "return_rate_value": 0.0,
                "return_rate_qty": 0.0,
                "tax_collected": 0.0,
                "tax_collected_formatted": fmt_inr(0),
                "unique_products": 0,
                "active_platforms": 0,
                "active_categories": 0,
                "sales_rows": 0,
                "return_rows": 0,
                "return_only_rows": 0,
                "has_sales_activity": False,
                "total_revenue": 0.0,
                "total_revenue_formatted": fmt_inr(0),
                "total_orders": 0,
                "total_returns": 0.0,
                "total_returns_formatted": fmt_inr(0),
                "return_rate": 0.0,
            }

        gross_sales = safe_float(df["gross_sales"].sum())
        net_revenue = safe_float(df["net_revenue"].sum())
        unique_orders = int(df["order_id"].dropna().nunique())
        sale_volume = safe_float(df["sale_qty"].sum())
        net_volume = safe_float(df["net_qty"].sum())
        return_qty = safe_float(df["return_qty"].sum())
        return_value = safe_float(df["return_value"].sum())
        sales_rows = int((df["gross_sales"] > 0).sum())
        return_rows = int((df["return_value"] > 0).sum())
        return_only_rows = int(((df["gross_sales"] <= 0) & (df["return_value"] > 0)).sum())
        aov = net_revenue / unique_orders if unique_orders else 0.0
        asp = net_revenue / net_volume if net_volume else 0.0
        return_rate_value = (return_value / gross_sales * 100) if gross_sales else 0.0
        return_rate_qty = (return_qty / sale_volume * 100) if sale_volume else 0.0
        tax_collected = safe_float(df["tax"].sum())

        return {
            "gross_sales": gross_sales,
            "gross_sales_formatted": fmt_inr(gross_sales),
            "net_revenue": net_revenue,
            "net_revenue_formatted": fmt_inr(net_revenue),
            "unique_orders": unique_orders,
            "sales_volume": sale_volume,
            "sales_volume_net": net_volume,
            "return_qty": return_qty,
            "return_value": return_value,
            "return_value_formatted": fmt_inr(return_value),
            "aov": aov,
            "aov_formatted": fmt_inr(aov),
            "asp": asp,
            "asp_formatted": fmt_inr(asp),
            "return_rate_value": round(return_rate_value, 2),
            "return_rate_qty": round(return_rate_qty, 2),
            "tax_collected": tax_collected,
            "tax_collected_formatted": fmt_inr(tax_collected),
            "unique_products": int(df["product"].nunique()),
            "active_platforms": int(df["platform_raw"].nunique()),
            "active_categories": int(df["category"].nunique()),
            "sales_rows": sales_rows,
            "return_rows": return_rows,
            "return_only_rows": return_only_rows,
            "has_sales_activity": bool(sales_rows),
            "total_revenue": net_revenue,
            "total_revenue_formatted": fmt_inr(net_revenue),
            "total_orders": unique_orders,
            "total_returns": return_value,
            "total_returns_formatted": fmt_inr(return_value),
            "return_rate": round(return_rate_value, 2),
        }

    def revenue_trend(self, df: pd.DataFrame, trend_mode: Optional[str] = None) -> Dict[str, Any]:
        requested_frequency = clean_text(trend_mode, "auto").lower()
        if requested_frequency not in TREND_MODE_OPTIONS:
            requested_frequency = "auto"

        if df.empty:
            return {"frequency": "none", "requested_frequency": requested_frequency, "series": []}

        dates = df["order_date"].dropna()
        if dates.empty:
            return {"frequency": "none", "requested_frequency": requested_frequency, "series": []}

        if requested_frequency == "auto":
            span_days = int((dates.max() - dates.min()).days)
            if span_days <= 45:
                frequency = "daily"
            elif span_days <= 180:
                frequency = "weekly"
            else:
                frequency = "monthly"
        else:
            frequency = requested_frequency

        period_column = df["order_date"].dt.to_period(TREND_MODE_PERIODS[frequency]).astype(str)

        temp = df.assign(period=period_column)
        grouped = (
            temp.groupby("period", observed=True)
            .agg(
                net_revenue=("net_revenue", "sum"),
                gross_sales=("gross_sales", "sum"),
                returns=("return_value", "sum"),
                orders=("order_id", "nunique"),
            )
            .reset_index()
        )

        grouped["return_rate_value"] = np.where(
            grouped["gross_sales"] > 0,
            grouped["returns"] / grouped["gross_sales"] * 100,
            0.0,
        )

        return {
            "frequency": frequency,
            "requested_frequency": requested_frequency,
            "series": [
                {
                    "date": row["period"],
                    "net_revenue": safe_float(row["net_revenue"]),
                    "gross_sales": safe_float(row["gross_sales"]),
                    "returns": safe_float(row["returns"]),
                    "orders": safe_int(row["orders"]),
                    "return_rate_value": round(safe_float(row["return_rate_value"]), 2),
                }
                for _, row in grouped.iterrows()
            ],
        }

    def platform_data(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        if df.empty:
            return []

        total_net_revenue = safe_float(df["net_revenue"].sum())
        grouped = (
            df.groupby(["platform_raw", "platform_label"], observed=True)
            .agg(
                gross_sales=("gross_sales", "sum"),
                net_revenue=("net_revenue", "sum"),
                return_value=("return_value", "sum"),
                sale_qty=("sale_qty", "sum"),
                net_qty=("net_qty", "sum"),
                return_qty=("return_qty", "sum"),
                orders=("order_id", "nunique"),
            )
            .reset_index()
        )

        grouped["aov"] = np.where(grouped["orders"] > 0, grouped["net_revenue"] / grouped["orders"], 0.0)
        grouped["asp"] = np.where(grouped["net_qty"] > 0, grouped["net_revenue"] / grouped["net_qty"], 0.0)
        grouped["return_rate_value"] = np.where(
            grouped["gross_sales"] > 0,
            grouped["return_value"] / grouped["gross_sales"] * 100,
            0.0,
        )
        grouped["return_rate_qty"] = np.where(
            grouped["sale_qty"] > 0,
            grouped["return_qty"] / grouped["sale_qty"] * 100,
            0.0,
        )
        grouped["share"] = np.where(total_net_revenue > 0, grouped["net_revenue"] / total_net_revenue * 100, 0.0)
        grouped = grouped.sort_values("net_revenue", ascending=False)

        return [
            {
                "platform": row["platform_raw"],
                "platform_label": row["platform_label"],
                "color": PLATFORM_COLORS.get(row["platform_raw"], "#64748B"),
                "gross_sales": safe_float(row["gross_sales"]),
                "net_revenue": safe_float(row["net_revenue"]),
                "returns": safe_float(row["return_value"]),
                "sale_qty": safe_float(row["sale_qty"]),
                "net_qty": safe_float(row["net_qty"]),
                "return_qty": safe_float(row["return_qty"]),
                "orders": safe_int(row["orders"]),
                "aov": safe_float(row["aov"]),
                "asp": safe_float(row["asp"]),
                "return_rate_value": round(safe_float(row["return_rate_value"]), 2),
                "return_rate_qty": round(safe_float(row["return_rate_qty"]), 2),
                "share": round(safe_float(row["share"]), 2),
            }
            for _, row in grouped.iterrows()
        ]

    def category_data(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        if df.empty:
            return []

        total_net_revenue = safe_float(df["net_revenue"].sum())
        grouped = (
            df.groupby("category", observed=True)
            .agg(
                gross_sales=("gross_sales", "sum"),
                net_revenue=("net_revenue", "sum"),
                return_value=("return_value", "sum"),
                net_qty=("net_qty", "sum"),
                orders=("order_id", "nunique"),
            )
            .reset_index()
        )
        grouped["aov"] = np.where(grouped["orders"] > 0, grouped["net_revenue"] / grouped["orders"], 0.0)
        grouped["share"] = np.where(total_net_revenue > 0, grouped["net_revenue"] / total_net_revenue * 100, 0.0)
        grouped = grouped.sort_values("net_revenue", ascending=False)

        return [
            {
                "category": row["category"],
                "gross_sales": safe_float(row["gross_sales"]),
                "net_revenue": safe_float(row["net_revenue"]),
                "returns": safe_float(row["return_value"]),
                "net_qty": safe_float(row["net_qty"]),
                "orders": safe_int(row["orders"]),
                "aov": safe_float(row["aov"]),
                "share": round(safe_float(row["share"]), 2),
            }
            for _, row in grouped.iterrows()
        ]

    def top_products(self, df: pd.DataFrame, metric: str, n: int = 10) -> List[Dict[str, Any]]:
        if df.empty:
            return []

        grouped = (
            df.groupby("product", observed=True)
            .agg(
                net_revenue=("net_revenue", "sum"),
                gross_sales=("gross_sales", "sum"),
                return_value=("return_value", "sum"),
                net_qty=("net_qty", "sum"),
                sale_qty=("sale_qty", "sum"),
                return_qty=("return_qty", "sum"),
                orders=("order_id", "nunique"),
            )
            .reset_index()
        )

        metric_column = {
            "revenue": "net_revenue",
            "volume": "net_qty",
            "orders": "orders",
        }.get(metric, "net_revenue")

        grouped = grouped[~grouped["product"].apply(is_non_merch_product)]

        grouped["return_rate_value"] = np.where(
            grouped["gross_sales"] > 0,
            grouped["return_value"] / grouped["gross_sales"] * 100,
            0.0,
        )
        grouped["asp"] = np.where(grouped["net_qty"] > 0, grouped["net_revenue"] / grouped["net_qty"], 0.0)
        grouped = grouped.sort_values(metric_column, ascending=False).head(n)

        return [
            {
                "product": row["product"],
                "net_revenue": safe_float(row["net_revenue"]),
                "gross_sales": safe_float(row["gross_sales"]),
                "returns": safe_float(row["return_value"]),
                "net_qty": safe_float(row["net_qty"]),
                "sale_qty": safe_float(row["sale_qty"]),
                "return_qty": safe_float(row["return_qty"]),
                "orders": safe_int(row["orders"]),
                "return_rate_value": round(safe_float(row["return_rate_value"]), 2),
                "asp": safe_float(row["asp"]),
            }
            for _, row in grouped.iterrows()
        ]

    def returns_by_platform(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        return self.platform_data(df)

    def returns_trend(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        if df.empty:
            return []

        grouped = (
            df.groupby("month", observed=True)
            .agg(
                gross_sales=("gross_sales", "sum"),
                net_revenue=("net_revenue", "sum"),
                return_value=("return_value", "sum"),
                return_qty=("return_qty", "sum"),
            )
            .reset_index()
        )
        grouped["return_rate_value"] = np.where(
            grouped["gross_sales"] > 0,
            grouped["return_value"] / grouped["gross_sales"] * 100,
            0.0,
        )

        return [
            {
                "month": row["month"],
                "gross_sales": safe_float(row["gross_sales"]),
                "net_revenue": safe_float(row["net_revenue"]),
                "returns": safe_float(row["return_value"]),
                "return_qty": safe_float(row["return_qty"]),
                "rate": round(safe_float(row["return_rate_value"]), 2),
            }
            for _, row in grouped.iterrows()
        ]

    def returns_by_reason(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        returns_df = df[df["return_value"] > 0]
        if returns_df.empty:
            return []

        grouped = (
            returns_df.groupby("return_reason", observed=True)
            .agg(
                amount=("return_value", "sum"),
                return_qty=("return_qty", "sum"),
                orders=("order_id", "nunique"),
            )
            .reset_index()
            .sort_values("amount", ascending=False)
        )

        return [
            {
                "reason": row["return_reason"],
                "amount": safe_float(row["amount"]),
                "return_qty": safe_float(row["return_qty"]),
                "orders": safe_int(row["orders"]),
            }
            for _, row in grouped.iterrows()
        ]

    def returns_validity(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        returns_df = df[df["return_value"] > 0]
        if returns_df.empty:
            return []

        grouped = (
            returns_df.groupby("return_validity", observed=True)["return_value"]
            .sum()
            .reset_index()
            .sort_values("return_value", ascending=False)
        )

        return [
            {
                "validity": row["return_validity"],
                "amount": safe_float(row["return_value"]),
            }
            for _, row in grouped.iterrows()
        ]

    def operations_summary(self, df: pd.DataFrame) -> Dict[str, Any]:
        if df.empty:
            return {
                "tax_collected": 0.0,
                "tax_collected_formatted": fmt_inr(0),
                "orders_per_month": [],
                "orders_by_weekday": [],
            }

        orders_per_month = (
            df.groupby("month", observed=True)
            .agg(
                orders=("order_id", "nunique"),
                gross_sales=("gross_sales", "sum"),
                net_revenue=("net_revenue", "sum"),
                returns=("return_value", "sum"),
            )
            .reset_index()
        )

        weekday_order = pd.CategoricalDtype(
            categories=[
                "Monday",
                "Tuesday",
                "Wednesday",
                "Thursday",
                "Friday",
                "Saturday",
                "Sunday",
            ],
            ordered=True,
        )
        weekday_df = df.copy()
        weekday_df["weekday"] = weekday_df["weekday"].astype(weekday_order)
        orders_by_weekday = (
            weekday_df.groupby("weekday", observed=True)
            .agg(
                orders=("order_id", "nunique"),
                net_revenue=("net_revenue", "sum"),
            )
            .reset_index()
            .sort_values("weekday")
        )

        tax_collected = safe_float(df["tax"].sum())
        return {
            "tax_collected": tax_collected,
            "tax_collected_formatted": fmt_inr(tax_collected),
            "orders_per_month": [
                {
                    "month": row["month"],
                    "orders": safe_int(row["orders"]),
                    "gross_sales": safe_float(row["gross_sales"]),
                    "net_revenue": safe_float(row["net_revenue"]),
                    "returns": safe_float(row["returns"]),
                }
                for _, row in orders_per_month.iterrows()
            ],
            "orders_by_weekday": [
                {
                    "weekday": row["weekday"],
                    "orders": safe_int(row["orders"]),
                    "net_revenue": safe_float(row["net_revenue"]),
                }
                for _, row in orders_by_weekday.iterrows()
            ],
        }

    def dynamic_insights(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        if df.empty:
            return []

        insights: List[Dict[str, Any]] = []
        kpis = self.kpis(df)
        platforms = self.platform_data(df)
        categories = self.category_data(df)
        trend = self.returns_trend(df)
        top_products = self.top_products(df, "revenue", 3)
        return_reasons = self.returns_by_reason(df)
        return_validity = self.returns_validity(df)
        operations = self.operations_summary(df)

        insights.append(
            {
                "title": "Revenue converted to net",
                "metric": kpis["net_revenue_formatted"],
                "body": (
                    f"Total revenue is {kpis['gross_sales_formatted']}. Returns remove "
                    f"{kpis['return_value_formatted']}, leaving {kpis['net_revenue_formatted']} net revenue "
                    "after returns."
                ),
                "note": f"{pct(kpis['return_rate_value'])} value return rate for the current slice.",
            }
        )

        if platforms:
            top_platform = platforms[0]
            insights.append(
                {
                    "title": "Channel concentration",
                    "metric": pct(top_platform["share"]),
                    "body": (
                        f"{top_platform['platform_label']} leads this slice with "
                        f"{fmt_inr(top_platform['net_revenue'])} net revenue and "
                        f"{top_platform['share']:.1f}% share."
                    ),
                }
            )

            eligible = [row for row in platforms if row["orders"] >= 250]
            if eligible:
                highest_rto = max(eligible, key=lambda row: row["return_rate_value"])
                insights.append(
                    {
                        "title": "Highest value return exposure",
                        "metric": pct(highest_rto["return_rate_value"]),
                        "body": (
                            f"{highest_rto['platform_label']} is the riskiest large channel here with "
                            f"{highest_rto['return_rate_value']:.1f}% value RTO across "
                            f"{highest_rto['orders']:,} unique orders."
                        ),
                    }
                )

        if categories:
            top_category = categories[0]
            insights.append(
                {
                    "title": "Category leader",
                    "metric": fmt_inr(top_category["net_revenue"]),
                    "body": (
                        f"{top_category['category']} contributes {fmt_inr(top_category['net_revenue'])} "
                        f"from {top_category['orders']:,} orders."
                    ),
                }
            )

        if top_products:
            top_product = top_products[0]
            insights.append(
                {
                    "title": "Hero product",
                    "metric": fmt_inr(top_product["net_revenue"]),
                    "body": (
                        f"{top_product['product']} is the highest net-revenue product in this slice with "
                        f"{top_product['orders']:,} unique orders and {top_product['return_rate_value']:.1f}% "
                        "value RTO."
                    ),
                }
            )

        if return_validity:
            invalid_amount = sum(
                safe_float(row["amount"])
                for row in return_validity
                if clean_text(row["validity"], "").lower() == "invalid"
            )
            total_return_amount = sum(safe_float(row["amount"]) for row in return_validity)
            invalid_share = safe_divide(invalid_amount, total_return_amount)
            insights.append(
                {
                    "title": "Return quality control",
                    "metric": format_signed_percentage((invalid_share or 0.0) * 100, decimals=1).replace("+", ""),
                    "body": (
                        f"Invalid returns currently account for {fmt_inr(invalid_amount)} out of "
                        f"{fmt_inr(total_return_amount)} total return value."
                    ),
                    "note": "Lower is better. This helps separate commercial returns from process leakage.",
                }
            )

        if trend:
            best_month = max(trend, key=lambda row: row["net_revenue"])
            worst_month = min(trend, key=lambda row: row["net_revenue"])
            insights.append(
                {
                    "title": "Demand swing",
                    "metric": f"{best_month['month']} vs {worst_month['month']}",
                    "body": (
                        f"Best month in the filtered range is {best_month['month']} at "
                        f"{fmt_inr(best_month['net_revenue'])}; weakest is {worst_month['month']} at "
                        f"{fmt_inr(worst_month['net_revenue'])}."
                    ),
                }
            )

        if return_reasons:
            top_reason = return_reasons[0]
            insights.append(
                {
                    "title": "Primary return driver",
                    "metric": clean_text(top_reason["reason"], "Unspecified"),
                    "body": (
                        f"The top coded return reason contributes {fmt_inr(top_reason['amount'])} across "
                        f"{top_reason['orders']:,} unique orders."
                    ),
                }
            )

        if operations["orders_by_weekday"]:
            busiest_weekday = max(operations["orders_by_weekday"], key=lambda row: row["orders"])
            insights.append(
                {
                    "title": "Order rhythm",
                    "metric": busiest_weekday["weekday"],
                    "body": (
                        f"{busiest_weekday['weekday']} is the strongest order day in this slice with "
                        f"{busiest_weekday['orders']:,} unique orders and "
                        f"{fmt_inr(busiest_weekday['net_revenue'])} net revenue."
                    ),
                }
            )

        return insights[:8]

    def export_csv(self, df: pd.DataFrame) -> str:
        if df.empty:
            return ""
        export_frame = df.copy()
        export_frame["order_date"] = export_frame["order_date"].dt.strftime("%Y-%m-%d")
        return export_frame.to_csv(index=False)


_dm = DataManager()
_cache = CacheManager(ttl_seconds=300)


app = FastAPI(title="SalesTrendsDashboard", version="3.0.0", docs_url=None, redoc_url=None)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def normalize_vercel_api_path(request: Request, call_next):
    path = request.scope.get("path", "")
    route_override = request.query_params.get("route", "").strip("/")
    if route_override and path == "/api":
        request.scope["path"] = f"/api/{route_override}"
    elif path and path != "/" and not path.startswith("/api"):
        request.scope["path"] = f"/api{path}"
    return await call_next(request)


def cached_response(endpoint: str, params: Dict[str, Any], builder: Any) -> Any:
    key = build_cache_key(endpoint, params, _dm.version)
    cached = _cache.get(key)
    if cached is not None:
        return cached
    result = builder()
    _cache.set(key, result)
    return result


def dashboard_payload(filters: Dict[str, Any]) -> Dict[str, Any]:
    df = _dm.apply_filters(filters)
    summary_sheet = _dm.summary_sheet_for(filters, df)
    return {
        "kpis": _dm.kpis(df),
        "trend": _dm.revenue_trend(df, filters.get("trend_mode")),
        "platforms": _dm.platform_data(df),
        "categories": _dm.category_data(df),
        "top_products": {
            "revenue": _dm.top_products(df, "revenue", 10),
            "volume": _dm.top_products(df, "volume", 10),
            "orders": _dm.top_products(df, "orders", 10),
        },
        "returns": {
            "trend": _dm.returns_trend(df),
            "by_platform": _dm.returns_by_platform(df),
            "by_reason": _dm.returns_by_reason(df),
            "validity": _dm.returns_validity(df),
        },
        "operations": _dm.operations_summary(df),
        "insights": _dm.dynamic_insights(df),
        "summary_sheet": summary_sheet,
    }


@app.get("/", response_class=HTMLResponse)
async def root(_: Request) -> HTMLResponse:
    if not HTML_PATH.exists():
        return HTMLResponse("<h1>dashboard.html is missing</h1>", status_code=500)
    return HTMLResponse(HTML_PATH.read_text(encoding="utf-8"))


@app.get("/api/health")
async def health() -> Dict[str, Any]:
    info = _dm.health()
    info["cache_entries"] = len(_cache._cache)
    info["cache_ttl_seconds"] = _cache.ttl_seconds
    return info


@app.get("/api/filters")
async def get_filters() -> Dict[str, Any]:
    return _dm.filter_options()


@app.get("/api/reload")
async def reload_data(url: Optional[str] = None, mode: Optional[str] = None) -> Dict[str, Any]:
    try:
        normalized_mode = (mode or "").strip().lower()
        if url:
            result = _dm.load_from_url(url)
        elif normalized_mode == "order_hub":
            result = _dm.load_order_hub_source()
        elif normalized_mode == "default":
            result = _dm.reset_to_default_source()
        else:
            result = _dm.refresh_current_source()
    except ValueError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    _cache.clear()
    return result


@app.get("/api/search/products")
async def search_products(
    q: str,
    platform: Optional[str] = None,
    category: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> Dict[str, Any]:
    filters = parse_filters(platform=platform, category=category, start_date=start_date, end_date=end_date)
    return {"items": _dm.search_products(filters, q)}


@app.get("/api/kpis")
async def get_kpis(
    platform: Optional[str] = None,
    category: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    product: Optional[str] = None,
    product_query: Optional[str] = None,
) -> Dict[str, Any]:
    filters = parse_filters(platform, category, start_date, end_date, product, product_query)
    return cached_response("kpis", filters, lambda: _dm.kpis(_dm.apply_filters(filters)))


@app.get("/api/trend")
async def get_trend(
    platform: Optional[str] = None,
    category: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    product: Optional[str] = None,
    product_query: Optional[str] = None,
    trend_mode: Optional[str] = Query(None, pattern="^(auto|daily|weekly|monthly)$"),
) -> Dict[str, Any]:
    filters = parse_filters(platform, category, start_date, end_date, product, product_query, trend_mode)
    return cached_response("trend", filters, lambda: _dm.revenue_trend(_dm.apply_filters(filters), filters.get("trend_mode")))


@app.get("/api/platforms")
async def get_platforms(
    platform: Optional[str] = None,
    category: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    product: Optional[str] = None,
    product_query: Optional[str] = None,
) -> List[Dict[str, Any]]:
    filters = parse_filters(platform, category, start_date, end_date, product, product_query)
    return cached_response("platforms", filters, lambda: _dm.platform_data(_dm.apply_filters(filters)))


@app.get("/api/categories")
async def get_categories(
    platform: Optional[str] = None,
    category: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    product: Optional[str] = None,
    product_query: Optional[str] = None,
) -> List[Dict[str, Any]]:
    filters = parse_filters(platform, category, start_date, end_date, product, product_query)
    return cached_response("categories", filters, lambda: _dm.category_data(_dm.apply_filters(filters)))


@app.get("/api/products")
async def get_products(
    n: int = Query(10, ge=5, le=50),
    metric: str = Query("revenue", pattern="^(revenue|volume|orders)$"),
    platform: Optional[str] = None,
    category: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    product: Optional[str] = None,
    product_query: Optional[str] = None,
) -> List[Dict[str, Any]]:
    filters = parse_filters(platform, category, start_date, end_date, product, product_query)
    params = dict(filters)
    params["metric"] = metric
    params["n"] = n
    return cached_response(
        "products",
        params,
        lambda: _dm.top_products(_dm.apply_filters(filters), metric=metric, n=n),
    )


@app.get("/api/products/volume")
async def get_products_volume(
    n: int = Query(10, ge=5, le=50),
    platform: Optional[str] = None,
    category: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    product: Optional[str] = None,
    product_query: Optional[str] = None,
) -> List[Dict[str, Any]]:
    filters = parse_filters(platform, category, start_date, end_date, product, product_query)
    params = dict(filters)
    params["n"] = n
    return cached_response(
        "products_volume",
        params,
        lambda: _dm.top_products(_dm.apply_filters(filters), metric="volume", n=n),
    )


@app.get("/api/products/orders")
async def get_products_orders(
    n: int = Query(10, ge=5, le=50),
    platform: Optional[str] = None,
    category: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    product: Optional[str] = None,
    product_query: Optional[str] = None,
) -> List[Dict[str, Any]]:
    filters = parse_filters(platform, category, start_date, end_date, product, product_query)
    params = dict(filters)
    params["n"] = n
    return cached_response(
        "products_orders",
        params,
        lambda: _dm.top_products(_dm.apply_filters(filters), metric="orders", n=n),
    )


@app.get("/api/returns/by-platform")
async def get_returns_by_platform(
    platform: Optional[str] = None,
    category: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    product: Optional[str] = None,
    product_query: Optional[str] = None,
) -> List[Dict[str, Any]]:
    filters = parse_filters(platform, category, start_date, end_date, product, product_query)
    return cached_response(
        "returns_by_platform",
        filters,
        lambda: _dm.returns_by_platform(_dm.apply_filters(filters)),
    )


@app.get("/api/returns/trend")
async def get_returns_trend(
    platform: Optional[str] = None,
    category: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    product: Optional[str] = None,
    product_query: Optional[str] = None,
) -> List[Dict[str, Any]]:
    filters = parse_filters(platform, category, start_date, end_date, product, product_query)
    return cached_response("returns_trend", filters, lambda: _dm.returns_trend(_dm.apply_filters(filters)))


@app.get("/api/returns/by-reason")
async def get_returns_by_reason(
    platform: Optional[str] = None,
    category: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    product: Optional[str] = None,
    product_query: Optional[str] = None,
) -> List[Dict[str, Any]]:
    filters = parse_filters(platform, category, start_date, end_date, product, product_query)
    return cached_response(
        "returns_by_reason",
        filters,
        lambda: _dm.returns_by_reason(_dm.apply_filters(filters)),
    )


@app.get("/api/returns/validity")
async def get_returns_validity(
    platform: Optional[str] = None,
    category: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    product: Optional[str] = None,
    product_query: Optional[str] = None,
) -> List[Dict[str, Any]]:
    filters = parse_filters(platform, category, start_date, end_date, product, product_query)
    return cached_response(
        "returns_validity",
        filters,
        lambda: _dm.returns_validity(_dm.apply_filters(filters)),
    )


@app.get("/api/operations")
async def get_operations(
    platform: Optional[str] = None,
    category: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    product: Optional[str] = None,
    product_query: Optional[str] = None,
) -> Dict[str, Any]:
    filters = parse_filters(platform, category, start_date, end_date, product, product_query)
    return cached_response("operations", filters, lambda: _dm.operations_summary(_dm.apply_filters(filters)))


@app.get("/api/summary-sheet")
async def get_summary_sheet() -> Dict[str, Any]:
    return _dm.summary_sheet()


@app.get("/api/dashboard")
async def get_dashboard(
    platform: Optional[str] = None,
    category: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    product: Optional[str] = None,
    product_query: Optional[str] = None,
    trend_mode: Optional[str] = Query(None, pattern="^(auto|daily|weekly|monthly)$"),
) -> Dict[str, Any]:
    filters = parse_filters(platform, category, start_date, end_date, product, product_query, trend_mode)
    return cached_response("dashboard", filters, lambda: dashboard_payload(filters))


@app.get("/api/export")
async def export_csv(
    platform: Optional[str] = None,
    category: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    product: Optional[str] = None,
    product_query: Optional[str] = None,
) -> Response:
    filters = parse_filters(platform, category, start_date, end_date, product, product_query)
    csv = _dm.export_csv(_dm.apply_filters(filters))
    return Response(
        content=csv,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=sales_export.csv"},
    )


try:
    from mangum import Mangum

    handler = Mangum(app, lifespan="off")
except ImportError:
    handler = None
