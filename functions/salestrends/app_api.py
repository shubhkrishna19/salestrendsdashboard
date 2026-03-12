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
from datetime import UTC, datetime
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


def fiscal_year_for(date_value: pd.Timestamp) -> str:
    if pd.isna(date_value):
        return "Unknown"
    if date_value.month >= 4:
        return f"FY{date_value.year}-{str(date_value.year + 1)[-2:]}"
    return f"FY{date_value.year - 1}-{str(date_value.year)[-2:]}"


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
            "summary_sheet_available": bool(self._summary_sheet),
            "data_source": {
                "source": self._source,
                "source_type": self._source_type,
            },
        }
        if not date_series.empty:
            out["date_range"] = {
                "min": date_series.min().strftime("%Y-%m-%d"),
                "max": date_series.max().strftime("%Y-%m-%d"),
            }
        return out

    def summary_sheet(self) -> Dict[str, Any]:
        return self._summary_sheet

    def load_from_url(self, url: str) -> Dict[str, Any]:
        if not self._load(preferred_url=url, prefer_snapshot=False):
            raise ValueError(self._load_error or "Workbook reload failed.")
        return self.health()

    def refresh_current_source(self) -> Dict[str, Any]:
        if not self._load(prefer_snapshot=False):
            raise ValueError(self._load_error or "Current source refresh failed.")
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

    def _load(self, preferred_url: Optional[str] = None, prefer_snapshot: bool = True) -> bool:
        self._load_error = None
        previous_state = self._current_state()

        if prefer_snapshot and preferred_url is None and self._load_snapshot():
            log.info("Loaded dashboard data from snapshot.")
            return True

        loaders: List[Tuple[str, str, Any]] = []
        if preferred_url:
            loaders.append(("url", preferred_url, self._load_remote_excel))
        else:
            if DATA_URL:
                loaders.append(("url", DATA_URL, self._load_remote_excel))
            elif self._source_type == "url" and str(self._source).startswith(("http://", "https://")):
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
        self._df = df
        self._source = source
        self._source_type = source_type
        self._summary_sheet = summary_sheet or {}
        self._loaded_at = datetime.now(UTC)
        self._data_version = self._loaded_at.strftime("%Y%m%d%H%M%S")
        self._load_error = None
        log.info("Loaded %s normalized rows from %s.", len(df), source)

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

        df = self._df

        if filters.get("platform"):
            df = df[df["platform_raw"].isin(filters["platform"])]
        if filters.get("category"):
            df = df[df["category"].isin(filters["category"])]
        if filters.get("product"):
            df = df[df["product"] == filters["product"]]
        if filters.get("product_query"):
            query = filters["product_query"]
            df = df[df["product"].str.contains(query, case=False, na=False)]
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
        products = sorted(df["product"].dropna().unique().tolist())
        return products[:limit]

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
            "total_revenue": net_revenue,
            "total_revenue_formatted": fmt_inr(net_revenue),
            "total_orders": unique_orders,
            "total_returns": return_value,
            "total_returns_formatted": fmt_inr(return_value),
            "return_rate": round(return_rate_value, 2),
        }

    def revenue_trend(self, df: pd.DataFrame) -> Dict[str, Any]:
        if df.empty:
            return {"frequency": "none", "series": []}

        dates = df["order_date"].dropna()
        if dates.empty:
            return {"frequency": "none", "series": []}

        span_days = int((dates.max() - dates.min()).days)
        if span_days <= 45:
            period_column = df["order_date"].dt.to_period("D").astype(str)
            frequency = "daily"
        elif span_days <= 180:
            period_column = df["order_date"].dt.to_period("W").astype(str)
            frequency = "weekly"
        else:
            period_column = df["order_date"].dt.to_period("M").astype(str)
            frequency = "monthly"

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

    def dynamic_insights(self, df: pd.DataFrame) -> List[Dict[str, str]]:
        if df.empty:
            return []

        insights: List[Dict[str, str]] = []
        kpis = self.kpis(df)
        platforms = self.platform_data(df)
        categories = self.category_data(df)
        trend = self.returns_trend(df)

        if platforms:
            top_platform = platforms[0]
            insights.append(
                {
                    "title": "Top channel",
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
                        "title": "Highest value return rate",
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
                    "title": "Top category",
                    "body": (
                        f"{top_category['category']} contributes {fmt_inr(top_category['net_revenue'])} "
                        f"from {top_category['orders']:,} orders."
                    ),
                }
            )

        if trend:
            best_month = max(trend, key=lambda row: row["net_revenue"])
            worst_month = min(trend, key=lambda row: row["net_revenue"])
            insights.append(
                {
                    "title": "Monthly range",
                    "body": (
                        f"Best month in the filtered range is {best_month['month']} at "
                        f"{fmt_inr(best_month['net_revenue'])}; weakest is {worst_month['month']} at "
                        f"{fmt_inr(worst_month['net_revenue'])}."
                    ),
                }
            )

        insights.append(
            {
                "title": "Commercial quality",
                "body": (
                    f"This slice is running at {kpis['return_rate_value']:.1f}% value RTO, "
                    f"{kpis['return_rate_qty']:.1f}% quantity RTO, and "
                    f"{fmt_inr(kpis['aov'])} AOV."
                ),
            }
        )

        return insights

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
    return {
        "kpis": _dm.kpis(df),
        "trend": _dm.revenue_trend(df),
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
        "summary_sheet": _dm.summary_sheet(),
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
async def reload_data(url: Optional[str] = None) -> Dict[str, Any]:
    try:
        result = _dm.load_from_url(url) if url else _dm.refresh_current_source()
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
) -> Dict[str, Any]:
    filters = parse_filters(platform, category, start_date, end_date, product, product_query)
    return cached_response("trend", filters, lambda: _dm.revenue_trend(_dm.apply_filters(filters)))


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
) -> Dict[str, Any]:
    filters = parse_filters(platform, category, start_date, end_date, product, product_query)
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
