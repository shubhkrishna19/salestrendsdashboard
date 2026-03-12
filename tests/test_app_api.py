from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest
from fastapi.testclient import TestClient

APP_DIR = Path(__file__).resolve().parents[1] / "functions" / "salestrends"
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

import app_api


def sample_raw_workbook() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "Final Order date": "2025-01-01",
                "Main Parties": "Amazon Online Sale",
                "Group Name": "Beds",
                "Item Desc": "Alpha Bed",
                "Alias": "ALPHA-1",
                "Sale (Qty.)": 2,
                "Sale Return (Qty.)": -1,
                "Sale (Amt.)": 1000,
                "Sale Return (Amt.)": -200,
                "Tax Value": 180,
                "Order ID": "ORD-1",
                "Return Type": "Damaged",
                "Valid/Invalid": "Valid",
            }
        ]
    )


def sample_snapshot_frame() -> pd.DataFrame:
    frame = pd.DataFrame(
        {
            "order_date": pd.to_datetime(["2025-01-01"]),
            "platform_raw": ["Amazon Online Sale"],
            "platform_label": ["Amazon"],
            "category": ["Beds"],
            "product": ["Alpha Bed"],
            "sku": ["ALPHA-1"],
            "sale_qty": [2.0],
            "return_qty_signed": [-1.0],
            "return_qty": [1.0],
            "gross_sales": [1000.0],
            "return_value_signed": [-200.0],
            "return_value": [200.0],
            "net_qty": [1.0],
            "net_revenue": [800.0],
            "tax": [180.0],
            "order_id": ["ORD-1"],
            "return_reason": ["Damaged"],
            "return_validity": ["Valid"],
            "fy": ["FY2024-25"],
            "month": ["2025-01"],
            "weekday": ["Wednesday"],
        }
    )
    return frame[app_api.SNAPSHOT_COLUMNS]


@pytest.fixture(scope="session")
def dm() -> app_api.DataManager:
    return app_api._dm


@pytest.fixture(scope="session")
def client() -> TestClient:
    with TestClient(app_api.app) as test_client:
        yield test_client


def test_dataset_baseline_kpis(dm: app_api.DataManager) -> None:
    df = dm.apply_filters({})
    kpis = dm.kpis(df)

    assert len(df) == 151770
    assert kpis["unique_orders"] == 107748
    assert kpis["gross_sales"] == pytest.approx(397286879.0)
    assert kpis["return_value"] == pytest.approx(59496957.0)
    assert kpis["net_revenue"] == pytest.approx(337789922.0)
    assert kpis["return_rate_value"] == pytest.approx(14.98)
    assert kpis["return_rate_qty"] == pytest.approx(3.86)


def test_blank_order_ids_do_not_inflate_grouped_metrics(dm: app_api.DataManager) -> None:
    df = dm.apply_filters({})

    assert int(df["order_id"].isna().sum()) == 24514

    expected_orders = (
        df.groupby("platform_raw", observed=True)
        .agg(orders=("order_id", "nunique"))
        .reset_index()
        .set_index("platform_raw")["orders"]
        .to_dict()
    )
    actual_orders = {row["platform"]: row["orders"] for row in dm.platform_data(df)}

    assert actual_orders == expected_orders
    assert actual_orders["Amazon Online Sale"] == 72113
    assert actual_orders["Flipkart Online Sale"] == 13386


def test_end_date_filter_is_inclusive(dm: app_api.DataManager) -> None:
    filtered = dm.apply_filters({"start_date": "2025-02-28", "end_date": "2025-02-28"})
    expected_rows = int((dm.apply_filters({})["order_date"] == pd.Timestamp("2025-02-28")).sum())

    assert len(filtered) == expected_rows
    assert len(filtered) > 0


def test_dashboard_api_contract(client: TestClient) -> None:
    response = client.get("/api/dashboard")

    assert response.status_code == 200
    payload = response.json()
    assert sorted(payload.keys()) == [
        "categories",
        "insights",
        "kpis",
        "operations",
        "platforms",
        "returns",
        "summary_sheet",
        "top_products",
        "trend",
    ]
    assert sorted(payload["top_products"].keys()) == ["orders", "revenue", "volume"]
    assert sorted(payload["returns"].keys()) == ["by_platform", "by_reason", "trend", "validity"]


def test_unprefixed_health_route_is_normalized_for_vercel_function_mount(client: TestClient) -> None:
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_unprefixed_dashboard_route_is_normalized_for_vercel_function_mount(client: TestClient) -> None:
    response = client.get("/dashboard")

    assert response.status_code == 200
    assert "kpis" in response.json()


def test_rewritten_vercel_api_route_query_is_normalized(client: TestClient) -> None:
    response = client.get("/api", params={"route": "health"})

    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_root_serves_dashboard_shell(client: TestClient) -> None:
    response = client.get("/")

    assert response.status_code == 200
    assert "Bluewud Sales Intelligence" in response.text
    assert 'id="overviewKpis"' in response.text
    assert 'id="chartTrend"' in response.text


def test_filtered_dashboard_respects_selected_platform(
    client: TestClient, dm: app_api.DataManager
) -> None:
    response = client.get(
        "/api/dashboard",
        params={
            "platform": "Amazon Online Sale",
            "start_date": "2025-01-01",
            "end_date": "2025-02-28",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    expected_unique_orders = dm.kpis(
        dm.apply_filters(
            {
                "platform": ["Amazon Online Sale"],
                "start_date": "2025-01-01",
                "end_date": "2025-02-28",
            }
        )
    )["unique_orders"]
    assert payload["kpis"]["unique_orders"] == expected_unique_orders
    assert {row["platform"] for row in payload["platforms"]} == {"Amazon Online Sale"}
    assert payload["trend"]["frequency"] == "weekly"


def test_summary_sheet_endpoint_returns_dict(client: TestClient) -> None:
    response = client.get("/api/summary-sheet")

    assert response.status_code == 200
    payload = response.json()
    assert isinstance(payload, dict)


def test_local_workbook_summary_sheet_parses_expected_shape() -> None:
    workbook_path = Path(__file__).resolve().parents[1] / "data.xlsx"
    sheet = app_api.read_excel_quietly(workbook_path, sheet_name=app_api.SUMMARY_SHEET_NAME, header=None)
    payload = app_api.parse_summary_sheet(sheet)

    assert sorted(payload.keys()) == [
        "budget_vs_achievement",
        "channel_growth",
        "channel_performance_current",
        "headline_cards",
        "insights",
        "monthly_fy_sales",
        "rto_monthly_current",
    ]
    assert len(payload["headline_cards"]) == 12
    assert len(payload["budget_vs_achievement"]) == 13


def test_refresh_current_source_bypasses_snapshot(monkeypatch: pytest.MonkeyPatch) -> None:
    manager = app_api.DataManager.__new__(app_api.DataManager)
    manager._df = sample_snapshot_frame()
    manager._loaded_at = pd.Timestamp("2026-03-12").to_pydatetime()
    manager._source = "https://example.com/data.xlsx"
    manager._source_type = "url"
    manager._summary_sheet = {}
    manager._load_error = None
    manager._data_version = "seed"

    calls: list[object] = []

    def fake_snapshot(self: app_api.DataManager) -> bool:
        calls.append("snapshot")
        return False

    def fake_remote(self: app_api.DataManager, url: str):
        calls.append(("url", url))
        return sample_raw_workbook(), {}

    def fake_local(self: app_api.DataManager, _: str):
        calls.append("local")
        raise AssertionError("Local fallback should not run after a successful URL refresh.")

    def fake_write_snapshot(self: app_api.DataManager) -> None:
        calls.append("write")

    monkeypatch.setattr(app_api.DataManager, "_load_snapshot", fake_snapshot)
    monkeypatch.setattr(app_api.DataManager, "_load_remote_excel", fake_remote)
    monkeypatch.setattr(app_api.DataManager, "_load_local", fake_local)
    monkeypatch.setattr(app_api.DataManager, "_write_snapshot", fake_write_snapshot)

    result = manager.refresh_current_source()

    assert result["source"] == "https://example.com/data.xlsx"
    assert "snapshot" not in calls
    assert ("url", "https://example.com/data.xlsx") in calls
    assert "local" not in calls
    assert "write" in calls


def test_explicit_url_failure_preserves_previous_dataset(monkeypatch: pytest.MonkeyPatch) -> None:
    previous_df = sample_snapshot_frame()

    manager = app_api.DataManager.__new__(app_api.DataManager)
    manager._df = previous_df
    manager._loaded_at = pd.Timestamp("2026-03-12").to_pydatetime()
    manager._source = "data.xlsx"
    manager._source_type = "local"
    manager._summary_sheet = {"ok": True}
    manager._load_error = None
    manager._data_version = "seed"

    def fake_remote(self: app_api.DataManager, _: str):
        raise ValueError("bad workbook link")

    def fake_snapshot(self: app_api.DataManager) -> bool:
        raise AssertionError("Snapshot path should not be used for an explicit URL load.")

    def fake_local(self: app_api.DataManager, _: str):
        raise AssertionError("Local fallback must not mask an explicit URL failure.")

    monkeypatch.setattr(app_api.DataManager, "_load_remote_excel", fake_remote)
    monkeypatch.setattr(app_api.DataManager, "_load_snapshot", fake_snapshot)
    monkeypatch.setattr(app_api.DataManager, "_load_local", fake_local)

    with pytest.raises(ValueError, match="bad workbook link"):
        manager.load_from_url("https://broken.example.com/data.xlsx")

    assert manager._df.equals(previous_df)
    assert manager._source == "data.xlsx"
    assert manager._source_type == "local"
    assert manager._load_error == "bad workbook link"


def test_url_normalization_helpers() -> None:
    google_url = "https://drive.google.com/file/d/abc123XYZ/view?usp=sharing"
    google_sheet_url = "https://docs.google.com/spreadsheets/d/abc123XYZ/edit?usp=sharing&gid=123456"
    sharepoint_url = "https://company.sharepoint.com/:x:/r/sites/analytics/report.xlsx?csf=1&web=1"

    assert app_api.normalize_data_url(google_url) == "https://drive.google.com/uc?export=download&id=abc123XYZ"
    assert (
        app_api.normalize_data_url(google_sheet_url)
        == "https://docs.google.com/spreadsheets/d/abc123XYZ/export?format=xlsx&gid=123456"
    )
    assert app_api.normalize_data_url(sharepoint_url).endswith("&download=1")
