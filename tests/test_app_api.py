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


def sample_fy_snapshot_frame() -> pd.DataFrame:
    frame = pd.DataFrame(
        {
            "order_date": pd.to_datetime(["2025-04-10", "2025-04-11", "2025-03-15", "2025-03-20"]),
            "platform_raw": [
                "Amazon Online Sale",
                "Flipkart Online Sale",
                "Amazon Online Sale",
                "Flipkart Online Sale",
            ],
            "platform_label": ["Amazon", "Flipkart", "Amazon", "Flipkart"],
            "category": ["Beds", "Chairs", "Beds", "Chairs"],
            "product": ["Alpha Bed", "Beta Chair", "Alpha Bed", "Beta Chair"],
            "sku": ["ALPHA-1", "BETA-1", "ALPHA-1", "BETA-1"],
            "sale_qty": [2.0, 1.0, 1.0, 1.0],
            "return_qty_signed": [0.0, -1.0, 0.0, 0.0],
            "return_qty": [0.0, 1.0, 0.0, 0.0],
            "gross_sales": [1000.0, 800.0, 700.0, 500.0],
            "return_value_signed": [0.0, -100.0, 0.0, 0.0],
            "return_value": [0.0, 100.0, 0.0, 0.0],
            "net_qty": [2.0, 0.0, 1.0, 1.0],
            "net_revenue": [1000.0, 700.0, 700.0, 500.0],
            "tax": [180.0, 144.0, 126.0, 90.0],
            "order_id": ["ORD-1", "ORD-2", "ORD-3", "ORD-4"],
            "return_reason": ["None", "Damaged", "None", "None"],
            "return_validity": ["Valid", "Invalid", "Valid", "Valid"],
            "fy": ["FY2025-26", "FY2025-26", "FY2024-25", "FY2024-25"],
            "month": ["2025-04", "2025-04", "2025-03", "2025-03"],
            "weekday": ["Thursday", "Friday", "Saturday", "Thursday"],
        }
    )
    return frame[app_api.SNAPSHOT_COLUMNS]


def sample_sku_search_snapshot_frame() -> pd.DataFrame:
    frame = pd.DataFrame(
        {
            "order_date": pd.to_datetime(["2025-04-10", "2025-04-11", "2025-04-12", "2025-04-13"]),
            "platform_raw": [
                "Flipkart Online Sale",
                "Flipkart Online Sale",
                "Amazon Online Sale",
                "Amazon Online Sale",
            ],
            "platform_label": ["Flipkart", "Flipkart", "Amazon", "Amazon"],
            "category": ["Study Tables", "Study Tables", "Shoe Racks", "Shoe Racks"],
            "product": [
                "Bluewud Corbyn L Shape Study Table-Maple",
                "Bluewud Corbyn L Shape Study Ta-Maple-CL",
                "Bluewud Kaspen Shoe Rack Wenge(FW)",
                "Bluewud Kaspen Shoe Rack Maple (MF)",
            ],
            "sku": ["ST-CBN-LSMF", "ST-CBN-LSMF-CL", "SR-KPN-FW", "SR-KPN-MF"],
            "sale_qty": [1.0, 1.0, 1.0, 1.0],
            "return_qty_signed": [0.0, 0.0, 0.0, 0.0],
            "return_qty": [0.0, 0.0, 0.0, 0.0],
            "gross_sales": [1000.0, 1100.0, 900.0, 950.0],
            "return_value_signed": [0.0, 0.0, 0.0, 0.0],
            "return_value": [0.0, 0.0, 0.0, 0.0],
            "net_qty": [1.0, 1.0, 1.0, 1.0],
            "net_revenue": [1000.0, 1100.0, 900.0, 950.0],
            "tax": [180.0, 198.0, 162.0, 171.0],
            "order_id": ["ORD-10", "ORD-11", "ORD-12", "ORD-13"],
            "return_reason": ["None", "None", "None", "None"],
            "return_validity": ["Valid", "Valid", "Valid", "Valid"],
            "fy": ["FY2025-26", "FY2025-26", "FY2025-26", "FY2025-26"],
            "month": ["2025-04", "2025-04", "2025-04", "2025-04"],
            "weekday": ["Thursday", "Friday", "Saturday", "Sunday"],
        }
    )
    return frame[app_api.SNAPSHOT_COLUMNS]


def sample_orderhub_snapshot_csv() -> bytes:
    return sample_snapshot_frame().assign(order_date=lambda frame: frame["order_date"].dt.strftime("%Y-%m-%d")).to_csv(index=False).encode("utf-8")


def sample_orderhub_bridge_csv() -> bytes:
    return pd.DataFrame(
        [
            {
                "order_date": "2025-01-01",
                "platform_raw": "Amazon Online Sale",
                "category": "Beds",
                "product": "Alpha Bed",
                "sku": "ALPHA-1",
                "sale_qty": 2,
                "return_qty_signed": -1,
                "gross_sales": 1000,
                "return_value_signed": -200,
                "tax": 180,
                "order_id": "ORD-1",
                "return_reason": "Damaged",
                "return_validity": "Valid",
            }
        ]
    ).to_csv(index=False).encode("utf-8")


def sample_non_merch_snapshot_frame() -> pd.DataFrame:
    frame = pd.DataFrame(
        {
            "order_date": pd.to_datetime(["2025-02-01", "2025-02-02", "2025-02-03", "2025-02-04"]),
            "platform_raw": ["Amazon Online Sale", "Amazon Online Sale", "Amazon Online Sale", "Amazon Online Sale"],
            "platform_label": ["Amazon", "Amazon", "Amazon", "Amazon"],
            "category": ["Misc", "Hardware", "Tables", "Adjustments"],
            "product": ["Scrap Polythene", "Minifix Housing 12x15", "Bluewud Study Table", "Post Sales Discount@18%"],
            "sku": ["SCRAP-1", "MINI-1", "TABLE-1", "DISC-1"],
            "sale_qty": [10.0, 9.0, 8.0, 0.0],
            "return_qty_signed": [0.0, 0.0, 0.0, -1.0],
            "return_qty": [0.0, 0.0, 0.0, 1.0],
            "gross_sales": [100.0, 200.0, 8000.0, 0.0],
            "return_value_signed": [0.0, 0.0, 0.0, -500.0],
            "return_value": [0.0, 0.0, 0.0, 500.0],
            "net_qty": [10.0, 9.0, 8.0, -1.0],
            "net_revenue": [100.0, 200.0, 8000.0, -500.0],
            "tax": [0.0, 0.0, 1440.0, 0.0],
            "order_id": ["ORD-S1", "ORD-M1", "ORD-T1", "ORD-D1"],
            "return_reason": ["None", "None", "None", "Adjustment"],
            "return_validity": ["Valid", "Valid", "Valid", "Valid"],
            "fy": ["FY2024-25", "FY2024-25", "FY2024-25", "FY2024-25"],
            "month": ["2025-02", "2025-02", "2025-02", "2025-02"],
            "weekday": ["Saturday", "Sunday", "Monday", "Tuesday"],
        }
    )
    return frame[app_api.SNAPSHOT_COLUMNS]


def sample_return_only_snapshot_frame() -> pd.DataFrame:
    frame = pd.DataFrame(
        {
            "order_date": pd.to_datetime(["2025-01-11", "2025-01-17"]),
            "platform_raw": ["Amazon Online Sale", "Amazon Online Sale"],
            "platform_label": ["Amazon", "Amazon"],
            "category": ["Shelves", "TV Units"],
            "product": ["Bluewud Seonn Bookshelf", "Post Sales Discount@18%"],
            "sku": ["SEONN-1", "DISC-1"],
            "sale_qty": [0.0, 0.0],
            "return_qty_signed": [-1.0, -1.0],
            "return_qty": [1.0, 1.0],
            "gross_sales": [0.0, 0.0],
            "return_value_signed": [-3942.0, -206.0],
            "return_value": [3942.0, 206.0],
            "net_qty": [-1.0, -1.0],
            "net_revenue": [-3942.0, -206.0],
            "tax": [0.0, 0.0],
            "order_id": ["ORD-R1", "ORD-R2"],
            "return_reason": ["Returned", "Adjustment"],
            "return_validity": ["Valid", "Valid"],
            "fy": ["FY2024-25", "FY2024-25"],
            "month": ["2025-01", "2025-01"],
            "weekday": ["Saturday", "Friday"],
        }
    )
    return frame[app_api.SNAPSHOT_COLUMNS]


def sample_invalid_platform_snapshot_frame() -> pd.DataFrame:
    frame = pd.DataFrame(
        {
            "order_date": pd.to_datetime(["2025-02-01", "2025-02-02", "2025-02-03"]),
            "platform_raw": ["Amazon Online Sale", "Misc", "Unknown Platform"],
            "platform_label": ["Amazon", "Misc", "Unknown Platform"],
            "category": ["Beds", "Beds", "Beds"],
            "product": ["Alpha Bed", "Beta Bed", "Ghost Bed"],
            "sku": ["ALPHA-1", "BETA-1", "GHOST-1"],
            "sale_qty": [1.0, 2.0, 0.0],
            "return_qty_signed": [0.0, 0.0, 0.0],
            "return_qty": [0.0, 0.0, 0.0],
            "gross_sales": [1000.0, 500.0, 0.0],
            "return_value_signed": [0.0, 0.0, 0.0],
            "return_value": [0.0, 0.0, 0.0],
            "net_qty": [1.0, 2.0, 0.0],
            "net_revenue": [1000.0, 500.0, 0.0],
            "tax": [180.0, 90.0, 0.0],
            "order_id": ["ORD-1", "ORD-2", None],
            "return_reason": ["None", "None", "None"],
            "return_validity": ["Valid", "Valid", "Unknown"],
            "fy": ["FY2024-25", "FY2024-25", "FY2024-25"],
            "month": ["2025-02", "2025-02", "2025-02"],
            "weekday": ["Saturday", "Sunday", "Monday"],
        }
    )
    return frame[app_api.SNAPSHOT_COLUMNS]


def build_manager(frame: pd.DataFrame, summary_sheet: dict | None = None) -> app_api.DataManager:
    manager = app_api.DataManager.__new__(app_api.DataManager)
    manager._df = frame.copy()
    manager._loaded_at = pd.Timestamp("2026-03-12").to_pydatetime()
    manager._source = "https://example.com/data.xlsx"
    manager._source_type = "url"
    manager._summary_sheet = summary_sheet or {}
    manager._load_error = None
    manager._data_version = "test"
    return manager


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
        .query("orders > 0")
        .reset_index()
        .set_index("platform_raw")["orders"]
        .to_dict()
    )
    actual_orders = {row["platform"]: row["orders"] for row in dm.platform_data(df)}

    assert actual_orders == expected_orders
    assert actual_orders["Amazon Online Sale"] == 72113
    assert actual_orders["Flipkart Online Sale"] == 13386


def test_top_products_volume_excludes_non_merchandise_names() -> None:
    manager = build_manager(sample_non_merch_snapshot_frame())

    volume = manager.top_products(manager.apply_filters({}), metric="volume", n=10)
    revenue = manager.top_products(manager.apply_filters({}), metric="revenue", n=10)

    assert [item["product"] for item in volume] == ["Bluewud Study Table"]
    assert [item["product"] for item in revenue] == ["Bluewud Study Table"]


def test_return_only_slice_is_flagged_in_kpis() -> None:
    manager = build_manager(sample_return_only_snapshot_frame())

    kpis = manager.kpis(manager.apply_filters({}))

    assert kpis["gross_sales"] == 0.0
    assert kpis["return_value"] == pytest.approx(4148.0)
    assert kpis["sales_rows"] == 0
    assert kpis["return_rows"] == 2
    assert kpis["return_only_rows"] == 2
    assert kpis["has_sales_activity"] is False


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
    assert "Sales Trends Dashboard" in response.text
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


def test_dashboard_trend_mode_override_is_honored(client: TestClient) -> None:
    response = client.get(
        "/api/dashboard",
        params={
            "start_date": "2025-01-01",
            "end_date": "2025-02-28",
            "trend_mode": "monthly",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["trend"]["requested_frequency"] == "monthly"
    assert payload["trend"]["frequency"] == "monthly"


def test_summary_sheet_endpoint_returns_dict(client: TestClient) -> None:
    response = client.get("/api/summary-sheet")

    assert response.status_code == 200
    payload = response.json()
    assert isinstance(payload, dict)


def test_filters_endpoint_exposes_trend_modes(client: TestClient) -> None:
    response = client.get("/api/filters")

    assert response.status_code == 200
    payload = response.json()
    assert payload["default_trend_mode"] == "auto"
    assert [item["value"] for item in payload["trend_modes"]] == ["auto", "daily", "weekly", "monthly"]


def test_summary_sheet_fallback_populates_when_workbook_sheet_missing() -> None:
    manager = build_manager(sample_fy_snapshot_frame(), summary_sheet={})

    payload = manager.summary_sheet()

    assert payload["meta"]["mode"] == "computed"
    assert payload["meta"]["budget_available"] is False
    assert len(payload["headline_cards"]) >= 10
    assert payload["monthly_fy_sales"]
    assert payload["channel_growth"]
    assert payload["budget_vs_achievement"]
    assert payload["channel_performance_current"]
    assert payload["rto_monthly_current"]
    assert payload["insights"]
    assert any(card["label"].startswith("Total revenue") for card in payload["headline_cards"])


def test_summary_sheet_for_filtered_scope_uses_computed_view() -> None:
    workbook_summary = {
        "headline_cards": [{"label": "Workbook card", "value": "Workbook total"}],
        "monthly_fy_sales": [{"month": "Apr", "fy_2025_26": 999999}],
        "channel_performance_current": [],
        "budget_vs_achievement": [],
        "rto_monthly_current": [],
        "channel_growth": [],
        "insights": [],
    }
    manager = build_manager(sample_fy_snapshot_frame(), summary_sheet=workbook_summary)
    filters = {"product_query": "Alpha Bed"}
    scoped_df = manager.apply_filters(filters)

    payload = manager.summary_sheet_for(filters, scoped_df)

    assert payload["meta"]["mode"] == "computed"
    assert payload["headline_cards"][0]["value"] != "Workbook total"
    assert payload["monthly_fy_sales"][0]["fy_2025_26"] != 999999


def test_dynamic_insights_expand_commercial_explanations() -> None:
    manager = build_manager(sample_fy_snapshot_frame(), summary_sheet={})

    insights = manager.dynamic_insights(sample_fy_snapshot_frame())

    assert len(insights) >= 5
    assert any(item.get("metric") for item in insights)
    assert {item["title"] for item in insights} >= {
        "Revenue converted to net",
        "Category leader",
        "Hero product",
    }


def test_product_query_matches_sku_prefix_and_compact_forms() -> None:
    manager = build_manager(sample_sku_search_snapshot_frame(), summary_sheet={})

    prefixed = manager.apply_filters({"product_query": "ST-CBN-LSMF"})
    compact = manager.apply_filters({"product_query": "STCBNLSMF"})
    family = manager.apply_filters({"product_query": "SR-KPN"})
    enriched = manager._ensure_search_dimensions(manager._df.copy())
    variant_row = enriched.loc[enriched["sku"] == "ST-CBN-LSMF-CL"].iloc[0]

    assert set(prefixed["sku"]) == {"ST-CBN-LSMF", "ST-CBN-LSMF-CL"}
    assert set(compact["sku"]) == {"ST-CBN-LSMF", "ST-CBN-LSMF-CL"}
    assert set(family["sku"]) == {"SR-KPN-FW", "SR-KPN-MF"}
    assert variant_row["sku_base"] == "ST-CBN-LSMF"
    assert variant_row["sku_extension"] == "CL"


def test_search_products_returns_sku_and_product_suggestions() -> None:
    manager = build_manager(sample_sku_search_snapshot_frame(), summary_sheet={})

    sku_suggestions = manager.search_products({}, "ST-CBN-LSMF")
    product_suggestions = manager.search_products({}, "Kaspen")

    assert sku_suggestions[:2] == ["ST-CBN-LSMF", "ST-CBN-LSMF-CL"]
    assert "Bluewud Corbyn L Shape Study Table-Maple" in sku_suggestions
    assert {
        "Bluewud Kaspen Shoe Rack Maple (MF)",
        "Bluewud Kaspen Shoe Rack Wenge(FW)",
    }.issubset(set(product_suggestions))


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


def test_order_hub_snapshot_loader_reads_csv(monkeypatch: pytest.MonkeyPatch) -> None:
    manager = build_manager(sample_snapshot_frame(), summary_sheet={})

    class FakeResponse:
        headers = {"content-type": "text/csv"}
        content = sample_orderhub_snapshot_csv()

        def raise_for_status(self) -> None:
            return None

    monkeypatch.setattr(app_api.http_requests, "get", lambda *args, **kwargs: FakeResponse())

    df, summary = manager._load_order_hub_snapshot("https://orderhub.example.com")

    assert summary == {}
    assert len(df) == 1
    assert list(df.columns) == app_api.SNAPSHOT_COLUMNS
    assert df.iloc[0]["platform_raw"] == "Amazon Online Sale"
    assert df.iloc[0]["order_date"].strftime("%Y-%m-%d") == "2025-01-01"


def test_order_hub_snapshot_loader_expands_minimal_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    manager = build_manager(sample_snapshot_frame(), summary_sheet={})

    class FakeResponse:
        headers = {"content-type": "text/csv"}
        content = sample_orderhub_bridge_csv()

        def raise_for_status(self) -> None:
            return None

    monkeypatch.setattr(app_api.http_requests, "get", lambda *args, **kwargs: FakeResponse())

    df, _ = manager._load_order_hub_snapshot("https://orderhub.example.com")

    row = df.iloc[0]
    assert row["platform_label"] == "Amazon"
    assert row["return_qty"] == pytest.approx(1.0)
    assert row["return_value"] == pytest.approx(200.0)
    assert row["net_qty"] == pytest.approx(1.0)
    assert row["net_revenue"] == pytest.approx(800.0)
    assert row["fy"] == "FY2024-25"
    assert row["month"] == "2025-01"
    assert row["weekday"] == "Wednesday"


def test_canonical_platform_raw_maps_invalid_values_to_misc() -> None:
    assert app_api.canonical_platform_raw("NA") == "Misc"
    assert app_api.canonical_platform_raw("Order ID") == "Misc"
    assert app_api.canonical_platform_raw("ORD-12345") == "Misc"
    assert app_api.canonical_platform_raw("Amazon Online Sale") == "Amazon Online Sale"


def test_filter_options_hide_empty_unknown_platforms() -> None:
    manager = build_manager(sample_invalid_platform_snapshot_frame(), summary_sheet={})

    platforms = manager.filter_options()["platforms"]

    assert [row["value"] for row in platforms] == ["Amazon Online Sale", "Misc"]
    assert [row["label"] for row in platforms] == ["Amazon", "Misc"]


def test_order_hub_snapshot_loader_rejects_missing_required_columns(monkeypatch: pytest.MonkeyPatch) -> None:
    manager = build_manager(sample_snapshot_frame(), summary_sheet={})

    broken_csv = pd.DataFrame(
        [
            {
                "order_date": "2025-01-01",
                "platform_raw": "Amazon Online Sale",
                "category": "Beds",
                "product": "Alpha Bed",
                "sku": "ALPHA-1",
                "sale_qty": 2,
                "return_qty_signed": -1,
                "gross_sales": 1000,
                "return_value_signed": -200,
                "tax": 180,
                "order_id": "ORD-1",
                "return_reason": "Damaged",
            }
        ]
    ).to_csv(index=False).encode("utf-8")

    class FakeResponse:
        headers = {"content-type": "text/csv"}
        content = broken_csv

        def raise_for_status(self) -> None:
            return None

    monkeypatch.setattr(app_api.http_requests, "get", lambda *args, **kwargs: FakeResponse())

    with pytest.raises(ValueError, match="missing required columns: return_validity"):
        manager._load_order_hub_snapshot("https://orderhub.example.com")


def test_refresh_uses_order_hub_before_local_sources(monkeypatch: pytest.MonkeyPatch) -> None:
    manager = app_api.DataManager.__new__(app_api.DataManager)
    manager._df = sample_snapshot_frame()
    manager._loaded_at = pd.Timestamp("2026-03-12").to_pydatetime()
    manager._source = "seed"
    manager._source_type = "local"
    manager._summary_sheet = {}
    manager._load_error = None
    manager._data_version = "seed"

    calls: list[object] = []

    def fake_snapshot(self: app_api.DataManager) -> bool:
        calls.append("snapshot")
        return False

    def fake_order_hub(self: app_api.DataManager, base_url: str):
        calls.append(("order_hub", base_url))
        return sample_snapshot_frame(), {}

    def fake_local(self: app_api.DataManager, _: str):
        calls.append("local")
        raise AssertionError("Local fallback should not run after a successful OrderHub load.")

    def fake_write_snapshot(self: app_api.DataManager) -> None:
        calls.append("write")

    monkeypatch.setattr(app_api.DataManager, "_load_snapshot", fake_snapshot)
    monkeypatch.setattr(app_api.DataManager, "_load_order_hub_snapshot", fake_order_hub)
    monkeypatch.setattr(app_api.DataManager, "_load_local", fake_local)
    monkeypatch.setattr(app_api, "ORDER_HUB_BASE_URL", "https://orderhub.example.com")
    monkeypatch.setattr(app_api, "DATA_URL", "")
    monkeypatch.setattr(app_api, "GITHUB_TOKEN", "")
    monkeypatch.setattr(app_api, "GITHUB_REPO", "")
    monkeypatch.setattr(app_api.DataManager, "_write_snapshot", fake_write_snapshot)

    result = manager.refresh_current_source()

    assert result["source"] == "https://orderhub.example.com"
    assert "snapshot" not in calls
    assert ("order_hub", "https://orderhub.example.com") in calls
    assert "local" not in calls
    assert "write" in calls


def test_invalid_order_hub_contract_falls_back_to_local_source(monkeypatch: pytest.MonkeyPatch) -> None:
    manager = app_api.DataManager.__new__(app_api.DataManager)
    manager._df = sample_snapshot_frame()
    manager._loaded_at = pd.Timestamp("2026-03-12").to_pydatetime()
    manager._source = "seed"
    manager._source_type = "local"
    manager._summary_sheet = {}
    manager._load_error = None
    manager._data_version = "seed"

    calls: list[object] = []

    def fake_order_hub(self: app_api.DataManager, base_url: str):
        calls.append(("order_hub", base_url))
        raise ValueError("OrderHub analytics snapshot is missing required columns: return_validity")

    def fake_local(self: app_api.DataManager, _: str):
        calls.append("local")
        return sample_raw_workbook(), {}

    def fake_write_snapshot(self: app_api.DataManager) -> None:
        calls.append("write")

    monkeypatch.setattr(app_api.DataManager, "_load_order_hub_snapshot", fake_order_hub)
    monkeypatch.setattr(app_api.DataManager, "_load_local", fake_local)
    monkeypatch.setattr(app_api, "ORDER_HUB_BASE_URL", "https://orderhub.example.com")
    monkeypatch.setattr(app_api, "DATA_URL", "")
    monkeypatch.setattr(app_api, "GITHUB_TOKEN", "")
    monkeypatch.setattr(app_api, "GITHUB_REPO", "")
    monkeypatch.setattr(app_api.DataManager, "_write_snapshot", fake_write_snapshot)

    result = manager.refresh_current_source()

    assert calls == [("order_hub", "https://orderhub.example.com"), "local", "write"]
    assert result["source"] == "data.xlsx"
    assert result["source_type"] == "local"
    assert result["rows"] == 1


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


def test_load_order_hub_source_uses_explicit_order_hub_loader(monkeypatch: pytest.MonkeyPatch) -> None:
    manager = app_api.DataManager.__new__(app_api.DataManager)
    manager._df = sample_snapshot_frame()
    manager._loaded_at = pd.Timestamp("2026-03-12").to_pydatetime()
    manager._source = "https://example.com/data.xlsx"
    manager._source_type = "url"
    manager._summary_sheet = {}
    manager._load_error = None
    manager._data_version = "seed"

    calls: list[object] = []

    def fake_order_hub(self: app_api.DataManager, base_url: str):
        calls.append(("order_hub", base_url))
        return sample_snapshot_frame(), {}

    def fake_write_snapshot(self: app_api.DataManager) -> None:
        calls.append("write")

    monkeypatch.setattr(app_api.DataManager, "_load_order_hub_snapshot", fake_order_hub)
    monkeypatch.setattr(app_api.DataManager, "_write_snapshot", fake_write_snapshot)
    monkeypatch.setattr(app_api, "ORDER_HUB_BASE_URL", "https://orderhub.example.com")

    result = manager.load_order_hub_source()

    assert result["source"] == "https://orderhub.example.com"
    assert result["source_type"] == "order_hub"
    assert calls == [("order_hub", "https://orderhub.example.com"), "write"]


def test_reset_to_default_source_ignores_previous_url_override(monkeypatch: pytest.MonkeyPatch) -> None:
    manager = app_api.DataManager.__new__(app_api.DataManager)
    manager._df = sample_snapshot_frame()
    manager._loaded_at = pd.Timestamp("2026-03-12").to_pydatetime()
    manager._source = "https://example.com/custom.xlsx"
    manager._source_type = "url"
    manager._summary_sheet = {}
    manager._load_error = None
    manager._data_version = "seed"

    calls: list[object] = []

    def fake_local(self: app_api.DataManager, path: str):
        calls.append(("local", path))
        return sample_raw_workbook(), {}

    def fake_remote(self: app_api.DataManager, url: str):
        calls.append(("url", url))
        raise AssertionError("URL override should not be reused after reset.")

    def fake_write_snapshot(self: app_api.DataManager) -> None:
        calls.append("write")

    monkeypatch.setattr(app_api.DataManager, "_load_local", fake_local)
    monkeypatch.setattr(app_api.DataManager, "_load_remote_excel", fake_remote)
    monkeypatch.setattr(app_api.DataManager, "_write_snapshot", fake_write_snapshot)
    monkeypatch.setattr(app_api, "ORDER_HUB_BASE_URL", "")
    monkeypatch.setattr(app_api, "DATA_URL", "")
    monkeypatch.setattr(app_api, "GITHUB_TOKEN", "")
    monkeypatch.setattr(app_api, "GITHUB_REPO", "")
    monkeypatch.setattr(app_api, "DATA_FILE", "data.xlsx")

    result = manager.reset_to_default_source()

    assert result["source"] == "data.xlsx"
    assert result["source_type"] == "local"
    assert calls == [("local", "data.xlsx"), "write"]


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
