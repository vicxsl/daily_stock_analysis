# -*- coding: utf-8 -*-
"""T01/T02/T03 — price-time semantics, OTC NAV routing, partial valuation.

Offline-only (D14): all quote sources are monkeypatched; no network.
"""

from __future__ import annotations

import os
import tempfile
import unittest
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from src.config import Config
from src.repositories.portfolio_repo import PortfolioRepository
from src.services import fund_nav
from src.services import portfolio_service as ps
from src.services.portfolio_service import PortfolioService
from src.storage import DatabaseManager

CN_TZ = ps._CN_TZ


def cn_dt(day: date, hour: int, minute: int) -> datetime:
    return datetime(day.year, day.month, day.day, hour, minute, tzinfo=CN_TZ)


class PriceSemanticsTestCase(unittest.TestCase):
    """Boundary tests for _build_realtime_resolved_price (T02)."""

    def setUp(self) -> None:
        self.service = PortfolioService.__new__(PortfolioService)
        self.as_of = date(2026, 10, 9)  # Friday

    def _resolve(self, quote_info, now_cn):
        return self.service._build_realtime_resolved_price(
            symbol="600887", as_of_date=self.as_of, quote_info=quote_info, now_cn=now_cn
        )

    def _quote(self, provider_time, fetched_at=None, price=25.5):
        return {
            "price": price,
            "provider": "tencent",
            "kind": "realtime",
            "provider_time": provider_time,
            "fetched_at": fetched_at or provider_time,
        }

    @staticmethod
    def _iso(dt: datetime) -> str:
        return dt.isoformat()

    def test_same_day_quote_fresh_inside_session(self):
        now = cn_dt(self.as_of, 10, 0)
        provider = now - timedelta(minutes=30)
        r = self._resolve(self._quote(self._iso(provider), self._iso(now)), now)
        self.assertFalse(r.is_stale)
        self.assertEqual(r.price_date, self.as_of)
        self.assertEqual(r.time_quality, "valid")

    def test_same_day_quote_61min_old_inside_session_is_stale(self):
        now = cn_dt(self.as_of, 10, 31)
        provider = now - timedelta(minutes=61)
        r = self._resolve(self._quote(self._iso(provider), self._iso(now)), now)
        self.assertTrue(r.is_stale)
        self.assertEqual(r.price_date, self.as_of)

    def test_same_day_quote_59min_old_inside_session_not_stale(self):
        now = cn_dt(self.as_of, 10, 29)
        provider = now - timedelta(minutes=59)
        r = self._resolve(self._quote(self._iso(provider), self._iso(now)), now)
        self.assertFalse(r.is_stale)

    def test_same_day_official_close_after_session_not_stale(self):
        now = cn_dt(self.as_of, 16, 0)
        provider = cn_dt(self.as_of, 15, 0)
        r = self._resolve(self._quote(self._iso(provider), self._iso(now)), now)
        self.assertFalse(r.is_stale)
        self.assertEqual(r.price_date, self.as_of)

    def test_previous_day_quote_after_open_is_stale(self):
        now = cn_dt(self.as_of, 10, 0)
        provider = cn_dt(self.as_of - timedelta(days=1), 15, 0)
        r = self._resolve(self._quote(self._iso(provider), self._iso(now)), now)
        self.assertTrue(r.is_stale)
        # price_date must stay honest: the provider's date, not today.
        self.assertEqual(r.price_date, self.as_of - timedelta(days=1))

    def test_previous_day_quote_premarket_not_stale(self):
        now = cn_dt(self.as_of, 8, 0)
        provider = cn_dt(self.as_of - timedelta(days=1), 15, 0)
        r = self._resolve(self._quote(self._iso(provider), self._iso(now)), now)
        self.assertFalse(r.is_stale)
        self.assertEqual(r.price_date, self.as_of - timedelta(days=1))

    def test_weekend_previous_close_not_stale(self):
        saturday = date(2026, 10, 10)
        now = cn_dt(saturday, 12, 0)
        provider = cn_dt(date(2026, 10, 9), 15, 0)
        r = self.service._build_realtime_resolved_price(
            symbol="600887",
            as_of_date=saturday,
            quote_info=self._quote(self._iso(provider), self._iso(now)),
            now_cn=now,
        )
        self.assertFalse(r.is_stale)
        self.assertEqual(r.price_date, date(2026, 10, 9))

    def test_missing_provider_time_marks_unknown(self):
        now = cn_dt(self.as_of, 10, 0)
        r = self._resolve(
            {"price": 25.5, "provider": "x", "kind": "realtime",
             "provider_time": None, "fetched_at": self._iso(now)},
            now,
        )
        self.assertEqual(r.time_quality, "unknown")
        # R3: 缺源时刻不填获取日期 — price_date 保持未知（None），不伪称今天。
        self.assertIsNone(r.price_date)

    def test_nav_t1_not_stale_and_carries_nav_date(self):
        now = cn_dt(self.as_of, 20, 0)
        nav_date = self.as_of - timedelta(days=1)
        r = self.service._build_realtime_resolved_price(
            symbol="003096",
            as_of_date=self.as_of,
            quote_info={
                "price": 1.7675,
                "provider": "akshare_fund_nav",
                "kind": "nav",
                "provider_time": cn_dt(nav_date, 15, 0).isoformat(),
                "fetched_at": self._iso(now),
            },
            now_cn=now,
        )
        self.assertEqual(r.price_kind, "nav")
        self.assertEqual(r.price_date, nav_date)
        self.assertFalse(r.is_stale)
        self.assertEqual(r.time_quality, "valid")

    def test_nav_lagging_over_7_days_is_stale(self):
        now = cn_dt(self.as_of, 20, 0)
        nav_date = self.as_of - timedelta(days=12)
        r = self.service._build_realtime_resolved_price(
            symbol="003096",
            as_of_date=self.as_of,
            quote_info={
                "price": 1.7675,
                "provider": "akshare_fund_nav",
                "kind": "nav",
                "provider_time": cn_dt(nav_date, 15, 0).isoformat(),
                "fetched_at": self._iso(now),
            },
            now_cn=now,
        )
        self.assertTrue(r.is_stale)


class SnapshotValuationTestCase(unittest.TestCase):
    """Partial valuation coverage + OTC routing through the snapshot path."""

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.env_path = Path(self.temp_dir.name) / ".env"
        self.db_path = Path(self.temp_dir.name) / "portfolio_test.db"
        self.env_path.write_text(
            "\n".join(
                [
                    "STOCK_LIST=600519",
                    "GEMINI_API_KEY=test",
                    "ADMIN_AUTH_ENABLED=false",
                    f"DATABASE_PATH={self.db_path}",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        os.environ["ENV_FILE"] = str(self.env_path)
        os.environ["DATABASE_PATH"] = str(self.db_path)
        Config.reset_instance()
        DatabaseManager.reset_instance()
        self.db = DatabaseManager.get_instance()
        self.service = PortfolioService()
        self.account_id = self.service.create_account(name="TEST", broker="TEST", market="cn", base_currency="CNY")["id"]

    def tearDown(self) -> None:
        DatabaseManager.reset_instance()
        Config.reset_instance()
        os.environ.pop("ENV_FILE", None)
        os.environ.pop("DATABASE_PATH", None)
        self.temp_dir.cleanup()

    def _snapshot(self, include_realtime=True):
        return self.service.get_portfolio_snapshot(
            account_id=self.account_id,
            as_of=date.today(),
            cost_method="fifo",
            include_realtime=include_realtime,
        )

    def test_missing_price_is_unvalued_not_zero_valued(self):
        self.service.record_trade(
            account_id=self.account_id,
            symbol="600887",
            trade_date=date.today(),
            side="buy",
            quantity=300,
            price=26.0,
        )
        # No price source at all: realtime monkeypatched to None, no history.
        with patch.object(
            PortfolioService, "_fetch_realtime_position_price", staticmethod(lambda symbol: None)
        ):
            snap = self._snapshot()
        account = snap["accounts"][0]
        position = account["positions"][0]
        self.assertEqual(len(account["positions"]), 1)
        self.assertFalse(position["price_available"])
        self.assertEqual(account["valuation_coverage"]["valued"], 0)
        self.assertEqual(account["valuation_coverage"]["total"], 1)
        self.assertIn("600887", account["valuation_coverage"]["unvalued_symbols"])
        self.assertTrue(
            any("price unavailable" in item for item in account["limitations"]),
            account["limitations"],
        )
        # Shares and cost stay visible; valuation contributes zero.
        self.assertEqual(position["quantity"], 300)
        self.assertEqual(position["total_cost"], 7800.0)
        self.assertEqual(position["market_value_base"], 0.0)

    def test_partial_valuation_mixes_valued_and_unvalued(self):
        self.service.record_trade(
            account_id=self.account_id, symbol="600887", trade_date=date.today(),
            side="buy", quantity=300, price=26.0,
        )
        self.service.record_trade(
            account_id=self.account_id, symbol="601899", trade_date=date.today(),
            side="buy", quantity=500, price=32.0,
        )
        quotes = {
            "600887": {
                "price": 25.0, "provider": "tencent", "kind": "realtime",
                "provider_time": datetime.now(CN_TZ).isoformat(),
                "fetched_at": datetime.now(CN_TZ).isoformat(),
            },
        }

        def fake_prefetch(*args, **kwargs):
            symbols = args[-1] if args else kwargs.get("symbols")
            return {s: quotes[s] for s in symbols if s in quotes}

        with patch.object(PortfolioService, "_prefetch_realtime_position_prices", fake_prefetch):
            snap = self._snapshot()
        account = snap["accounts"][0]
        self.assertEqual(account["valuation_coverage"]["valued"], 1)
        self.assertEqual(account["valuation_coverage"]["total"], 2)
        self.assertIn("601899", account["valuation_coverage"]["unvalued_symbols"])
        # Only the valued position contributes to market value.
        self.assertAlmostEqual(account["total_market_value"], 300 * 25.0, places=4)

    def test_otc_fund_routes_to_nav_never_stock_quote(self):
        nav_date = date.today() - timedelta(days=1)
        # Patch the module-level helpers that production routing calls; the
        # stock DataFetcherManager must never be imported on this path.
        with patch.object(ps, "classify_cn_symbol", lambda symbol: "otc"), \
             patch.object(
                 ps, "fetch_latest_nav",
                 lambda symbol: {"nav": 1.8012, "nav_date": nav_date},
             ):
            info = PortfolioService._fetch_realtime_position_price("003096")
        self.assertIsNotNone(info)
        self.assertEqual(info["kind"], "nav")
        self.assertEqual(info["provider"], "akshare_fund_nav")
        self.assertAlmostEqual(info["price"], 1.8012, places=8)
        self.assertTrue(str(nav_date) in info["provider_time"])

        # End-to-end through the snapshot: NAV semantics surface on the position.
        self.service.record_trade(
            account_id=self.account_id, symbol="003096", trade_date=date.today(),
            side="buy", quantity=4088.71, price=1.7675,
        )
        with patch.object(ps, "classify_cn_symbol", lambda symbol: "otc"), \
             patch.object(
                 ps, "fetch_latest_nav",
                 lambda symbol: {"nav": 1.8012, "nav_date": nav_date},
             ):
            snap = self._snapshot()
        account = snap["accounts"][0]
        position = account["positions"][0]
        self.assertEqual(position["price_kind"], "nav")
        self.assertEqual(position["price_source"], "fund_nav")
        self.assertEqual(position["price_date"], nav_date.isoformat())
        self.assertAlmostEqual(position["last_price"], 1.8012, places=6)
        self.assertFalse(position["price_stale"])

    def test_listed_symbol_keeps_stock_quote_path(self):
        # A prefixed listed symbol classifies as listed and never reaches NAV.
        self.assertEqual(ps.classify_cn_symbol("SZ600887"), "listed")
        self.assertEqual(ps.classify_cn_symbol("600887.SH"), "listed")
        with patch.object(ps, "classify_cn_symbol", lambda symbol: "listed"), \
             patch.object(ps, "_fetch_stock_realtime_quote", lambda symbol, ts: None), \
             patch.object(ps, "fetch_latest_nav") as nav_mock:
            info = PortfolioService._fetch_realtime_position_price("SZ600887")
        self.assertIsNone(info)
        nav_mock.assert_not_called()

    def test_etf_registry_hit_stays_listed_never_nav(self):
        # 2026-10-09 live incident: pre-market 159632 (exchange ETF) fell back
        # to the OTC twin's NAV when the quote source returned nothing. ETF/LOF
        # range registry hits must classify as listed — NAV never substitutes.
        with patch.object(ps, "classify_cn_symbol", lambda symbol: "listed"),              patch.object(ps, "_fetch_stock_realtime_quote", lambda symbol, ts: None),              patch.object(ps, "fetch_latest_nav") as nav_mock:
            info = PortfolioService._fetch_realtime_position_price("159632")
        self.assertIsNone(info)
        nav_mock.assert_not_called()

    def test_ambiguous_unconfirmed_is_paused_never_guessed(self):
        # R3/F06: 003096 sits in the SZ listed range AND the OTC registry —
        # identity is ambiguous. Without a confirmed type the symbol's valuation
        # is PAUSED: no stock-quote-then-NAV guessing, no history-close
        # substitute for a live valuation.
        with patch.object(ps, "classify_cn_symbol", lambda symbol: "ambiguous"), \
             patch.object(ps, "_fetch_stock_realtime_quote") as stock_mock, \
             patch.object(ps, "fetch_latest_nav") as nav_mock:
            info = PortfolioService._fetch_realtime_position_price("003096")
        self.assertEqual(info["kind"], "identity_paused")
        stock_mock.assert_not_called()
        nav_mock.assert_not_called()

        # End-to-end: the position stays unvalued with an explicit reason and
        # never falls back to a history close.
        self.service.record_trade(
            account_id=self.account_id, symbol="003096", trade_date=date.today(),
            side="buy", quantity=4088.71, price=1.7675,
        )
        with patch.object(ps, "classify_cn_symbol", lambda symbol: "ambiguous"), \
             patch.object(ps, "_fetch_stock_realtime_quote") as stock_mock, \
             patch.object(ps, "fetch_latest_nav") as nav_mock:
            snap = self._snapshot()
        account = snap["accounts"][0]
        position = account["positions"][0]
        self.assertEqual(position["price_source"], "identity_unconfirmed")
        self.assertEqual(position["price_kind"], "identity_paused")
        self.assertFalse(position["price_available"])
        self.assertIn("003096", account["valuation_coverage"]["unvalued_symbols"])
        self.assertTrue(
            any("identity unconfirmed" in item for item in position["limitations"])
        )
        stock_mock.assert_not_called()
        nav_mock.assert_not_called()

    def test_confirmed_identity_routes_without_guessing(self):
        # A CONFIRMED identity (R3 registry) routes deterministically: fund →
        # NAV even for an ambiguous-range code; stock → exchange, never NAV.
        from src.services.instrument_identity import InstrumentIdentityService

        identity = InstrumentIdentityService()
        identity.upsert(symbol="003096", kind="otc_fund", source="test")
        nav_date = date.today() - timedelta(days=1)
        with patch.object(ps, "classify_cn_symbol", lambda symbol: "ambiguous"), \
             patch.object(ps, "_fetch_stock_realtime_quote") as stock_mock, \
             patch.object(ps, "fetch_latest_nav",
                          lambda symbol: {"nav": 1.7675, "nav_date": nav_date}):
            info = PortfolioService._fetch_realtime_position_price("003096")
        self.assertEqual(info["kind"], "nav")
        stock_mock.assert_not_called()

        identity.upsert(symbol="003096", kind="stock", source="test")
        with patch.object(ps, "classify_cn_symbol", lambda symbol: "ambiguous"), \
             patch.object(ps, "_fetch_stock_realtime_quote",
                          lambda symbol, ts: {"price": 26.0, "provider": "tencent",
                                              "kind": "realtime", "provider_time": None,
                                              "fetched_at": ts}), \
             patch.object(ps, "fetch_latest_nav") as nav_mock:
            info = PortfolioService._fetch_realtime_position_price("003096")
        self.assertEqual(info["kind"], "realtime")
        nav_mock.assert_not_called()

    def test_identity_aliases_resolve_to_one_instrument(self):
        # F06: 600887 / 600887.SH / SH600887 point at the same registry row.
        from src.services.instrument_identity import (
            InstrumentIdentityService,
            normalize_identity_symbol,
        )

        self.assertEqual(normalize_identity_symbol("SH600887"), "600887")
        self.assertEqual(normalize_identity_symbol("600887.SH"), "600887")
        identity = InstrumentIdentityService()
        identity.upsert(symbol="SH600887", kind="stock", source="test")
        self.assertEqual(identity.confirmed_kind("600887.SH"), "stock")
        self.assertEqual(identity.confirmed_kind("600887"), "stock")

    def test_registry_unavailable_outside_listed_ranges_pauses(self):
        # F06: OTC registry down + code outside every listed range → identity
        # unknown → paused (never silently routed to the stock path).
        with patch.object(ps, "classify_cn_symbol", lambda symbol: "unknown"), \
             patch.object(ps, "_fetch_stock_realtime_quote") as stock_mock, \
             patch.object(ps, "fetch_latest_nav") as nav_mock:
            info = PortfolioService._fetch_realtime_position_price("016452")
        self.assertEqual(info["kind"], "identity_paused")
        stock_mock.assert_not_called()
        nav_mock.assert_not_called()

    def test_nav_provider_time_is_date_only(self):
        # R3/F07: NAV keeps DATE precision — provider_time is the nav date
        # string; no fabricated 15:00 publish time.
        nav_date = date.today() - timedelta(days=1)
        with patch.object(ps, "classify_cn_symbol", lambda symbol: "otc"), \
             patch.object(ps, "fetch_latest_nav",
                          lambda symbol: {"nav": 1.8012, "nav_date": nav_date}):
            info = PortfolioService._fetch_realtime_position_price("016452")
        self.assertEqual(info["provider_time"], nav_date.isoformat())
        self.assertNotIn("T", info["provider_time"])

    def test_missing_provider_time_keeps_price_date_unknown(self):
        # R3/F07: no provider time → price_date stays None (never the fetch
        # date) and time_quality=unknown.
        quote_time = datetime.now(CN_TZ).isoformat()
        quotes = {
            "600887": {
                "price": 25.0, "provider": "tencent", "kind": "realtime",
                "provider_time": None, "fetched_at": quote_time,
            },
        }
        self.service.record_trade(
            account_id=self.account_id, symbol="600887", trade_date=date.today(),
            side="buy", quantity=300, price=26.667,
        )
        resolved = self.service._resolve_position_price(
            symbol="600887", as_of_date=date.today(), realtime_prices=quotes,
        )
        self.assertTrue(resolved.is_available)
        self.assertIsNone(resolved.price_date)
        self.assertEqual(resolved.time_quality, "unknown")

    def test_snapshot_exposes_valuation_id_and_quote_refs(self):
        quote_time = datetime.now(CN_TZ).isoformat()
        quotes = {
            "600887": {
                "price": 25.0, "provider": "tencent", "kind": "realtime",
                "provider_time": quote_time, "fetched_at": quote_time,
            },
        }
        self.service.record_trade(
            account_id=self.account_id, symbol="600887", trade_date=date.today(),
            side="buy", quantity=300, price=26.667,
        )
        with patch.object(PortfolioService, "_prefetch_realtime_position_prices",
                          lambda self, symbols: dict(quotes)):
            snap = self._snapshot()
        self.assertTrue(str(snap.get("valuation_id", "")).startswith("val-"))
        refs = snap.get("quote_refs", [])
        self.assertEqual(len(refs), 1)
        self.assertEqual(refs[0]["symbol"], "600887")
        self.assertEqual(refs[0]["provider"], "tencent")
        self.assertEqual(refs[0]["price_kind"], "realtime")

    def test_shared_upstream_groups_are_disclosed_not_dual_source(self):
        # F09: akshare ETF / efinance ETF / 基金净值 同属 eastmoney 上游组。
        from data_provider.realtime_types import (
            CircuitBreaker,
            upstream_group_for,
        )

        self.assertEqual(upstream_group_for("akshare_etf"), "eastmoney")
        self.assertEqual(upstream_group_for("efinance_etf"), "eastmoney")
        self.assertEqual(upstream_group_for("akshare_fund_nav"), "eastmoney")
        self.assertEqual(upstream_group_for("tencent"), "tencent")
        self.assertEqual(upstream_group_for("akshare_etf"), upstream_group_for("efinance_etf"),
                         "同一上游组：不得称独立双源")

        # 熔断范围按源：源级连续失败进入熔断；单次成功恢复；无全量循环重试语义。
        breaker = CircuitBreaker(failure_threshold=3, cooldown_seconds=300.0)
        for _ in range(3):
            breaker.record_failure("akshare_etf", "RemoteDisconnected")
        self.assertFalse(breaker.is_available("akshare_etf"), "连续 3 次失败进入熔断")
        self.assertTrue(breaker.is_available("efinance_etf"),
                        "熔断按源隔离，不殃及其他来源键")
        self.assertTrue(breaker.is_available("akshare_etf_unknown_symbol_absent"),
                        "单标的缺数据不是源失败（缺行路径不计 failure）")

if __name__ == "__main__":
    unittest.main()
