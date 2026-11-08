# -*- coding: utf-8 -*-
"""P1/P2 follow-up tests (2026-10-24): tencent provider time + sector asset-kind routing.

P1: tencent realtime field 30 must be parsed into provider_timestamp (Beijing
time, ISO 8601); missing/garbage/future times stay None (unknown), never a
fabricated fetch-time. The value must flow through _enrich_realtime_quote
(fetched_at vs provider time kept distinct) and into the portfolio quote_info.
P2: sector classification routes by instrument identity; OTC funds and ETFs
never call the stock board API; UNCLASSIFIED is a coverage gap, never a real
sector that triggers concentration alerts.
Run offline inside the DSA image (no network, no model).
"""

from __future__ import annotations

import sys
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

CN_TZ = timezone(timedelta(hours=8))


def _tencent_fields(time_value: str) -> list:
    fields = ["0"] * 50
    fields[1] = "贵州茅台"
    fields[2] = "600519"
    fields[3] = "1688.00"
    fields[4] = "1680.00"
    fields[5] = "1685.00"
    fields[6] = "12345"
    fields[31] = "8.00"
    fields[32] = "0.48"
    fields[33] = "1699.00"
    fields[34] = "1677.00"
    fields[38] = "0.10"
    if time_value is not None:
        fields[30] = time_value
    return fields


class TencentProviderTimeParseTest(unittest.TestCase):
    # 固定基准时钟，避免夹具时间相对真实时钟变成"未来时间"被守卫拒绝。
    NOW = datetime(2026, 10, 24, 15, 30, 0, tzinfo=CN_TZ)

    def test_valid_14_digit_beijing_time(self) -> None:
        from data_provider.akshare_fetcher import _parse_tencent_provider_time

        parsed = _parse_tencent_provider_time(_tencent_fields("20261024145959"), now_cn=self.NOW)
        self.assertEqual(parsed, "2026-10-24T14:59:59+08:00")

    def test_dashed_variant_accepted(self) -> None:
        from data_provider.akshare_fetcher import _parse_tencent_provider_time

        parsed = _parse_tencent_provider_time(_tencent_fields("2026-10-24 14:59:03"), now_cn=self.NOW)
        self.assertEqual(parsed, "2026-10-24T14:59:03+08:00")

    def test_missing_or_short_payload_is_unknown(self) -> None:
        from data_provider.akshare_fetcher import _parse_tencent_provider_time

        self.assertIsNone(_parse_tencent_provider_time(_tencent_fields(None), now_cn=self.NOW))
        short = _tencent_fields("20261024145959")[:29]
        self.assertIsNone(_parse_tencent_provider_time(short, now_cn=self.NOW))
        self.assertIsNone(_parse_tencent_provider_time(_tencent_fields(""), now_cn=self.NOW))

    def test_garbage_format_is_unknown(self) -> None:
        from data_provider.akshare_fetcher import _parse_tencent_provider_time

        for bad in ["not-a-time", "0", "20261024", "2026/10/24"]:
            self.assertIsNone(_parse_tencent_provider_time(_tencent_fields(bad), now_cn=self.NOW), bad)

    def test_future_time_beyond_tolerance_is_unknown(self) -> None:
        from data_provider.akshare_fetcher import _parse_tencent_provider_time

        now = self.NOW
        future = (now + timedelta(hours=1)).strftime("%Y%m%d%H%M%S")
        self.assertIsNone(_parse_tencent_provider_time(_tencent_fields(future), now_cn=now))
        # 时钟偏差容忍 5 分钟内：保留源时间，不用抓取时间顶替。
        skew = (now + timedelta(minutes=2)).strftime("%Y%m%d%H%M%S")
        self.assertIsNotNone(_parse_tencent_provider_time(_tencent_fields(skew), now_cn=now))

    @staticmethod
    def _past_time() -> str:
        return (datetime.now(CN_TZ) - timedelta(minutes=3)).strftime("%Y%m%d%H%M%S")

    def test_quote_construction_carries_provider_time(self) -> None:
        from data_provider.akshare_fetcher import AkshareFetcher

        fetcher = AkshareFetcher()
        time_value = self._past_time()
        payload = "v_sh600519=\"" + "~".join(_tencent_fields(time_value)) + "\";"
        response = SimpleNamespace(status_code=200, text=payload)
        with patch.object(fetcher, "_enforce_rate_limit", lambda: None), \
             patch("data_provider.akshare_fetcher.get_realtime_circuit_breaker",
                   lambda: MagicMock(is_available=lambda key: True)), \
             patch("data_provider.akshare_fetcher.requests.get", return_value=response):
            quote = fetcher._get_stock_realtime_quote_tencent("600519")
        self.assertIsNotNone(quote)
        expected = datetime.strptime(time_value, "%Y%m%d%H%M%S").replace(tzinfo=CN_TZ).isoformat()
        self.assertEqual(quote.provider_timestamp, expected)

    def test_quote_construction_garbage_time_stays_unknown(self) -> None:
        from data_provider.akshare_fetcher import AkshareFetcher

        fetcher = AkshareFetcher()
        payload = "v_sh600519=\"" + "~".join(_tencent_fields("not-a-time")) + "\";"
        response = SimpleNamespace(status_code=200, text=payload)
        with patch.object(fetcher, "_enforce_rate_limit", lambda: None), \
             patch("data_provider.akshare_fetcher.get_realtime_circuit_breaker",
                   lambda: MagicMock(is_available=lambda key: True)), \
             patch("data_provider.akshare_fetcher.requests.get", return_value=response):
            quote = fetcher._get_stock_realtime_quote_tencent("600519")
        self.assertIsNotNone(quote)
        self.assertIsNone(quote.provider_timestamp)


class EnrichAndPortfolioChainTest(unittest.TestCase):
    """源时间与fetched_at分开保存；quote_info 原样传递源时间。

    2026-10-25 时钟修复：原 test_enrich_keeps_provider_time_and_sets_fetched_at
    固定报价时间为 2026-10-24 14:59:59 却用真实当前时间 enrich 并断言
    is_stale=False——超过 600s TTL 后必然失败（复核现场 66/67 的那 1 例）。
    enrich 的唯一实时依赖是 DataFetcherManager._utc_now_iso（静态方法），
    这里用实例级补丁替换为固定时刻，报价时间相对该时刻构造：同一套输入
    在任何真实日期运行结果一致，且新鲜/陈旧/TTL 边界/未来/未知全覆盖。
    不放宽生产 TTL，不改变 provider_timestamp/fetched_at 的真实语义。
    """

    # 两个模拟时钟证明用例与真实日期解耦：盘后时点 + 次日夜盘时点。
    CLOCKS = (
        datetime(2026, 10, 24, 15, 30, 0, tzinfo=CN_TZ),
        datetime(2026, 10, 25, 23, 15, 0, tzinfo=CN_TZ),
    )

    def _quote(self, provider_time: str | None):
        from data_provider.realtime_types import RealtimeSource, UnifiedRealtimeQuote

        return UnifiedRealtimeQuote(
            code="600519", name="贵州茅台", source=RealtimeSource.TENCENT,
            price=1688.0, provider_timestamp=provider_time,
        )

    def _enrich(self, quote, *, ttl=600, now_cn):
        from data_provider.base import DataFetcherManager

        mgr = DataFetcherManager.__new__(DataFetcherManager)
        fetched_iso = now_cn.astimezone(timezone.utc).isoformat()
        with patch.object(mgr, "_utc_now_iso", lambda: fetched_iso):
            return mgr._enrich_realtime_quote(quote, realtime_cache_ttl=ttl)

    def test_enrich_fresh_stale_and_ttl_edge_by_fixed_clock(self) -> None:
        for now_cn in self.CLOCKS:
            with self.subTest(now=now_cn.isoformat()):
                fetched_iso = now_cn.astimezone(timezone.utc).isoformat()

                # 新鲜：61 秒前的报价；输入用北京时段字符串，断言规范化为 UTC。
                fresh_provider = (now_cn - timedelta(seconds=61)).isoformat()
                enriched = self._enrich(self._quote(fresh_provider), now_cn=now_cn)
                self.assertEqual(
                    enriched.provider_timestamp,
                    (now_cn - timedelta(seconds=61)).astimezone(timezone.utc).isoformat(),
                )
                self.assertEqual(enriched.fetched_at, fetched_iso)
                # fetched_at 是抓取时刻、provider_timestamp 是源时刻：不同字段不得混同。
                self.assertNotEqual(enriched.fetched_at, enriched.provider_timestamp)
                self.assertEqual(enriched.stale_seconds, 61)
                self.assertFalse(enriched.is_stale)

                # 陈旧：1 小时前的报价超过 600s TTL → is_stale（生产"旧价判陈旧"语义不变）。
                stale_provider = (now_cn - timedelta(hours=1)).astimezone(timezone.utc).isoformat()
                stale = self._enrich(self._quote(stale_provider), now_cn=now_cn)
                self.assertEqual(stale.stale_seconds, 3600)
                self.assertTrue(stale.is_stale)

                # TTL 边界：恰好 600 秒不判陈旧（is_stale 是严格大于 TTL）。
                edge_provider = (now_cn - timedelta(seconds=600)).astimezone(timezone.utc).isoformat()
                edge = self._enrich(self._quote(edge_provider), now_cn=now_cn)
                self.assertEqual(edge.stale_seconds, 600)
                self.assertFalse(edge.is_stale)

                # 抓取同刻报价：不产生负 stale（max(0,...) 兜底）。
                same_now = self._enrich(
                    self._quote(now_cn.astimezone(timezone.utc).isoformat()), now_cn=now_cn)
                self.assertEqual(same_now.stale_seconds, 0)
                self.assertFalse(same_now.is_stale)

    def test_enrich_unknown_time_yields_unknown_flags(self) -> None:
        for now_cn in self.CLOCKS:
            with self.subTest(now=now_cn.isoformat()):
                enriched = self._enrich(self._quote(None), now_cn=now_cn)
                self.assertIsNone(enriched.provider_timestamp)
                self.assertIsNone(enriched.stale_seconds)
                self.assertIsNone(enriched.is_stale)

    def test_portfolio_quote_info_carries_provider_time(self) -> None:
        from src.services import portfolio_service as ps_mod

        quote = self._quote("2026-10-24T06:59:59+00:00")
        fake_manager = MagicMock()
        fake_manager.get_realtime_quote.return_value = quote
        with patch("data_provider.base.DataFetcherManager", lambda: fake_manager):
            info = ps_mod._fetch_stock_realtime_quote("600519", "2026-10-24T07:00:00+00:00")
        self.assertIsNotNone(info)
        self.assertEqual(info["provider_time"], "2026-10-24T06:59:59+00:00")
        self.assertEqual(info["fetched_at"], "2026-10-24T07:00:00+00:00")
        self.assertEqual(info["kind"], "realtime")


class SectorAssetKindRoutingTest(unittest.TestCase):
    """场外基金/ETF 不调股票板块接口；UNCLASSIFIED 是覆盖缺口不是告警行业。"""

    def _service(self):
        from src.services.portfolio_risk_service import PortfolioRiskService

        svc = PortfolioRiskService.__new__(PortfolioRiskService)
        svc.portfolio_service = SimpleNamespace(
            convert_amount=lambda **kw: (kw["amount"], False, "identity")
        )
        return svc

    def _snapshot(self, positions):
        return {
            "total_market_value": sum(p["market_value_base"] for p in positions),
            "accounts": [{
                "market": "cn",
                "base_currency": "CNY",
                "positions": positions,
            }],
        }

    def _position(self, symbol, mv, kind_hint, market="cn"):
        return {
            "symbol": symbol, "market": market, "market_value_base": mv,
            "valuation_currency": "CNY" if market == "cn" else "USD", "price_kind": kind_hint,
        }

    def _run_concentration(self, svc, snapshot, threshold=35.0):
        from datetime import date

        return svc._build_sector_concentration(snapshot, threshold, as_of_date=date(2026, 10, 24))

    def test_otc_fund_never_calls_board_api(self) -> None:
        svc = self._service()
        snapshot = self._snapshot([self._position("003096", 100.0, "nav")])
        calls = []
        with patch.object(svc, "_fetch_belong_boards",
                          side_effect=lambda symbol: calls.append(symbol) or []), \
             patch.object(svc, "_resolve_asset_kind", return_value="otc_fund"):
            result = self._run_concentration(svc, snapshot)
        self.assertEqual(calls, [])
        self.assertEqual(result["coverage"]["otc_fund_count"], 1)
        self.assertEqual(result["coverage"]["failed_count"], 0)
        self.assertEqual(result["top_sectors"][0]["sector"], "UNCLASSIFIED")
        self.assertFalse(result["top_sectors"][0]["is_alert"])
        self.assertTrue(result["top_sectors"][0]["coverage_gap"])
        self.assertFalse(result["alert"])
        self.assertEqual(result["top_weight_pct"], 0.0)

    def test_etf_without_disclosure_stays_unclassified(self) -> None:
        svc = self._service()
        snapshot = self._snapshot([self._position("516630", 100.0, "realtime")])
        calls = []
        with patch.object(svc, "_fetch_belong_boards",
                          side_effect=lambda symbol: calls.append(symbol) or []), \
             patch.object(svc, "_resolve_asset_kind", return_value="etf"):
            result = self._run_concentration(svc, snapshot)
        self.assertEqual(calls, [])
        self.assertEqual(result["coverage"]["etf_count"], 1)
        self.assertFalse(result["alert"])

    def test_stock_sector_board_lookup_unchanged(self) -> None:
        svc = self._service()
        snapshot = self._snapshot([self._position("600519", 60.0, "realtime"),
                                   self._position("000651", 40.0, "realtime")])
        calls = []
        def fake_boards(symbol):
            calls.append(symbol)
            return [{"name": "白酒" if symbol == "600519" else "家电", "type": "行业"}]
        with patch.object(svc, "_fetch_belong_boards", side_effect=fake_boards), \
             patch.object(svc, "_resolve_asset_kind", return_value="stock"):
            result = self._run_concentration(svc, snapshot)
        self.assertEqual(sorted(calls), ["000651", "600519"])
        sectors = {row["sector"]: row for row in result["top_sectors"]}
        self.assertIn("白酒", sectors)
        self.assertEqual(result["coverage"]["classified_count"], 2)
        self.assertAlmostEqual(result["top_weight_pct"], 60.0)
        # 60% 的真实行业集中度超过 35% 阈值 → 告警是正确语义。
        self.assertTrue(result["alert"])

    def test_unclassified_excluded_from_alert_even_when_dominant(self) -> None:
        svc = self._service()
        snapshot = self._snapshot([self._position("003096", 90.0, "nav"),
                                   self._position("600519", 10.0, "realtime")])
        def fake_boards(symbol):
            return [{"name": "白酒", "type": "行业"}]
        with patch.object(svc, "_fetch_belong_boards", side_effect=fake_boards), \
             patch.object(svc, "_resolve_asset_kind",
                          side_effect=lambda symbol, market, cache: "otc_fund" if symbol == "003096" else "stock"):
            result = self._run_concentration(svc, snapshot, threshold=35.0)
        # 白酒 10% 是唯一真实行业：top_weight 按 classified 计，不是 90% 的缺口。
        self.assertAlmostEqual(result["top_weight_pct"], 10.0)
        self.assertFalse(result["alert"])
        unclassified = next(row for row in result["top_sectors"] if row["sector"] == "UNCLASSIFIED")
        self.assertTrue(unclassified["coverage_gap"])
        self.assertFalse(unclassified["is_alert"])

    def test_identity_unconfirmed_skips_board_api(self) -> None:
        svc = self._service()
        snapshot = self._snapshot([self._position("000001", 100.0, "identity_paused")])
        calls = []
        with patch.object(svc, "_fetch_belong_boards",
                          side_effect=lambda symbol: calls.append(symbol) or []), \
             patch.object(svc, "_resolve_asset_kind", return_value=None):
            result = self._run_concentration(svc, snapshot)
        self.assertEqual(calls, [])
        self.assertEqual(result["coverage"]["unclassified_count"], 1)

    def test_resolve_asset_kind_confirmed_registry_first(self) -> None:
        svc = self._service()
        fake_identity = MagicMock()
        fake_identity.confirmed_kind.return_value = "otc_fund"
        with patch("src.services.instrument_identity.InstrumentIdentityService",
                   return_value=fake_identity):
            kind = svc._resolve_asset_kind("003096", "cn", {})
        self.assertEqual(kind, "otc_fund")

    def test_resolve_asset_kind_heuristic_otc(self) -> None:
        svc = self._service()
        fake_identity = MagicMock()
        fake_identity.confirmed_kind.return_value = None
        with patch("src.services.instrument_identity.InstrumentIdentityService",
                   return_value=fake_identity), \
             patch("src.services.fund_nav.classify_cn_symbol", return_value="otc"):
            kind = svc._resolve_asset_kind("003096", "cn", {})
        self.assertEqual(kind, "otc_fund")

    def test_resolve_asset_kind_heuristic_listed_stock_vs_etf(self) -> None:
        svc = self._service()
        fake_identity = MagicMock()
        fake_identity.confirmed_kind.return_value = None
        with patch("src.services.instrument_identity.InstrumentIdentityService",
                   return_value=fake_identity), \
             patch("src.services.fund_nav.classify_cn_symbol", return_value="listed"), \
             patch("data_provider.akshare_fetcher._is_etf_code", side_effect=lambda s: s == "516630"):
            self.assertEqual(svc._resolve_asset_kind("516630", "cn", {}), "etf")
            self.assertEqual(svc._resolve_asset_kind("600519", "cn", {}), "stock")

    def test_resolve_asset_kind_ambiguous_is_none(self) -> None:
        svc = self._service()
        fake_identity = MagicMock()
        fake_identity.confirmed_kind.return_value = None
        with patch("src.services.instrument_identity.InstrumentIdentityService",
                   return_value=fake_identity), \
             patch("src.services.fund_nav.classify_cn_symbol", return_value="ambiguous"):
            self.assertIsNone(svc._resolve_asset_kind("000001", "cn", {}))

    def test_non_cn_market_unclassified(self) -> None:
        svc = self._service()
        snapshot = {
            "total_market_value": 100.0,
            "accounts": [{"market": "us", "base_currency": "USD",
                          "positions": [self._position("AAPL", 100.0, "realtime", market="us")]}],
        }
        calls = []
        with patch.object(svc, "_fetch_belong_boards",
                          side_effect=lambda symbol: calls.append(symbol) or []):
            result = self._run_concentration(svc, snapshot)
        self.assertEqual(calls, [])
        self.assertEqual(result["coverage"]["unclassified_count"], 1)


if __name__ == "__main__":
    unittest.main()
