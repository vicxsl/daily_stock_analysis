# -*- coding: utf-8 -*-
"""T04/T05 — account baseline vs real trades, confirm idempotency (D12)."""

from __future__ import annotations

import os
import tempfile
import unittest
from datetime import date
from pathlib import Path

from src.config import Config
from src.services.portfolio_service import (
    PortfolioConflictError,
    PortfolioOversellError,
    PortfolioService,
)
from src.storage import DatabaseManager


class BaselineTestCase(unittest.TestCase):
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

    def _snapshot(self):
        return self.service.get_portfolio_snapshot(
            account_id=self.account_id,
            as_of=date.today(),
            cost_method="fifo",
            include_realtime=False,
        )

    def test_baseline_establishes_positions_without_cash_movement(self):
        result = self.service.record_baseline(
            account_id=self.account_id,
            baseline_date=date(2026, 9, 1),
            positions=[
                {"symbol": "600887", "quantity": 300, "cost_per_unit": 26.667},
                {"symbol": "159632", "quantity": 11300, "cost_per_unit": 1.748},
            ],
            cash=10000.0,
        )
        self.assertEqual(len(result["positions"]), 2)
        self.assertTrue(all(not p["duplicate"] for p in result["positions"]))
        self.assertFalse(result["cash"]["duplicate"])

        snap = self._snapshot()
        account = snap["accounts"][0]
        self.assertEqual(account["baseline_date"], "2026-09-01")
        self.assertEqual(account["event_type_counts"].get("baseline"), 2)
        # Baseline cash is the only cash movement.
        self.assertAlmostEqual(account["total_cash"], 10000.0, places=4)
        # Baseline positions hold without negative cash from fake buys.
        symbols = {p["symbol"] for p in account["positions"]}
        self.assertIn("600887", symbols)
        self.assertIn("159632", symbols)
        # No real trades yet -> no fees, no realized pnl.
        self.assertAlmostEqual(account["fee_total"], 0.0, places=6)
        self.assertAlmostEqual(account["realized_pnl"], 0.0, places=6)

    def test_baseline_resubmit_identical_is_idempotent(self):
        kwargs = dict(
            account_id=self.account_id,
            baseline_date=date(2026, 9, 1),
            positions=[{"symbol": "600887", "quantity": 300, "cost_per_unit": 26.667}],
            cash=5000.0,
        )
        first = self.service.record_baseline(**kwargs)
        second = self.service.record_baseline(**kwargs)
        self.assertTrue(all(p["duplicate"] for p in second["positions"]))
        self.assertTrue(second["cash"]["duplicate"])
        # Same trade rows created once.
        trades = self.service.list_trade_events(account_id=self.account_id, event_type="baseline")
        self.assertEqual(trades["total"], 1)
        self.assertEqual(first["positions"][0]["id"], second["positions"][0]["id"])

    def test_baseline_conflict_on_different_values(self):
        self.service.record_baseline(
            account_id=self.account_id,
            baseline_date=date(2026, 9, 1),
            positions=[{"symbol": "600887", "quantity": 300, "cost_per_unit": 26.667}],
        )
        with self.assertRaises(PortfolioConflictError):
            self.service.record_baseline(
                account_id=self.account_id,
                baseline_date=date(2026, 9, 1),
                positions=[{"symbol": "600887", "quantity": 400, "cost_per_unit": 26.667}],
            )

    def test_real_trade_after_baseline_moves_cash_and_realized(self):
        self.service.record_baseline(
            account_id=self.account_id,
            baseline_date=date(2026, 9, 1),
            positions=[{"symbol": "600887", "quantity": 300, "cost_per_unit": 26.667}],
            cash=20000.0,
        )
        self.service.record_trade(
            account_id=self.account_id,
            symbol="600887",
            trade_date=date(2026, 9, 15),
            side="sell",
            quantity=100,
            price=28.0,
            fee=5.0,
        )
        snap = self._snapshot()
        account = snap["accounts"][0]
        # cash = 20000 baseline + sell proceeds 100*28-5 = 2795
        self.assertAlmostEqual(account["total_cash"], 22795.0, places=4)
        # Realized = proceeds - fifo cost (100 * 26.667)
        self.assertAlmostEqual(account["realized_pnl"], 2795.0 - 2666.7, places=4)
        self.assertAlmostEqual(account["fee_total"], 5.0, places=6)
        positions = {p["symbol"]: p for p in account["positions"]}
        self.assertAlmostEqual(positions["600887"]["quantity"], 200.0, places=6)

    def test_init_events_excluded_from_cash_and_stats(self):
        # Legacy screenshot-style init buy must not push cash negative.
        self.service.record_trade(
            account_id=self.account_id,
            symbol="513010",
            trade_date=date(2026, 5, 13),
            side="buy",
            quantity=56400,
            price=0.753,
            note="港股科技 ETF；截图持有448天初始化",
            event_type="init",
        )
        snap = self._snapshot()
        account = snap["accounts"][0]
        self.assertAlmostEqual(account["total_cash"], 0.0, places=6)
        self.assertEqual(account["event_type_counts"].get("init"), 1)
        self.assertAlmostEqual(account["fee_total"], 0.0, places=6)

    def test_baseline_only_accepts_buy_side(self):
        with self.assertRaises(ValueError):
            self.service.record_trade(
                account_id=self.account_id,
                symbol="600887",
                trade_date=date(2026, 9, 1),
                side="sell",
                quantity=10,
                price=26.0,
                event_type="baseline",
            )

    def test_sell_cannot_exceed_baseline_holdings(self):
        self.service.record_baseline(
            account_id=self.account_id,
            baseline_date=date(2026, 9, 1),
            positions=[{"symbol": "600887", "quantity": 300, "cost_per_unit": 26.667}],
        )
        with self.assertRaises(PortfolioOversellError):
            self.service.record_trade(
                account_id=self.account_id,
                symbol="600887",
                trade_date=date(2026, 9, 20),
                side="sell",
                quantity=301,
                price=28.0,
            )

    def test_trades_listing_exposes_event_type_filter(self):
        self.service.record_baseline(
            account_id=self.account_id,
            baseline_date=date(2026, 9, 1),
            positions=[{"symbol": "600887", "quantity": 300, "cost_per_unit": 26.667}],
        )
        self.service.record_trade(
            account_id=self.account_id,
            symbol="600887",
            trade_date=date(2026, 9, 15),
            side="sell",
            quantity=10,
            price=28.0,
        )
        real = self.service.list_trade_events(account_id=self.account_id, event_type="trade")
        base = self.service.list_trade_events(account_id=self.account_id, event_type="baseline")
        self.assertEqual(real["total"], 1)
        self.assertEqual(base["total"], 1)
        self.assertEqual(real["items"][0]["event_type"], "trade")
        self.assertEqual(base["items"][0]["event_type"], "baseline")

    # ── R2 回归（2026-10-09 F01/F02）：基线接管旧事实，不叠加 ──────────────

    def test_f01_baseline_takes_over_init_history_no_double_count(self):
        """旧 init 100 份 + 现金流入 1000，再确认同值基线：快照仍是 100/1000。

        这是实施复核 F1 的失败复现：修复前回放把 init 与 baseline 相加得 200/2000。
        """
        self.service.record_trade(
            account_id=self.account_id,
            symbol="600887",
            trade_date=date(2026, 8, 1),
            side="buy",
            quantity=100,
            price=20.0,
            note="初始化",
            event_type="init",
        )
        self.service.record_cash_ledger(
            account_id=self.account_id,
            event_date=date(2026, 8, 1),
            direction="in",
            amount=1000.0,
            note="初始化现金",
        )
        self.service.record_baseline(
            account_id=self.account_id,
            baseline_date=date(2026, 9, 1),
            positions=[{"symbol": "600887", "quantity": 100, "cost_per_unit": 20.0}],
            cash=1000.0,
        )
        snap = self._snapshot()
        account = snap["accounts"][0]
        positions = {p["symbol"]: p for p in account["positions"]}
        self.assertAlmostEqual(positions["600887"]["quantity"], 100.0, places=6, msg="基线接管后不得叠加为 200")
        self.assertAlmostEqual(account["total_cash"], 1000.0, places=4, msg="基线现金不得叠加为 2000")
        # 被接管的旧事件在快照中可见为 superseded，不再是事实输入。
        self.assertGreaterEqual(account["superseded_event_count"], 2)
        self.assertEqual(account["event_type_counts"].get("init"), None)

        # 基线后的真实买卖正常移动现金；旧 init 仍不视为真实成交。
        self.service.record_trade(
            account_id=self.account_id,
            symbol="600887",
            trade_date=date(2026, 9, 15),
            side="buy",
            quantity=10,
            price=20.0,
        )
        snap = self._snapshot()
        account = snap["accounts"][0]
        positions = {p["symbol"]: p for p in account["positions"]}
        self.assertAlmostEqual(positions["600887"]["quantity"], 110.0, places=6)
        self.assertAlmostEqual(account["total_cash"], 1000.0 - 200.0, places=4)

        # 旧原件保留：init 与 baseline 行都还在库里。
        init_rows = self.service.list_trade_events(account_id=self.account_id, event_type="init")
        baseline_rows = self.service.list_trade_events(account_id=self.account_id, event_type="baseline")
        self.assertEqual(init_rows["total"], 1)
        self.assertEqual(baseline_rows["total"], 1)

    def test_f02_new_baseline_drops_symbol_without_fabricated_sell(self):
        """旧基线两标的，新基线只留一只：消失标的不持仓也不生成卖出。"""
        self.service.record_baseline(
            account_id=self.account_id,
            baseline_date=date(2026, 8, 1),
            positions=[
                {"symbol": "600887", "quantity": 100, "cost_per_unit": 20.0},
                {"symbol": "513010", "quantity": 1000, "cost_per_unit": 0.75},
            ],
            cash=500.0,
        )
        self.service.record_baseline(
            account_id=self.account_id,
            baseline_date=date(2026, 9, 1),
            positions=[{"symbol": "600887", "quantity": 100, "cost_per_unit": 20.0}],
            cash=500.0,
        )
        snap = self._snapshot()
        account = snap["accounts"][0]
        symbols = {p["symbol"] for p in account["positions"]}
        self.assertEqual(symbols, {"600887"}, "513010 从新基线消失 = 基线时点无该仓位")
        sells = self.service.list_trade_events(account_id=self.account_id, event_type="trade")
        self.assertEqual(sells["total"], 0, "不得伪造成交")
        self.assertEqual(account["baseline_date"], "2026-09-01", "活动基线是最新确认的那个")

    def test_f02_empty_baseline_with_zero_cash_confirmed(self):
        """确认空仓/零现金合法；未知现金保持 unknown 不补 0。"""
        result = self.service.record_baseline(
            account_id=self.account_id,
            baseline_date=date(2026, 9, 1),
            positions=[],
            cash=0.0,
        )
        self.assertEqual(result["positions"], [])
        snap = self._snapshot()
        account = snap["accounts"][0]
        self.assertAlmostEqual(account["total_cash"], 0.0, places=6)
        self.assertTrue(account["cash_known"])
        self.assertEqual(account["positions"], [])

    def test_f02_unknown_cash_stays_unknown_not_zero(self):
        self.service.record_baseline(
            account_id=self.account_id,
            baseline_date=date(2026, 9, 1),
            positions=[{"symbol": "600887", "quantity": 100, "cost_per_unit": 20.0}],
            cash=None,
        )
        snap = self._snapshot()
        account = snap["accounts"][0]
        self.assertIsNone(account["total_cash"], "未知现金不得用 0 替代")
        self.assertFalse(account["cash_known"])
        self.assertIsNone(account["total_equity"], "现金未知时不算完整总权益")
        self.assertTrue(any("cash unknown" in item for item in account["limitations"]))
        # 聚合层同样不给完整现金/权益。
        self.assertIsNone(snap["total_cash"])
        self.assertFalse(snap["cash_known"])

    def test_f02_same_day_real_events_block_baseline_and_vice_versa(self):
        """同日真实事件与基线的先后不可判定：双向拒绝，要求核对。"""
        self.service.record_trade(
            account_id=self.account_id,
            symbol="600887",
            trade_date=date(2026, 9, 1),
            side="buy",
            quantity=10,
            price=20.0,
        )
        with self.assertRaises(PortfolioConflictError):
            self.service.record_baseline(
                account_id=self.account_id,
                baseline_date=date(2026, 9, 1),
                positions=[{"symbol": "600887", "quantity": 10, "cost_per_unit": 20.0}],
                cash=0.0,
            )
        # 反向：先落基线，再往同日补真实事件同样拒绝。
        self.service.record_baseline(
            account_id=self.account_id,
            baseline_date=date(2026, 9, 5),
            positions=[{"symbol": "600887", "quantity": 10, "cost_per_unit": 20.0}],
            cash=0.0,
        )
        with self.assertRaises(PortfolioConflictError):
            self.service.record_cash_ledger(
                account_id=self.account_id,
                event_date=date(2026, 9, 5),
                direction="in",
                amount=100.0,
            )
        with self.assertRaises(PortfolioConflictError):
            self.service.record_trade(
                account_id=self.account_id,
                symbol="600887",
                trade_date=date(2026, 9, 5),
                side="buy",
                quantity=5,
                price=21.0,
            )

    def test_f02_whole_baseline_retry_and_conflict(self):
        """相同确认重试不加量；同日不同内容冲突；换日重新确认合法。"""
        kwargs = dict(
            account_id=self.account_id,
            baseline_date=date(2026, 9, 1),
            positions=[{"symbol": "600887", "quantity": 100, "cost_per_unit": 20.0}],
            cash=1000.0,
        )
        self.service.record_baseline(**kwargs)
        again = self.service.record_baseline(**kwargs)
        self.assertTrue(again["duplicate"])
        rows = self.service.list_trade_events(account_id=self.account_id, event_type="baseline")
        self.assertEqual(rows["total"], 1, "相同确认重试不得加量")
        with self.assertRaises(PortfolioConflictError):
            self.service.record_baseline(
                account_id=self.account_id,
                baseline_date=date(2026, 9, 1),
                positions=[{"symbol": "600887", "quantity": 100, "cost_per_unit": 20.0}],
                cash=2000.0,
            )
        # 新基线日重新确认：接管旧基线。
        self.service.record_baseline(
            account_id=self.account_id,
            baseline_date=date(2026, 9, 20),
            positions=[{"symbol": "600887", "quantity": 100, "cost_per_unit": 20.0}],
            cash=1500.0,
        )
        snap = self._snapshot()
        self.assertAlmostEqual(snap["accounts"][0]["total_cash"], 1500.0, places=4)

    def test_f02_unknown_cost_keeps_position_without_pnl(self):
        """成本未确认：保留份额与市值，不计算盈亏。"""
        self.service.record_baseline(
            account_id=self.account_id,
            baseline_date=date(2026, 9, 1),
            positions=[{"symbol": "600887", "quantity": 100, "cost_per_unit": None}],
            cash=100.0,
        )
        snap = self._snapshot()
        account = snap["accounts"][0]
        position = account["positions"][0]
        self.assertAlmostEqual(position["quantity"], 100.0, places=6)
        self.assertTrue(position["cost_basis_unknown"])
        self.assertIsNone(position["unrealized_pnl_pct"])
        self.assertAlmostEqual(position["unrealized_pnl_base"], 0.0, places=6)
        self.assertTrue(any("cost basis unknown" in item for item in position["limitations"]))

    def test_f01_oversell_check_respects_baseline_takeover(self):
        """卖出入库的数量校验同样不得把 init+baseline 相加（否则允许超卖）。"""
        self.service.record_trade(
            account_id=self.account_id,
            symbol="600887",
            trade_date=date(2026, 8, 1),
            side="buy",
            quantity=100,
            price=20.0,
            note="初始化",
            event_type="init",
        )
        self.service.record_baseline(
            account_id=self.account_id,
            baseline_date=date(2026, 9, 1),
            positions=[{"symbol": "600887", "quantity": 100, "cost_per_unit": 20.0}],
            cash=1000.0,
        )
        with self.assertRaises(PortfolioOversellError):
            self.service.record_trade(
                account_id=self.account_id,
                symbol="600887",
                trade_date=date(2026, 9, 15),
                side="sell",
                quantity=150,  # init(100)+baseline(100)=200 的旧错误会放行
                price=22.0,
            )


if __name__ == "__main__":
    unittest.main()
