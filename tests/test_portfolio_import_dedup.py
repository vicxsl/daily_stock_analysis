# -*- coding: utf-8 -*-
"""T05 — CSV import dedup identity: reorder idempotency + legitimate identical fills."""

from __future__ import annotations

import os
import tempfile
import unittest
from datetime import date
from pathlib import Path

from src.config import Config
from src.services.portfolio_import_service import PortfolioImportService
from src.services.portfolio_service import PortfolioService
from src.storage import DatabaseManager

CSV_HEADER = "成交日期,证券代码,买卖标志,成交数量,成交均价,成交编号\n"


def csv_content(rows):
    return CSV_HEADER + "".join(",".join(str(c) for c in row) + "\n" for row in rows)


class ImportDedupTestCase(unittest.TestCase):
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
        self.importer = PortfolioImportService()
        self.account_id = self.service.create_account(name="TEST", broker="TEST", market="cn", base_currency="CNY")["id"]

    def tearDown(self) -> None:
        DatabaseManager.reset_instance()
        Config.reset_instance()
        os.environ.pop("ENV_FILE", None)
        os.environ.pop("DATABASE_PATH", None)
        self.temp_dir.cleanup()

    def _parse_commit(self, text):
        parsed = self.importer.parse_trade_csv(broker="huatai", content=text.encode("utf-8"))
        assert parsed["record_count"] > 0, parsed
        return self.importer.commit_trade_records(
            account_id=self.account_id,
            broker="huatai",
            records=parsed["records"],
        )

    def test_reimport_same_file_is_idempotent(self):
        text = csv_content([
            ("2026-09-15", "600887", "买入", 100, 26.5, "T1001"),
            ("2026-09-15", "601899", "买入", 200, 32.0, "T1002"),
        ])
        first = self._parse_commit(text)
        second = self._parse_commit(text)
        self.assertEqual(first["inserted_count"], 2)
        self.assertEqual(second["inserted_count"], 0)
        self.assertEqual(second["duplicate_count"], 2)

    def test_reordered_file_does_not_duplicate(self):
        first_text = csv_content([
            ("2026-09-15", "600887", "买入", 100, 26.5, "T1001"),
            ("2026-09-15", "601899", "买入", 200, 32.0, "T1002"),
        ])
        reordered = csv_content([
            ("2026-09-15", "601899", "买入", 200, 32.0, "T1002"),
            ("2026-09-15", "600887", "买入", 100, 26.5, "T1001"),
        ])
        first = self._parse_commit(first_text)
        second = self._parse_commit(reordered)
        self.assertEqual(first["inserted_count"], 2)
        self.assertEqual(second["inserted_count"], 0)
        self.assertEqual(second["duplicate_count"], 2)

    def test_legitimate_identical_fills_stay_distinct(self):
        # Same content, no trade_uid, two real fills on separate lines.
        text = csv_content([
            ("2026-09-15", "600887", "买入", 100, 26.5, ""),
            ("2026-09-15", "600887", "买入", 100, 26.5, ""),
        ])
        result = self._parse_commit(text)
        self.assertEqual(result["inserted_count"], 2)
        self.assertEqual(result["duplicate_count"], 0)
        trades = self.service.list_trade_events(account_id=self.account_id)
        self.assertEqual(trades["total"], 2)
        # Re-import: both already known, still exactly two trades.
        again = self._parse_commit(text)
        self.assertEqual(again["inserted_count"], 0)
        self.assertEqual(again["duplicate_count"], 2)

    def test_trade_uid_identity_wins_over_content(self):
        # Same uid, different values: duplicate by uid, not a second trade.
        self._parse_commit(csv_content([("2026-09-15", "600887", "买入", 100, 26.5, "T2001")]))
        result = self._parse_commit(csv_content([("2026-09-15", "600887", "买入", 90, 27.0, "T2001")]))
        self.assertEqual(result["inserted_count"], 0)
        self.assertEqual(result["duplicate_count"], 1)


if __name__ == "__main__":
    unittest.main()
