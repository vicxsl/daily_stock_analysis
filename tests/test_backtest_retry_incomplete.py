# -*- coding: utf-8 -*-
"""Auto incomplete-retry semantics for backtest evaluation (2026-10-10 spec).

Covers AC11-13/AC17: insufficient/error rows are re-evaluated on their
original (analysis_history_id, eval_window_days, engine_version) key with an
immutable pre-update snapshot; completed rows are never rewritten or
downgraded; manual force / force=false semantics stay unchanged.
"""

import json
import os
import tempfile
import unittest
from datetime import date, datetime
from typing import Any, Dict
from unittest.mock import patch

from src.config import Config
from src.repositories.backtest_repo import BacktestRepository
from src.services.backtest_service import BacktestService
from src.storage import AnalysisHistory, BacktestResult, DatabaseManager, StockDaily


def _phase_snapshot(
    analysis_date: date,
    *,
    phase: str = "premarket",
    market: str = "cn",
    effective_date: date | None = None,
) -> str:
    summary: Dict[str, Any] = {"phase": phase, "market": market}
    if effective_date is not None:
        summary["effective_daily_bar_date"] = effective_date.isoformat()
    return json.dumps(
        {
            "enhanced_context": {"date": analysis_date.isoformat()},
            "market_phase_summary": summary,
        }
    )


class BacktestRetryIncompleteTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_dir = tempfile.TemporaryDirectory()
        self._db_path = os.path.join(self._temp_dir.name, "retry_backtest.db")
        self._revision_root = os.path.join(self._temp_dir.name, "backtest-revisions")
        self._original_env = {
            key: os.environ.get(key)
            for key in ("ENV_FILE", "DATABASE_PATH", "BACKTEST_EVAL_WINDOW_DAYS")
        }
        self._env_path = os.path.join(self._temp_dir.name, ".env")
        with open(self._env_path, "w", encoding="utf-8") as env_file:
            env_file.write("STOCK_LIST=600519\n")

        os.environ["ENV_FILE"] = self._env_path
        os.environ["DATABASE_PATH"] = self._db_path
        os.environ["BACKTEST_EVAL_WINDOW_DAYS"] = "3"

        Config._instance = None
        DatabaseManager.reset_instance()
        self.db = DatabaseManager.get_instance()

        with self.db.get_session() as session:
            session.add(
                AnalysisHistory(
                    query_id="retry-1",
                    code="600519",
                    name="贵州茅台",
                    report_type="simple",
                    sentiment_score=70,
                    operation_advice="买入",
                    trend_prediction="看多",
                    analysis_summary="retry-test",
                    stop_loss=95.0,
                    take_profit=110.0,
                    created_at=datetime(2024, 1, 1, 0, 0, 0),
                    context_snapshot=_phase_snapshot(date(2024, 1, 1), effective_date=date(2024, 1, 1)),
                )
            )
            session.add(
                StockDaily(
                    code="600519",
                    date=date(2024, 1, 1),
                    open=100.0,
                    high=101.0,
                    low=99.0,
                    close=100.0,
                )
            )
            # Only ONE forward bar initially: window=3 -> insufficient_data.
            session.add(
                StockDaily(code="600519", date=date(2024, 1, 2), high=111.0, low=100.0, close=105.0)
            )
            session.commit()
        self.analysis_id = 1

    def tearDown(self) -> None:
        Config._instance = None
        DatabaseManager.reset_instance()
        for key, value in self._original_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        self._temp_dir.cleanup()

    # -- helpers ---------------------------------------------------------

    def _run_retry(self) -> Dict[str, Any]:
        service = BacktestService(self.db)
        with patch.object(service, "_try_fill_daily_data"):
            return service.run_backtest(
                force=False,
                retry_incomplete=True,
                eval_window_days=3,
                min_age_days=0,
                limit=10,
                revision_root=self._revision_root,
            )

    def _result_row(self) -> BacktestResult:
        with self.db.get_session() as session:
            return session.query(BacktestResult).filter(
                BacktestResult.analysis_history_id == self.analysis_id
            ).one()

    def _count_rows(self) -> int:
        with self.db.get_session() as session:
            return session.query(BacktestResult).count()

    def _snapshot_files(self) -> list:
        directory = os.path.join(self._revision_root, f"{self.analysis_id}-3-v1")
        if not os.path.isdir(directory):
            return []
        return sorted(
            name for name in os.listdir(directory)
            if name.endswith(".json") and not name.startswith(".")
        )

    def _add_full_forward_bars(self) -> None:
        with self.db.get_session() as session:
            session.add_all([
                StockDaily(code="600519", date=date(2024, 1, 3), high=108.0, low=103.0, close=106.0),
                StockDaily(code="600519", date=date(2024, 1, 4), high=109.0, low=104.0, close=107.0),
            ])
            session.commit()

    # -- tests -----------------------------------------------------------

    def test_manual_default_excludes_incomplete_rows(self) -> None:
        first = self._run_retry()
        self.assertEqual(first["processed"], 1)
        self.assertEqual(first["insufficient"], 1)
        self.assertEqual(first["diagnostics"]["save_inserted"], 1)

        # Manual default (force=False, no retry flag) keeps legacy semantics:
        # the stored insufficient row hides the analysis from candidates.
        service = BacktestService(self.db)
        with patch.object(service, "_try_fill_daily_data"):
            manual = service.run_backtest(force=False, eval_window_days=3, min_age_days=0, limit=10)
        self.assertEqual(manual["processed"], 0)
        self.assertEqual(self._count_rows(), 1)

    def test_force_still_replaces_incomplete_rows(self) -> None:
        self._run_retry()
        self._add_full_forward_bars()

        service = BacktestService(self.db)
        with patch.object(service, "_try_fill_daily_data"):
            forced = service.run_backtest(force=True, eval_window_days=3, min_age_days=0, limit=10)
        self.assertEqual(forced["processed"], 1)
        self.assertEqual(forced["completed"], 1)
        self.assertEqual(forced["saved"], 1)
        self.assertEqual(self._count_rows(), 1)
        self.assertEqual(self._result_row().eval_status, "completed")

    def test_retry_updates_original_key_and_keeps_snapshot(self) -> None:
        first = self._run_retry()
        self.assertEqual(first["insufficient"], 1)
        original_id = self._result_row().id

        self._add_full_forward_bars()
        second = self._run_retry()
        self.assertEqual(second["processed"], 1)
        self.assertEqual(second["completed"], 1)
        self.assertEqual(second["saved"], 1)
        self.assertEqual(second["diagnostics"]["save_updated"], 1)

        row = self._result_row()
        self.assertEqual(row.id, original_id, "original row id must be preserved")
        self.assertEqual(row.eval_status, "completed")

        snapshots = self._snapshot_files()
        self.assertEqual(len(snapshots), 1, "exactly one pre-update snapshot expected")
        snapshot_path = os.path.join(self._revision_root, f"{self.analysis_id}-3-v1", snapshots[0])
        with open(snapshot_path, encoding="utf-8") as handle:
            payload = json.load(handle)
        self.assertEqual(payload["original"]["eval_status"], "insufficient_data")
        self.assertEqual(payload["key"]["id"], original_id)

        # Third run: completed rows are never re-selected by the retry path.
        third = self._run_retry()
        self.assertEqual(third["processed"], 0)
        self.assertEqual(self._count_rows(), 1)
        self.assertEqual(len(self._snapshot_files()), 1)

    def test_retry_reports_waiting_and_unsupported(self) -> None:
        with self.db.get_session() as session:
            # Partial window (1 of 3 bars) with valid identity -> waiting.
            session.add(
                AnalysisHistory(
                    query_id="retry-wait",
                    code="000002",
                    report_type="simple",
                    sentiment_score=60,
                    operation_advice="买入",
                    trend_prediction="看多",
                    analysis_summary="wait",
                    created_at=datetime(2024, 1, 1, 0, 0, 0),
                    context_snapshot=_phase_snapshot(date(2024, 1, 1), effective_date=date(2024, 1, 1)),
                )
            )
            session.add(
                StockDaily(code="000002", date=date(2024, 1, 1), open=10, high=10, low=10, close=10)
            )
            session.add(
                StockDaily(code="000002", date=date(2024, 1, 2), open=10, high=10, low=10, close=10)
            )
            # Unresolvable identity -> unsupported (no fabricated completed).
            session.add(
                AnalysisHistory(
                    query_id="retry-unsup",
                    code="NOTACODE",
                    report_type="simple",
                    sentiment_score=60,
                    operation_advice="买入",
                    trend_prediction="看多",
                    analysis_summary="unsup",
                    created_at=datetime(2024, 1, 1, 0, 0, 0),
                    context_snapshot=_phase_snapshot(date(2024, 1, 1), effective_date=date(2024, 1, 1)),
                )
            )
            session.commit()

        stats = self._run_retry()
        diagnostics = stats["diagnostics"]
        self.assertGreaterEqual(diagnostics.get("waiting_window", 0), 2)
        self.assertGreaterEqual(diagnostics.get("unsupported", 0), 1)
        self.assertEqual(stats["completed"], 0)
        with self.db.get_session() as session:
            statuses = {
                row.code: row.eval_status
                for row in session.query(BacktestResult).all()
            }
        self.assertEqual(statuses.get("600519"), "insufficient_data")
        self.assertEqual(statuses.get("000002"), "insufficient_data")
        self.assertEqual(statuses.get("NOTACODE"), "insufficient_data")

    def test_completed_row_is_never_downgraded_by_retry_writer(self) -> None:
        self._run_retry()

        # Simulate another writer completing the row first.
        with self.db.get_session() as session:
            row = session.query(BacktestResult).filter(
                BacktestResult.analysis_history_id == self.analysis_id
            ).one()
            row.eval_status = "completed"
            row.stock_return_pct = 7.0
            row.outcome = "win"
            session.commit()
        completed_id = self._result_row().id
        self.assertEqual(self._snapshot_files(), [])

        repo = BacktestRepository(self.db)
        late_result = BacktestResult(
            analysis_history_id=self.analysis_id,
            code="600519",
            analysis_date=date(2024, 1, 1),
            eval_window_days=3,
            engine_version="v1",
            eval_status="insufficient_data",
            evaluated_at=datetime.now(),
            operation_advice="买入",
        )
        report = repo.save_results_idempotent([late_result], revision_root=self._revision_root)
        self.assertEqual(report["skipped_completed"], 1)
        self.assertEqual(report["updated"], 0)

        row = self._result_row()
        self.assertEqual(row.id, completed_id)
        self.assertEqual(row.eval_status, "completed")
        self.assertEqual(row.stock_return_pct, 7.0)
        self.assertEqual(self._snapshot_files(), [])

    def test_snapshot_failure_aborts_row_update(self) -> None:
        self._run_retry()
        self.assertEqual(self._result_row().eval_status, "insufficient_data")
        self._add_full_forward_bars()

        # revision_root whose parent is a regular file -> snapshot write fails.
        blocker = os.path.join(self._temp_dir.name, "blocker")
        with open(blocker, "w", encoding="utf-8") as handle:
            handle.write("not a dir")
        broken_root = os.path.join(blocker, "revisions")

        service = BacktestService(self.db)
        with patch.object(service, "_try_fill_daily_data"):
            stats = service.run_backtest(
                force=False,
                retry_incomplete=True,
                eval_window_days=3,
                min_age_days=0,
                limit=10,
                revision_root=broken_root,
            )
        self.assertEqual(stats["completed"], 1)
        self.assertEqual(stats["diagnostics"]["save_errors"], 1)
        self.assertEqual(stats["diagnostics"]["save_updated"], 0)
        self.assertEqual(stats["saved"], 0)
        self.assertEqual(self._result_row().eval_status, "insufficient_data")

    def test_db_failure_keeps_row_and_unapplied_snapshot(self) -> None:
        self._run_retry()
        self.assertEqual(self._result_row().eval_status, "insufficient_data")

        repo = BacktestRepository(self.db)
        replacement = BacktestResult(
            analysis_history_id=self.analysis_id,
            code="600519",
            analysis_date=date(2024, 1, 1),
            eval_window_days=3,
            engine_version="v1",
            eval_status="completed",
            evaluated_at=datetime.now(),
            operation_advice="买入",
            stock_return_pct=5.0,
        )
        with patch("src.storage.Session.commit", side_effect=RuntimeError("db down")):
            report = repo.save_results_idempotent(
                [replacement], revision_root=self._revision_root
            )
        self.assertEqual(report["errors"], 1)
        self.assertEqual(report["updated"], 0)

        row = self._result_row()
        self.assertEqual(row.eval_status, "insufficient_data")
        # Snapshot was written before the failing commit and stays on disk.
        self.assertEqual(len(self._snapshot_files()), 1)

    def test_retry_and_force_are_mutually_exclusive(self) -> None:
        service = BacktestService(self.db)
        with self.assertRaises(ValueError):
            service.run_backtest(force=True, retry_incomplete=True, min_age_days=0)


if __name__ == "__main__":
    unittest.main()
