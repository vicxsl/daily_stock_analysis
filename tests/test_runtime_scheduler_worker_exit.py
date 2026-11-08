# -*- coding: utf-8 -*-
"""Worker exit and auto-backtest stage status regressions (2026-10-10 spec).

Covers AC14-16: the scheduled-analysis worker must exit deterministically after
delivering its receipt even when third-party non-daemon threads linger
(confirmed in production: litellm completion executor threads); receipts larger
than the pipe buffer must arrive intact; the auto-backtest stage status file
is exposed via RuntimeSchedulerService.status() and never fabricates success.
"""

from __future__ import annotations

import json
import os
import tempfile
import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from src.services.runtime_scheduler import (
    RuntimeSchedulerService,
    _exit_scheduled_analysis_worker,
    auto_backtest_status_path,
    build_auto_backtest_status,
    read_auto_backtest_status,
    write_auto_backtest_status,
)


def _failing_spawn_runner(result_queue, stock_codes, schedule_args_overrides):
    result_queue.put({"success": False, "error": "analysis failed"})


def _lingering_thread_success_spawn_runner(result_queue, stock_codes, schedule_args_overrides):
    os.setsid()
    release = threading.Event()
    thread = threading.Thread(target=release.wait, name="stub-lingering-worker", daemon=False)
    thread.start()
    lingering = [{"name": thread.name, "daemon": False}]
    result_queue.put({"success": True, "error": None, "lingering_threads": lingering})
    _exit_scheduled_analysis_worker(result_queue, lingering)


def _lingering_thread_large_success_spawn_runner(result_queue, stock_codes, schedule_args_overrides):
    os.setsid()
    release = threading.Event()
    thread = threading.Thread(target=release.wait, name="stub-lingering-large", daemon=False)
    thread.start()
    lingering = [{"name": thread.name, "daemon": False}]
    # Payload far beyond the 64KB pipe buffer: the feeder must flush fully
    # before the forced exit, otherwise the receipt would be truncated.
    result_queue.put(
        {"success": True, "error": None, "padding": "x" * (1024 * 1024), "lingering_threads": lingering}
    )
    _exit_scheduled_analysis_worker(result_queue, lingering)


def _make_service(target) -> RuntimeSchedulerService:
    config = SimpleNamespace(
        schedule_enabled=True,
        schedule_time="18:00",
        schedule_times=["18:00"],
    )
    service = RuntimeSchedulerService(config_provider=lambda: config)
    service._reload_config = lambda: config
    service._analysis_process_target = target
    return service


def _wait_for(service, predicate, timeout: float = 15.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate(service.status()):
            return True
        time.sleep(0.05)
    return predicate(service.status())


class WorkerExitTestCase(unittest.TestCase):
    def test_failed_receipt_records_error_without_success(self) -> None:
        service = _make_service(_failing_spawn_runner)
        self.assertTrue(service.run_now()["accepted"])
        ok = _wait_for(service, lambda s: s["last_error"] == "analysis failed")
        self.assertTrue(ok, "failure receipt should set last_error")
        status = service.status()
        self.assertIsNone(status["last_success_at"])
        self.assertFalse(status["running"])

    def test_worker_with_lingering_nondaemon_thread_exits_promptly(self) -> None:
        service = _make_service(_lingering_thread_success_spawn_runner)
        self.assertTrue(service.run_now()["accepted"])

        ok = _wait_for(service, lambda s: s["last_success_at"] is not None)
        self.assertTrue(ok, "successful receipt should record last_success_at")
        status = service.status()
        self.assertIsNone(status["last_error"], "worker must not be flagged as 'did not exit'")
        self.assertFalse(status["running"])

    def test_receipt_larger_than_pipe_buffer_arrives_intact(self) -> None:
        service = _make_service(_lingering_thread_large_success_spawn_runner)
        self.assertTrue(service.run_now()["accepted"])

        ok = _wait_for(service, lambda s: s["last_success_at"] is not None)
        self.assertTrue(ok, "large receipt should still record success")
        status = service.status()
        self.assertIsNone(status["last_error"])
        self.assertFalse(status["running"])


class AutoBacktestStatusTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_dir = tempfile.TemporaryDirectory()
        self._original = os.environ.get("DATABASE_PATH")
        os.environ["DATABASE_PATH"] = os.path.join(self._temp_dir.name, "stock_analysis.db")

    def tearDown(self) -> None:
        if self._original is None:
            os.environ.pop("DATABASE_PATH", None)
        else:
            os.environ["DATABASE_PATH"] = self._original
        self._temp_dir.cleanup()

    def test_missing_status_file_reads_unknown(self) -> None:
        self.assertEqual(read_auto_backtest_status(), {"status": "unknown"})

    def test_corrupt_status_file_reads_unknown(self) -> None:
        path = auto_backtest_status_path()
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as handle:
            handle.write("{not json")
        self.assertEqual(read_auto_backtest_status(), {"status": "unknown"})

    def test_write_read_roundtrip_and_scheduler_status_field(self) -> None:
        payload = build_auto_backtest_status(
            run_id="abc123",
            status="completed",
            started_at="2026-10-10T22:00:05",
            finished_at="2026-10-10T22:05:00",
            stats={"processed": 2},
        )
        self.assertTrue(write_auto_backtest_status(payload))
        self.assertEqual(read_auto_backtest_status(), payload)

        service = RuntimeSchedulerService()
        self.assertEqual(service.status()["last_backtest"], payload)

    def test_failed_run_carries_previous_last_success(self) -> None:
        first = build_auto_backtest_status(
            run_id="run-1",
            status="completed",
            started_at="2026-10-09T22:00:05",
            finished_at="2026-10-09T22:05:00",
        )
        self.assertTrue(write_auto_backtest_status(first))
        self.assertEqual(
            read_auto_backtest_status().get("last_success_at"),
            first["last_success_at"],
        )

        failed = build_auto_backtest_status(
            run_id="run-2",
            status="failed",
            started_at="2026-10-10T22:00:05",
            finished_at="2026-10-10T22:01:00",
            error="boom",
        )
        self.assertTrue(write_auto_backtest_status(failed))
        stored = read_auto_backtest_status()
        self.assertEqual(stored["status"], "failed")
        self.assertEqual(stored["last_success_at"], first["last_success_at"])

    def test_write_failure_returns_false_without_raising(self) -> None:
        blocker = os.path.join(self._temp_dir.name, "blocker")
        with open(blocker, "w", encoding="utf-8") as handle:
            handle.write("file")
        with patch(
            "src.services.runtime_scheduler.auto_backtest_status_path",
            return_value=os.path.join(blocker, "auto-backtest-status.json"),
        ):
            self.assertFalse(
                write_auto_backtest_status(
                    build_auto_backtest_status(
                        run_id="x", status="completed", started_at="2026-10-10T22:00:00"
                    )
                )
            )


if __name__ == "__main__":
    unittest.main()
