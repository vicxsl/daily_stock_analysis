# -*- coding: utf-8 -*-
"""Backtest repository.

Provides database access helpers for backtest tables.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import and_, case, delete, desc, func, or_, select, update

from src.core.backtest_engine import OVERALL_SENTINEL_CODE
from src.services.stock_code_utils import resolve_daily_stock_identity

from src.storage import BacktestResult, BacktestSummary, DatabaseManager, AnalysisHistory

logger = logging.getLogger(__name__)

MARKET_REVIEW_REPORT_TYPE = "market_review"
RETRYABLE_INCOMPLETE_STATUSES = ("insufficient_data", "error")
# Columns that constitute the substantive content of one evaluation row.
# ``evaluated_at`` alone never counts as a content change.
RESULT_CONTENT_COLUMNS = (
    "analysis_history_id",
    "code",
    "analysis_date",
    "eval_window_days",
    "engine_version",
    "eval_status",
    "operation_advice",
    "position_recommendation",
    "start_price",
    "end_close",
    "max_high",
    "min_low",
    "stock_return_pct",
    "direction_expected",
    "direction_correct",
    "outcome",
    "stop_loss",
    "take_profit",
    "hit_stop_loss",
    "hit_take_profit",
    "first_hit",
    "first_hit_date",
    "first_hit_trading_days",
    "simulated_entry_price",
    "simulated_exit_price",
    "simulated_exit_reason",
    "simulated_return_pct",
)
BacktestResultContextRow = Tuple[
    BacktestResult,
    Optional[str],
    Optional[str],
    Optional[datetime],
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[int],
]


def _result_content_map(result: BacktestResult) -> Dict[str, Any]:
    """Canonical content of one evaluation row, ignoring evaluated_at."""

    content: Dict[str, Any] = {}
    for column in RESULT_CONTENT_COLUMNS:
        value = getattr(result, column, None)
        if isinstance(value, datetime):
            value = value.isoformat()
        elif isinstance(value, date):
            value = value.isoformat()
        content[column] = value
    return content


def _result_content_hash(content: Dict[str, Any]) -> str:
    payload = json.dumps(content, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _write_result_snapshot(
    existing: BacktestResult,
    *,
    revision_root: str,
    reason: str,
) -> Optional[str]:
    """Persist an immutable pre-update snapshot of ``existing``.

    Returns the snapshot path, or None when identical content was already
    snapshotted. Raises on write/verify failure so the caller can skip the
    DB update (snapshot first, update second, never the other way round).
    """

    content = _result_content_map(existing)
    content_hash = _result_content_hash(content)
    directory = os.path.join(
        revision_root,
        f"{existing.analysis_history_id}-{existing.eval_window_days}-{existing.engine_version}",
    )
    os.makedirs(directory, exist_ok=True)
    payload = {
        "snapshot_version": 1,
        "reason": reason,
        "saved_at": datetime.now().isoformat(),
        "key": {
            "id": existing.id,
            "analysis_history_id": existing.analysis_history_id,
            "eval_window_days": existing.eval_window_days,
            "engine_version": existing.engine_version,
        },
        "snapshot_hash": content_hash,
        "evaluated_at": existing.evaluated_at.isoformat() if existing.evaluated_at else None,
        "original": content,
    }
    path = os.path.join(directory, f"{content_hash}.json")
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as handle:
            previous = json.load(handle)
        if isinstance(previous, dict) and previous.get("snapshot_hash") == content_hash:
            return path

    tmp_path = f"{path}.tmp-{os.getpid()}"
    with open(tmp_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, default=str)
    os.replace(tmp_path, path)
    with open(path, "r", encoding="utf-8") as handle:
        verify = json.load(handle)
    if not isinstance(verify, dict) or verify.get("snapshot_hash") != content_hash:
        raise RuntimeError(f"backtest result snapshot verify failed: {path}")
    return path


class BacktestRepository:
    """DB access layer for backtesting."""

    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        self.db = db_manager or DatabaseManager.get_instance()

    def get_candidates(
        self,
        *,
        code: Optional[str],
        min_age_days: int,
        limit: int,
        offset: int = 0,
        eval_window_days: int,
        engine_version: str,
        force: bool,
        retry_incomplete: bool = False,
    ) -> List[AnalysisHistory]:
        """Return AnalysisHistory rows eligible for backtest.

        ``force=False`` (default) keeps the manual semantics: rows that already
        have any result for the same (window, engine) are excluded.
        ``retry_incomplete=True`` additionally re-selects rows whose stored
        result is ``insufficient_data``/``error`` so the automatic post-analysis
        evaluation can complete them on their original key. Completed rows are
        never re-selected by this flag.
        """
        cutoff_dt = datetime.now() - timedelta(days=min_age_days)

        with self.db.get_session() as session:
            conditions = [AnalysisHistory.created_at <= cutoff_dt]
            if code:
                conditions.extend(self._build_code_conditions(AnalysisHistory.code, code))
            conditions.append(
                or_(
                    AnalysisHistory.report_type.is_(None),
                    AnalysisHistory.report_type != MARKET_REVIEW_REPORT_TYPE,
                )
            )

            if force:
                pass
            elif retry_incomplete:
                retryable = (
                    select(BacktestResult.analysis_history_id)
                    .where(
                        and_(
                            BacktestResult.eval_window_days == eval_window_days,
                            BacktestResult.engine_version == engine_version,
                            BacktestResult.eval_status.in_(RETRYABLE_INCOMPLETE_STATUSES),
                        )
                    )
                    .subquery()
                )
                any_result = (
                    select(BacktestResult.analysis_history_id)
                    .where(
                        and_(
                            BacktestResult.eval_window_days == eval_window_days,
                            BacktestResult.engine_version == engine_version,
                        )
                    )
                    .subquery()
                )
                conditions.append(
                    or_(
                        AnalysisHistory.id.not_in(any_result),
                        AnalysisHistory.id.in_(retryable),
                    )
                )
            else:
                existing_ids = select(BacktestResult.analysis_history_id).where(
                    and_(
                        BacktestResult.eval_window_days == eval_window_days,
                        BacktestResult.engine_version == engine_version,
                    )
                )
                query = select(AnalysisHistory).where(and_(*conditions))
                query = query.where(AnalysisHistory.id.not_in(existing_ids))
                query = query.order_by(desc(AnalysisHistory.created_at)).offset(offset).limit(limit)
                rows = session.execute(query).scalars().all()
                return list(rows)

            query = select(AnalysisHistory).where(and_(*conditions))
            if retry_incomplete and not force:
                # Deterministic pagination for the auto retry path.
                query = query.order_by(desc(AnalysisHistory.created_at), desc(AnalysisHistory.id))
            else:
                query = query.order_by(desc(AnalysisHistory.created_at))
            query = query.offset(offset).limit(limit)
            rows = session.execute(query).scalars().all()
            return list(rows)

    def align_existing_result_dates(
        self,
        *,
        code: Optional[str],
        min_age_days: int,
        eval_window_days: int,
        engine_version: str,
        analysis_date_from: Optional[date],
        analysis_date_to: Optional[date],
    ) -> int:
        """Align legacy result dates to their linked analysis snapshot date.

        Older backtest rows may have stored the trading/start daily date instead
        of the historical analysis snapshot date. When a date-filtered run skips
        already-existing rows, those legacy rows would remain invisible to the
        same date-filtered result query. Updating the stored result date keeps
        rerun and query semantics aligned without inserting duplicate rows.
        """
        cutoff_dt = datetime.now() - timedelta(days=min_age_days)

        with self.db.get_session() as session:
            conditions = [
                AnalysisHistory.created_at <= cutoff_dt,
                BacktestResult.eval_window_days == eval_window_days,
                BacktestResult.engine_version == engine_version,
                or_(
                    AnalysisHistory.report_type.is_(None),
                    AnalysisHistory.report_type != MARKET_REVIEW_REPORT_TYPE,
                ),
            ]
            if code:
                conditions.extend(self._build_code_conditions(AnalysisHistory.code, code))

            rows = session.execute(
                select(BacktestResult, AnalysisHistory)
                .join(AnalysisHistory, AnalysisHistory.id == BacktestResult.analysis_history_id)
                .where(and_(*conditions))
            ).all()

            updated = 0
            for result, analysis in rows:
                analysis_date = self.parse_analysis_date_from_snapshot(analysis.context_snapshot)
                if analysis_date is None and analysis.created_at is not None:
                    analysis_date = analysis.created_at.date()
                if analysis_date is None:
                    continue
                if analysis_date_from is not None and analysis_date < analysis_date_from:
                    continue
                if analysis_date_to is not None and analysis_date > analysis_date_to:
                    continue
                if result.analysis_date != analysis_date:
                    result.analysis_date = analysis_date
                    updated += 1

            if updated:
                session.commit()
            return updated

    def save_result(self, result: BacktestResult) -> None:
        with self.db.get_session() as session:
            session.add(result)
            session.commit()

    def save_results_batch(self, results: List[BacktestResult], *, replace_existing: bool = False) -> int:
        if not results:
            return 0

        with self.db.get_session() as session:
            try:
                if replace_existing:
                    analysis_ids = sorted({r.analysis_history_id for r in results if r.analysis_history_id is not None})
                    key_pairs = sorted({(r.eval_window_days, r.engine_version) for r in results})

                    if analysis_ids and key_pairs:
                        for window_days, engine_version in key_pairs:
                            session.execute(
                                delete(BacktestResult).where(
                                    and_(
                                        BacktestResult.analysis_history_id.in_(analysis_ids),
                                        BacktestResult.eval_window_days == window_days,
                                        BacktestResult.engine_version == engine_version,
                                    )
                                )
                            )

                session.add_all(results)
                session.commit()
                return len(results)
            except Exception as exc:
                session.rollback()
                logger.error(f"批量保存回测结果失败: {exc}")
                raise

    def save_results_idempotent(
        self,
        results: List[BacktestResult],
        *,
        revision_root: str,
        reason: str = "auto_retry_incomplete_update",
    ) -> Dict[str, int]:
        """Per-row transactional upsert that preserves the original row id.

        Contract for the automatic incomplete-retry path:
        - no current row -> INSERT (``inserted``);
        - current row is ``completed`` -> skip without writes (``skipped_completed``);
        - same canonical content (``evaluated_at`` excluded) -> no write (``unchanged``);
        - substantive change -> immutable pre-update snapshot first, then a
          conditional UPDATE on the original id whose WHERE excludes ``completed``
          (``updated``; a concurrently completed row yields ``skipped_completed``).

        Snapshot write failure aborts that row's update (row stays intact, the
        error is counted); DB failure keeps any already-written snapshot on disk
        (unapplied snapshots are harmless: content-hash keyed and immutable).
        """
        counters = {
            "inserted": 0,
            "updated": 0,
            "unchanged": 0,
            "skipped_completed": 0,
            "saved": 0,
            "errors": 0,
        }
        if not results:
            return counters

        for result in results:
            try:
                outcome = self._save_result_idempotent_one(result, revision_root=revision_root, reason=reason)
                if outcome == "inserted":
                    counters["inserted"] += 1
                    counters["saved"] += 1
                elif outcome == "updated":
                    counters["updated"] += 1
                    counters["saved"] += 1
                elif outcome == "unchanged":
                    counters["unchanged"] += 1
                elif outcome == "skipped_completed":
                    counters["skipped_completed"] += 1
            except Exception as exc:
                counters["errors"] += 1
                logger.error(
                    "幂等保存回测结果失败 analysis=%s window=%s engine=%s: %s",
                    result.analysis_history_id,
                    result.eval_window_days,
                    result.engine_version,
                    exc,
                )
        return counters

    def _save_result_idempotent_one(
        self,
        result: BacktestResult,
        *,
        revision_root: str,
        reason: str,
    ) -> str:
        new_content = _result_content_map(result)
        with self.db.get_session() as session:
            existing = session.execute(
                select(BacktestResult).where(
                    and_(
                        BacktestResult.analysis_history_id == result.analysis_history_id,
                        BacktestResult.eval_window_days == result.eval_window_days,
                        BacktestResult.engine_version == result.engine_version,
                    )
                ).limit(1)
            ).scalar_one_or_none()

            if existing is None:
                try:
                    session.add(result)
                    session.commit()
                    return "inserted"
                except Exception:
                    session.rollback()
                    # A concurrent writer may have inserted this key first;
                    # re-read and classify against the row that now exists.
                    existing = session.execute(
                        select(BacktestResult).where(
                            and_(
                                BacktestResult.analysis_history_id == result.analysis_history_id,
                                BacktestResult.eval_window_days == result.eval_window_days,
                                BacktestResult.engine_version == result.engine_version,
                            )
                        ).limit(1)
                    ).scalar_one_or_none()
                    if existing is None:
                        raise

            if existing.eval_status == "completed":
                return "skipped_completed"

            if _result_content_map(existing) == new_content:
                return "unchanged"

            _write_result_snapshot(existing, revision_root=revision_root, reason=reason)

            values = {column: getattr(result, column, None) for column in RESULT_CONTENT_COLUMNS}
            values["evaluated_at"] = result.evaluated_at or datetime.now()
            outcome = session.execute(
                update(BacktestResult)
                .where(
                    and_(
                        BacktestResult.id == existing.id,
                        BacktestResult.eval_status != "completed",
                    )
                )
                .values(**values)
            )
            if outcome.rowcount:
                session.commit()
                return "updated"
            session.rollback()
            return "skipped_completed"

    def get_results_paginated(
        self,
        *,
        code: Optional[str],
        eval_window_days: Optional[int] = None,
        engine_version: Optional[str] = None,
        analysis_date_from: Optional[date] = None,
        analysis_date_to: Optional[date] = None,
        days: Optional[int],
        offset: int,
        limit: int,
        code_candidates: Optional[Tuple[str, ...]] = None,
    ) -> Tuple[List[BacktestResultContextRow], int]:
        with self.db.get_session() as session:
            conditions = self._build_result_conditions(
                code=code,
                eval_window_days=eval_window_days,
                engine_version=engine_version,
                analysis_date_from=analysis_date_from,
                analysis_date_to=analysis_date_to,
                days=days,
                code_candidates=code_candidates,
            )

            where_clause = and_(*conditions) if conditions else True

            total = session.execute(
                select(func.count(BacktestResult.id))
                .select_from(BacktestResult)
                .join(AnalysisHistory, AnalysisHistory.id == BacktestResult.analysis_history_id)
                .where(where_clause)
            ).scalar() or 0
            rows = session.execute(
                select(
                    BacktestResult,
                    AnalysisHistory.name,
                    AnalysisHistory.trend_prediction,
                    AnalysisHistory.created_at,
                    AnalysisHistory.context_snapshot,
                    AnalysisHistory.raw_result,
                    AnalysisHistory.report_type,
                    AnalysisHistory.sentiment_score,
                )
                .join(AnalysisHistory, AnalysisHistory.id == BacktestResult.analysis_history_id)
                .where(where_clause)
                .order_by(desc(BacktestResult.analysis_date), desc(BacktestResult.evaluated_at))
                .offset(offset)
                .limit(limit)
            ).all()
            return list(rows), int(total)

    def get_results_with_context_batch(
        self,
        *,
        code: Optional[str],
        eval_window_days: Optional[int] = None,
        engine_version: Optional[str] = None,
        analysis_date_from: Optional[date] = None,
        analysis_date_to: Optional[date] = None,
        days: Optional[int],
        offset: int,
        limit: int,
    ) -> List[BacktestResultContextRow]:
        """Return result rows plus AnalysisHistory.context_snapshot for dynamic filtering."""
        with self.db.get_session() as session:
            conditions = self._build_result_conditions(
                code=code,
                eval_window_days=eval_window_days,
                engine_version=engine_version,
                analysis_date_from=analysis_date_from,
                analysis_date_to=analysis_date_to,
                days=days,
            )
            where_clause = and_(*conditions) if conditions else True
            rows = session.execute(
                select(
                    BacktestResult,
                    AnalysisHistory.name,
                    AnalysisHistory.trend_prediction,
                    AnalysisHistory.created_at,
                    AnalysisHistory.context_snapshot,
                    AnalysisHistory.raw_result,
                    AnalysisHistory.report_type,
                    AnalysisHistory.sentiment_score,
                )
                .join(AnalysisHistory, AnalysisHistory.id == BacktestResult.analysis_history_id)
                .where(where_clause)
                .order_by(desc(BacktestResult.analysis_date), desc(BacktestResult.evaluated_at))
                .offset(offset)
                .limit(limit)
            ).all()
            return list(rows)

    def list_results_with_context(
        self,
        *,
        code: Optional[str],
        eval_window_days: Optional[int] = None,
        engine_version: Optional[str] = None,
        analysis_date_from: Optional[date] = None,
        analysis_date_to: Optional[date] = None,
        days: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> List[Tuple[BacktestResult, Optional[str]]]:
        with self.db.get_session() as session:
            conditions = self._build_result_conditions(
                code=code,
                eval_window_days=eval_window_days,
                engine_version=engine_version,
                analysis_date_from=analysis_date_from,
                analysis_date_to=analysis_date_to,
                days=days,
            )
            where_clause = and_(*conditions) if conditions else True
            query = (
                select(BacktestResult, AnalysisHistory.context_snapshot)
                .join(AnalysisHistory, AnalysisHistory.id == BacktestResult.analysis_history_id)
                .where(where_clause)
                .order_by(desc(BacktestResult.analysis_date), desc(BacktestResult.evaluated_at))
            )
            if limit is not None:
                query = query.limit(limit)
            return list(session.execute(query).all())

    def count_results(
        self,
        *,
        code: Optional[str],
        eval_window_days: Optional[int] = None,
        engine_version: Optional[str] = None,
        analysis_date_from: Optional[date] = None,
        analysis_date_to: Optional[date] = None,
        days: Optional[int] = None,
    ) -> int:
        """Return the number of matching BacktestResult rows without loading them."""
        with self.db.get_session() as session:
            conditions = self._build_result_conditions(
                code=code,
                eval_window_days=eval_window_days,
                engine_version=engine_version,
                analysis_date_from=analysis_date_from,
                analysis_date_to=analysis_date_to,
                days=days,
            )
            where_clause = and_(*conditions) if conditions else True
            count = session.execute(
                select(func.count(BacktestResult.id))
                .select_from(BacktestResult)
                .where(where_clause)
            ).scalar() or 0
            return int(count)

    def list_results(
        self,
        *,
        code: Optional[str],
        eval_window_days: Optional[int] = None,
        engine_version: Optional[str] = None,
        analysis_date_from: Optional[date] = None,
        analysis_date_to: Optional[date] = None,
        days: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> List[BacktestResult]:
        with self.db.get_session() as session:
            conditions = self._build_result_conditions(
                code=code,
                eval_window_days=eval_window_days,
                engine_version=engine_version,
                analysis_date_from=analysis_date_from,
                analysis_date_to=analysis_date_to,
                days=days,
            )
            where_clause = and_(*conditions) if conditions else True
            query = (
                select(BacktestResult)
                .where(where_clause)
                .order_by(desc(BacktestResult.analysis_date), desc(BacktestResult.evaluated_at))
            )
            if limit is not None:
                query = query.limit(limit)
            rows = session.execute(query).scalars().all()
            return list(rows)

    def upsert_summary(self, summary: BacktestSummary) -> None:
        """Insert or replace summary row by unique key."""
        with self.db.get_session() as session:
            existing = session.execute(
                select(BacktestSummary)
                .where(
                    and_(
                        BacktestSummary.scope == summary.scope,
                        BacktestSummary.code == summary.code,
                        BacktestSummary.eval_window_days == summary.eval_window_days,
                        BacktestSummary.engine_version == summary.engine_version,
                    )
                )
                .limit(1)
            ).scalar_one_or_none()

            if existing:
                for attr in (
                    "computed_at",
                    "total_evaluations",
                    "completed_count",
                    "insufficient_count",
                    "long_count",
                    "cash_count",
                    "win_count",
                    "loss_count",
                    "neutral_count",
                    "direction_accuracy_pct",
                    "win_rate_pct",
                    "neutral_rate_pct",
                    "avg_stock_return_pct",
                    "avg_simulated_return_pct",
                    "stop_loss_trigger_rate",
                    "take_profit_trigger_rate",
                    "ambiguous_rate",
                    "avg_days_to_first_hit",
                    "advice_breakdown_json",
                    "diagnostics_json",
                ):
                    setattr(existing, attr, getattr(summary, attr))
                session.commit()
                return

            session.add(summary)
            session.commit()

    def get_summary(
        self,
        *,
        scope: str,
        code: Optional[str],
        eval_window_days: Optional[int] = None,
        engine_version: str,
        code_candidates: Optional[Tuple[str, ...]] = None,
    ) -> Optional[BacktestSummary]:
        with self.db.get_session() as session:
            conditions = [
                BacktestSummary.scope == scope,
                BacktestSummary.engine_version == engine_version,
            ]
            if code or code_candidates is not None:
                conditions.extend(self._build_code_conditions(BacktestSummary.code, code, code_candidates))
            numeric = tuple(value for value in code_candidates or () if value.isdigit())
            if numeric:
                # A numeric rollup has no identity column of its own. Its
                # contributing results must witness this identity, without a
                # conflicting numeric source in the same rollup bucket.
                same_bucket = (
                    BacktestResult.eval_window_days == BacktestSummary.eval_window_days,
                    BacktestResult.engine_version == BacktestSummary.engine_version,
                )
                owned = self._strict_result_code_condition(code_candidates)
                witness = select(BacktestResult.id).where(*same_bucket, owned).correlate(BacktestSummary).exists()
                conflict = select(BacktestResult.id).where(
                    *same_bucket, BacktestResult.code.in_(numeric), ~owned,
                ).correlate(BacktestSummary).exists()
                conditions.append(or_(BacktestSummary.code.not_in(numeric), and_(witness, ~conflict)))
            if eval_window_days is not None:
                conditions.append(BacktestSummary.eval_window_days == eval_window_days)

            row = session.execute(
                select(BacktestSummary)
                .where(and_(*conditions))
                .order_by(desc(BacktestSummary.computed_at))
                .limit(1)
            ).scalar_one_or_none()
            return row

    @staticmethod
    def parse_analysis_date_from_snapshot(context_snapshot: Optional[str]) -> Optional[date]:
        if not context_snapshot:
            return None

        try:
            payload = json.loads(context_snapshot)
        except Exception:
            return None

        if not isinstance(payload, dict):
            return None

        enhanced = payload.get("enhanced_context")
        if not isinstance(enhanced, dict):
            return None

        date_str = enhanced.get("date")
        if not date_str:
            return None

        try:
            return datetime.strptime(str(date_str)[:10], "%Y-%m-%d").date()
        except Exception:
            return None

    def get_distinct_eval_windows(
        self,
        *,
        code: Optional[str],
        engine_version: Optional[str] = None,
        analysis_date_from: Optional[date] = None,
        analysis_date_to: Optional[date] = None,
    ) -> List[int]:
        """Return sorted distinct eval_window_days for matching results."""
        with self.db.get_session() as session:
            conditions = self._build_result_conditions(
                code=code,
                eval_window_days=None,
                engine_version=engine_version,
                analysis_date_from=analysis_date_from,
                analysis_date_to=analysis_date_to,
                days=None,
            )
            where_clause = and_(*conditions) if conditions else True
            rows = session.execute(
                select(BacktestResult.eval_window_days)
                .where(where_clause)
                .distinct()
                .order_by(BacktestResult.eval_window_days)
            ).scalars().all()
            return [int(w) for w in rows if w is not None]

    @staticmethod
    def _build_result_conditions(
        *,
        code: Optional[str],
        eval_window_days: Optional[int],
        engine_version: Optional[str],
        analysis_date_from: Optional[date],
        analysis_date_to: Optional[date],
        days: Optional[int],
        code_candidates: Optional[Tuple[str, ...]] = None,
    ) -> List[object]:
        conditions = []
        if code_candidates is not None:
            conditions.append(BacktestRepository._strict_result_code_condition(code_candidates))
        elif code:
            conditions.extend(BacktestRepository._build_code_conditions(BacktestResult.code, code))
        if eval_window_days is not None:
            conditions.append(BacktestResult.eval_window_days == eval_window_days)
        if engine_version:
            conditions.append(BacktestResult.engine_version == engine_version)
        if analysis_date_from is not None:
            conditions.append(BacktestResult.analysis_date >= analysis_date_from)
        if analysis_date_to is not None:
            conditions.append(BacktestResult.analysis_date <= analysis_date_to)
        if days:
            cutoff = datetime.now() - timedelta(days=int(days))
            conditions.append(BacktestResult.evaluated_at >= cutoff)
        return conditions

    @staticmethod
    def _strict_result_code_condition(code_candidates: Tuple[str, ...]):
        """Use the fixed storage namespace and existing source FK, not an index.

        Qualified keys retain their existing identity. Numeric CN writer keys
        additionally require a same-object source code and no contrary persisted
        market. Missing market uses the writer's bare-CN storage convention.
        """
        numeric = tuple(value for value in code_candidates if value.isdigit())
        if not numeric:
            return BacktestResult.code.in_(code_candidates)
        qualified = tuple(value for value in code_candidates if not value.isdigit())
        snapshot = case((func.json_valid(AnalysisHistory.context_snapshot), AnalysisHistory.context_snapshot), else_="{}")
        market = func.json_extract(snapshot, "$.market_phase_summary.market")
        source = select(AnalysisHistory.id).where(
            AnalysisHistory.id == BacktestResult.analysis_history_id,
            AnalysisHistory.code.in_(code_candidates),
            or_(market.is_(None), func.lower(market) == "cn"),
        ).correlate(BacktestResult).exists()
        return or_(BacktestResult.code.in_(qualified), and_(BacktestResult.code.in_(numeric), source))

    @staticmethod
    def _build_code_conditions(column, code: Optional[str], code_candidates: Optional[Tuple[str, ...]] = None) -> List[object]:
        if code_candidates is not None:
            # An empty explicit constraint means no matches, not legacy/all rows.
            return [column.in_(code_candidates)]
        if not code:
            return []

        raw_code = str(code).strip()
        if raw_code.lower() == OVERALL_SENTINEL_CODE.lower():
            raw_code = OVERALL_SENTINEL_CODE
        else:
            raw_code = raw_code.upper()

        if raw_code == OVERALL_SENTINEL_CODE:
            candidates = [OVERALL_SENTINEL_CODE]
        else:
            identity = resolve_daily_stock_identity(raw_code)
            candidates = list(identity.code_candidates) if identity is not None else []
        if not candidates:
            return [column.in_([])]

        if len(candidates) == 1:
            return [column == candidates[0]]

        unique = list(dict.fromkeys(candidates))
        return [or_(*[column == candidate for candidate in unique])]
