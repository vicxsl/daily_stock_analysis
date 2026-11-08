# -*- coding: utf-8 -*-
"""T1（2026-11-07）：入场区间数量级异常隔离——禁网单元测试。

背景样本：decision_signals id287（513010）——现价 0.526、secondary_buy=3.0
被保存为 entry_high 且 plan_quality=complete。修复后：异常检测写入
metadata.entry_range_anomaly，原始价格字段保留，计划质量至多 partial。

运行（DSA 容器内，禁网）：
    docker exec dsa-server python -m pytest tests/test_entry_range_anomaly.py -q
或 unittest：
    docker exec dsa-server python -m unittest tests.test_entry_range_anomaly -v
"""

import json
import unittest

from src.services.decision_signal_service import (
    ENTRY_RANGE_MAGNITUDE_RATIO,
    DecisionSignalService,
    _entry_range_anomalies,
    _evidence_current_price,
)

# id287 原始形态（2026-11-06 DB 实测值）
SIG_287 = dict(
    entry_low=0.518,
    entry_high=3.0,
    stop_loss=0.51,
    target_price=0.549,
    current_price=0.526,
)


class EntryRangeAnomalyTest(unittest.TestCase):
    def test_id287_shape_detected(self):
        anomalies = _entry_range_anomalies(**SIG_287)
        codes = {a["code"] for a in anomalies}
        self.assertIn("entry_range_magnitude_anomaly", codes)
        self.assertIn("signal_field_inconsistent", codes)
        self.assertIn("target_not_above_range", codes)
        stray = [a for a in anomalies if a["code"] == "signal_field_inconsistent"]
        self.assertEqual(stray[0]["field"], "entry_high")
        self.assertEqual(stray[0]["value"], 3.0)

    def test_normal_pullback_passes(self):
        anomalies = _entry_range_anomalies(
            entry_low=0.511, entry_high=0.518, stop_loss=0.50,
            target_price=0.549, current_price=0.526)
        self.assertEqual(anomalies, [])

    def test_normal_breakout_passes(self):
        anomalies = _entry_range_anomalies(
            entry_low=1.40, entry_high=1.42, stop_loss=1.33,
            target_price=1.55, current_price=1.377)
        self.assertEqual(anomalies, [])

    def test_cross_asset_scale_no_false_positive(self):
        # 高价股与低价 ETF：比率口径不误杀
        self.assertEqual(_entry_range_anomalies(
            entry_low=1450, entry_high=1480, stop_loss=1380,
            target_price=1620, current_price=1462), [])
        self.assertEqual(_entry_range_anomalies(
            entry_low=0.511, entry_high=0.518, stop_loss=0.49,
            target_price=0.56, current_price=0.524), [])

    def test_missing_current_price_still_clusters(self):
        # 无现价：聚簇一致性仍可捕获 id287 形态（排序+聚簇）
        anomalies = _entry_range_anomalies(
            entry_low=0.518, entry_high=3.0, stop_loss=0.51,
            target_price=0.549, current_price=None)
        codes = {a["code"] for a in anomalies}
        self.assertIn("signal_field_inconsistent", codes)
        self.assertNotIn("entry_range_magnitude_anomaly", codes)

    def test_threshold_boundary(self):
        # 恰好 3 倍不触发（严格大于），3.01 倍触发
        self.assertEqual(_entry_range_anomalies(
            entry_low=None, entry_high=None, stop_loss=None,
            target_price=3.0, current_price=1.0), [])
        anomalies = _entry_range_anomalies(
            entry_low=None, entry_high=None, stop_loss=None,
            target_price=3.01, current_price=1.0)
        self.assertTrue(any(a["code"] == "entry_range_magnitude_anomaly" for a in anomalies))

    def test_evidence_current_price_variants(self):
        self.assertEqual(_evidence_current_price({"current_price": 0.526}), 0.526)
        self.assertIsNone(_evidence_current_price({"current_price": None}))
        self.assertIsNone(_evidence_current_price("not-a-dict"))
        self.assertIsNone(_evidence_current_price({"current_price": "garbage"}))
        self.assertIsNone(_evidence_current_price({"current_price": -1}))


class PlanQualityCapTest(unittest.TestCase):
    SVC = None

    @classmethod
    def setUpClass(cls):
        cls.SVC = object.__new__(DecisionSignalService)  # 只用 staticmethod，不触 DB

    # 绕过 __init__（其会初始化 DB）：_normalize_plan_quality 只依赖 staticmethod
    @staticmethod
    def fields_with(anomaly, slots_complete=True):
        fields = {
            "entry_low": 0.518,
            "entry_high": 3.0 if anomaly else 0.526,
            "stop_loss": 0.51,
            "target_price": 0.549,
            "watch_conditions": "观察条件" if slots_complete else None,
            "action": "watch",
            "reason": "理由",
        }
        if anomaly:
            fields["metadata_json"] = json.dumps(
                {"entry_range_anomaly": [{"code": "entry_range_magnitude_anomaly", "field": "entry_high"}]})
        return fields

    def test_anomaly_caps_quality_at_partial(self):
        quality = self.SVC._normalize_plan_quality(None, fields=self.fields_with(anomaly=True))
        self.assertEqual(quality, "partial")

    def test_explicit_complete_also_capped(self):
        quality = self.SVC._normalize_plan_quality(
            "complete", fields=self.fields_with(anomaly=True))
        self.assertEqual(quality, "partial")

    def test_no_anomaly_keeps_slot_semantics(self):
        quality = self.SVC._normalize_plan_quality(None, fields=self.fields_with(anomaly=False))
        self.assertEqual(quality, "complete")

    def test_invalid_metadata_json_not_fatal(self):
        fields = self.fields_with(anomaly=False)
        fields["metadata_json"] = "{not json"
        quality = self.SVC._normalize_plan_quality(None, fields=fields)
        self.assertEqual(quality, "complete", "损坏 metadata 不阻断质量判定（保持原语义）")

    def test_threshold_constant_matches_openclaw_gate(self):
        self.assertEqual(ENTRY_RANGE_MAGNITUDE_RATIO, 3.0)


if __name__ == "__main__":
    unittest.main()
