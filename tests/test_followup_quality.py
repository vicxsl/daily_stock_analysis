from datetime import date, datetime
from unittest.mock import patch
from tests.test_portfolio_price_semantics import SnapshotValuationTestCase as Base, CN_TZ
from src.services.portfolio_service import PortfolioService
class FollowupQualityTest(Base):
    def _case(self, stamp, expected):
        self.service.record_trade(account_id=self.account_id,symbol="600887",trade_date=date.today(),side="buy",quantity=100,price=20)
        quote={"price":21,"provider":"tencent","kind":"realtime","provider_time":stamp,"fetched_at":datetime.now(CN_TZ).isoformat()}
        with patch.object(PortfolioService,"_prefetch_realtime_position_prices",lambda *args,**kwargs:{"600887":quote}): snap=self._snapshot()
        self.assertEqual(snap["data_quality"],expected)
        self.assertEqual(snap["accounts"][0]["positions"][0]["quantity"],100)
        if expected=="partial": self.assertTrue(snap["limitations"])
    def test_unknown_source_time_cannot_aggregate_ok(self): self._case(None,"partial")
    def test_known_valid_time_retains_values(self): self._case(datetime.now(CN_TZ).isoformat(),"ok")


import unittest
from types import SimpleNamespace
from src.services.alert_service import AlertService
from src.services.portfolio_alerts import evaluate_portfolio_risk_alert, PortfolioRiskAlert
class AlertFreshnessTest(unittest.TestCase):
    def test_unknown_quote_does_not_trigger(self):
        s=AlertService.__new__(AlertService)
        rule=SimpleNamespace(stock_code="600887",id=1,metadata={},alert_type="price",direction="below",price=20)
        result=s._freshness_guard(rule,SimpleNamespace(price=19),20)
        self.assertFalse(result["triggered"])
        self.assertEqual(result["record_status"],"degraded")
    def test_unknown_portfolio_quote_does_not_trigger(self):
        rule=PortfolioRiskAlert(target_scope="portfolio",target="all",alert_type="portfolio_stop_loss",parameters={"mode":"breach"})
        risk=SimpleNamespace(get_risk_report=lambda **kw:{"quote_refs":[{"symbol":"600887","time_quality":"unknown"}],"stop_loss":{"items":[{"symbol":"600887","is_triggered":True}]}})
        result=evaluate_portfolio_risk_alert(rule,portfolio_service=SimpleNamespace(),risk_service=risk)
        self.assertFalse(result["triggered"])
        self.assertEqual(result["record_status"],"degraded")
