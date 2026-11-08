import time
from unittest.mock import patch
import pandas as pd
from src.services.screening import hotspot
from src.services.screening_service import DsaEastMoneyHotspotProvider


def test_discovery_bounds_constituent_latency_and_reports_partial_source_failure():
    provider = DsaEastMoneyHotspotProvider()
    rows = [{"topic": f"topic{i}", "source": "concept", "heat_score": 50 - i} for i in range(4)]

    def failing_eastmoney(topic, *, source):
        time.sleep(0.2)
        raise TimeoutError("simulated timeout")

    with patch.object(hotspot, "_load_board_summaries", return_value=rows), \
         patch.object(provider, "_fetch_eastmoney_constituents", side_effect=failing_eastmoney), \
         patch.object(provider, "_fetch_ths_constituents", return_value=pd.DataFrame([{"code": "600519", "name": "stock"}])), \
         patch.object(provider, "_fallback_constituents", return_value=pd.DataFrame()), \
         patch.object(provider, "_related_hotspot_constituents", return_value=pd.DataFrame()):
        started = time.monotonic()
        result = hotspot.discover_hotspots(provider=provider, top=4)
        elapsed = time.monotonic() - started

    assert len(result) == 4
    assert elapsed < 0.7, elapsed
    assert any("eastmoney" in item and "topic" in item for item in result.source_errors)
    assert result.fallback_used


def test_hotspot_detail_retains_partial_constituent_failure():
    provider = DsaEastMoneyHotspotProvider()
    frame = pd.DataFrame([{"code": "600519", "name": "stock"}])
    frame.attrs["source_errors"] = ["eastmoney constituents topic: TimeoutError"]
    with patch.object(provider, "_find_board_change", return_value={}), \
         patch.object(provider, "_is_industry_hotspot", return_value=False), \
         patch.object(provider, "stock_board_concept_cons_em", return_value=frame), \
         patch.object(provider, "_fetch_ths_summary_event", return_value=""), \
         patch.object(provider, "_fetch_ths_info", return_value={}):
        detail = provider.hotspot_detail("topic")
    assert detail["source_errors"] == ["eastmoney constituents topic: TimeoutError"]
    assert detail["fallback_used"] is True
    assert detail["leader_stocks"][0]["code"] == "600519"
