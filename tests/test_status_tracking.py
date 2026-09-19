from __future__ import annotations

import NanoTest as nanotest


def test_normalize_status_short_unwraps_collected_fixture_value() -> None:
    fixture = {
        "status": {
            "value": {
                "long": "Match Finished",
                "short": "AET",
                "elapsed": 120,
            }
        }
    }

    assert nanotest._normalize_status_short(fixture) == "AET"


def test_normalize_status_short_keeps_direct_api_shape() -> None:
    assert nanotest._normalize_status_short({"status": {"short": "pen"}}) == "PEN"
    assert nanotest._normalize_status_short({"status": "ft"}) == "FT"


def test_startup_prune_closes_stale_live_tracking_record(monkeypatch) -> None:
    fixture_id = 1581037
    fixture_key = str(fixture_id)
    isolated_state = {
        "monitored_matches": [fixture_id],
        "active_fixtures": [fixture_id],
        "excluded_matches": [],
        "tracked_matches": {
            fixture_key: {"status": "LIVE", "finished": False},
        },
        "sent": {
            fixture_key: {"status": "LIVE", "finished": False},
        },
    }
    isolated_matches_state = {
        fixture_key: {
            "is_finished": True,
            "last_status": "{'SHORT': 'AET', 'ELAPSED': 120}",
        }
    }

    monkeypatch.setattr(nanotest, "state", isolated_state)
    monkeypatch.setattr(nanotest, "matches_state", isolated_matches_state)
    monkeypatch.setattr(nanotest, "ACTIVE_FIXTURES", {fixture_id})
    monkeypatch.setattr(nanotest, "mark_state_dirty", lambda: None)

    nanotest.prune_finished_matches_from_tracking()

    assert isolated_state["monitored_matches"] == []
    assert isolated_state["active_fixtures"] == []
    assert isolated_state["tracked_matches"][fixture_key]["finished"] is True
    assert isolated_state["tracked_matches"][fixture_key]["status"] == "FT"
    assert isolated_state["sent"][fixture_key]["finished"] is True
    assert isolated_state["excluded_matches"] == [fixture_id]
    assert nanotest.get_tracked_update_candidates() == []
    assert fixture_id not in nanotest.ACTIVE_FIXTURES
