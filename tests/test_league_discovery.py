from collections import deque
import json

import NanoTest as bot


def _isolate_league_state(monkeypatch):
    saves = []
    monkeypatch.setattr(bot, "leagues_state", {})
    monkeypatch.setattr(bot, "league_update_queue", deque())
    monkeypatch.setattr(bot, "league_update_set", set())
    monkeypatch.setattr(bot, "save_leagues_state", lambda: saves.append(True))
    return saves


def _fixture(league_id, name, *, country="Testland", season=2026, league_type="League"):
    return {
        "fixture": {"id": league_id * 100},
        "league": {
            "id": league_id,
            "name": name,
            "country": country,
            "season": season,
            "type": league_type,
        },
    }


def test_live_discovery_registers_all_unique_leagues_before_filters(monkeypatch):
    saves = _isolate_league_state(monkeypatch)
    fixtures = [
        _fixture(101, "Premier Division"),
        _fixture(202, "Regional Division"),
        _fixture(202, "Regional Division"),
    ]

    summary = bot.discover_live_leagues(fixtures)

    assert summary["fixtures"] == 3
    assert summary["unique_leagues"] == 2
    assert summary["new"] == 2
    assert set(bot.leagues_state) == {"101", "202"}
    assert list(bot.league_update_queue) == [101, 202]
    assert bot.league_update_set == {101, 202}
    assert len(saves) == 2

    bot.discover_live_leagues(fixtures)
    assert list(bot.league_update_queue) == [101, 202]


def test_live_discovery_does_not_queue_fresh_known_league(monkeypatch):
    _isolate_league_state(monkeypatch)
    record = bot._league_default_record(
        303,
        name="Known League",
        country="Testland",
        season=2026,
        league_type="League",
    )
    record["last_updated_utc"] = bot._utc_now_iso()
    bot.leagues_state["303"] = record

    summary = bot.discover_live_leagues([_fixture(303, "Known League")])

    assert summary["known"] == 1
    assert list(bot.league_update_queue) == []


def test_v2_context_registers_missing_league_as_fallback(monkeypatch):
    _isolate_league_state(monkeypatch)
    metrics = {
        "league_id": 404,
        "league_name": "Lower League",
        "league_country": "Testland",
        "league_season": 2026,
        "league_type": "League",
    }

    context = bot._get_v2_league_context(metrics)

    assert context["league_id"] == 404
    assert context["league_factor_source"] == "default"
    assert bot.leagues_state["404"]["name"] == "Lower League"
    assert list(bot.league_update_queue) == [404]


def test_bootstrap_registers_leagues_from_second_half_stats(monkeypatch, tmp_path):
    _isolate_league_state(monkeypatch)
    stats_path = tmp_path / "league_2h_stats.json"
    stats_path.write_text(
        json.dumps(
            {
                "_meta": {"history_records": 10},
                "leagues": {
                    "505": {
                        "league_id": 505,
                        "league_name": "Historic League",
                        "league_type": "League",
                    },
                    "606": {
                        "league_id": 606,
                        "league_name": "Historic Cup",
                        "league_type": "Cup",
                    },
                },
            }
        ),
        encoding="utf-8",
    )

    summary = bot.bootstrap_leagues_from_2h_stats(str(stats_path))

    assert summary["records"] == 2
    assert summary["new"] == 2
    assert set(bot.leagues_state) == {"505", "606"}
    assert bot.leagues_state["606"]["type"] == "Cup"
    assert list(bot.league_update_queue) == [505, 606]
