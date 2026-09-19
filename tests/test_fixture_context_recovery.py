import NanoTest as bot


def _live_fixture():
    return {
        "fixture": {
            "id": 123,
            "timestamp": 1770000000,
            "status": {"short": "2H", "elapsed": 52, "extra": 1},
            "venue": {"name": "Test Arena", "city": "Test City"},
        },
        "league": {
            "id": 77,
            "name": "Test League",
            "country": "Testland",
            "season": 2026,
            "type": "League",
        },
        "teams": {
            "home": {"id": 10, "name": "Home FC"},
            "away": {"id": 20, "name": "Away FC"},
        },
        "goals": {"home": 1, "away": 2},
    }


def _raw_stats():
    # Reverse API order to prove that recovered team IDs drive side mapping.
    return [
        {
            "team": {"id": 20, "name": "Away FC"},
            "statistics": [{"type": "Total Shots", "value": 8}],
        },
        {
            "team": {"id": 10, "name": "Home FC"},
            "statistics": [{"type": "Total Shots", "value": 12}],
        },
    ]


def _client(monkeypatch, fetched_fixture):
    client = bot.APISportsMetricsClient.__new__(bot.APISportsMetricsClient)
    monkeypatch.setattr(client, "fetch_fixture", lambda _fixture_id: fetched_fixture)
    monkeypatch.setattr(client, "fetch_fixture_statistics", lambda _fixture_id: _raw_stats())
    monkeypatch.setattr(client, "fetch_fixture_events", lambda _fixture_id: [])
    monkeypatch.setattr(client, "fetch_players", lambda _fixture_id: [])
    monkeypatch.setattr(client, "fetch_shotmap", lambda _fixture_id: [])
    monkeypatch.setattr(client, "fetch_odds", lambda _fixture_id: [])
    monkeypatch.setattr(client, "fetch_lineups", lambda _fixture_id: [])
    monkeypatch.setattr(bot, "update_team_stats_for_fixture", lambda *_args, **_kwargs: None)
    return client


def test_collect_match_all_recovers_context_when_fixture_endpoint_is_empty(monkeypatch):
    client = _client(monkeypatch, {})

    result = client.collect_match_all(123, live_fixture=_live_fixture())
    fixture = result["fixture"]

    assert bot.get_fixture_id(fixture) == 123
    assert fixture["team_home_id"]["value"] == 10
    assert fixture["team_away_id"]["value"] == 20
    assert fixture["team_home_name"]["value"] == "Home FC"
    assert fixture["team_away_name"]["value"] == "Away FC"
    assert fixture["league_id"]["value"] == 77
    assert fixture["league_name"]["value"] == "Test League"
    assert fixture["score_home"]["value"] == 1
    assert fixture["score_away"]["value"] == 2
    assert fixture["status_short"]["value"] == "2H"
    assert fixture["elapsed"]["value"] == 52
    assert fixture["total_shots_home"]["value"] == 12
    assert fixture["total_shots_away"]["value"] == 8


def test_detailed_fixture_values_win_while_missing_identity_uses_live_fallback(monkeypatch):
    fetched_fixture = {
        "fixture": {
            "id": 123,
            "status": {"short": "2H", "elapsed": 53, "extra": 0},
        },
        "goals": {"home": 2, "away": 2},
    }
    client = _client(monkeypatch, fetched_fixture)

    result = client.collect_match_all(123, live_fixture=_live_fixture())
    fixture = result["fixture"]

    assert fixture["elapsed"]["value"] == 53
    assert fixture["score_home"]["value"] == 2
    assert fixture["score_away"]["value"] == 2
    assert fixture["team_home_id"]["value"] == 10
    assert fixture["team_away_id"]["value"] == 20
    assert fixture["league_id"]["value"] == 77


def test_collect_match_all_never_replaces_requested_fixture_id_with_zero(monkeypatch):
    client = _client(monkeypatch, {})

    result = client.collect_match_all(123)

    assert bot.get_fixture_id(result["fixture"]) == 123
