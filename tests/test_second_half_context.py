import NanoTest as bot


def _fixture_metrics(*, include_halftime=True):
    metrics = {
        "fixture_id": {"value": 1001},
        "league_id": {"value": 218},
        "team_home_id": {"value": 571},
        "team_away_id": {"value": 601},
        "score_home": {"value": 2},
        "score_away": {"value": 1},
    }
    if include_halftime:
        metrics["score"] = {"halftime": {"home": 0, "away": 1}}
    return metrics


def _patch_neutral_factors(monkeypatch, score_callback):
    monkeypatch.setattr(
        bot,
        "compute_team_2h_factor",
        lambda *_args, **_kwargs: {
            "factor": 1.0,
            "team_sample_home": 10,
            "team_sample_away": 10,
            "events_coverage": 1.0,
        },
    )
    monkeypatch.setattr(
        bot,
        "compute_league_2h_factor",
        lambda *_args, **_kwargs: {"factor": 1.0, "league_sample": 100},
    )
    monkeypatch.setattr(bot, "compute_score_state_factor", score_callback)


def test_second_half_context_uses_halftime_score_not_current_score(monkeypatch):
    captured = []

    def score_factor(home, away, *_args, **_kwargs):
        captured.append((home, away))
        return {"factor": 1.0}

    _patch_neutral_factors(monkeypatch, score_factor)

    context = bot._compute_second_half_context(_fixture_metrics(), minute=65)

    assert captured == [(0, 1)]
    assert context["halftime_score_home"] == 0
    assert context["halftime_score_away"] == 1
    assert context["score_state_source"] == "score.halftime"
    assert context["fallback_reason"] == ""


def test_missing_halftime_score_makes_only_score_factor_neutral(monkeypatch):
    def score_factor(*_args, **_kwargs):
        raise AssertionError("current score must not replace a missing halftime score")

    _patch_neutral_factors(monkeypatch, score_factor)

    context = bot._compute_second_half_context(
        _fixture_metrics(include_halftime=False),
        minute=65,
    )

    assert context["score_state_factor"] == 1.0
    assert context["team_2h_factor"] == 1.0
    assert context["league_2h_factor"] == 1.0
    assert context["score_state_source"] == "missing"
    assert context["fallback_reason"] == "missing_halftime_score"


def test_signal_monitor_defers_empty_fixture_payload_without_parsing(
    monkeypatch,
    caplog,
):
    calls = []

    class EmptyFixtureClient:
        def fetch_fixture(self, fixture_id):
            calls.append(("fixture", fixture_id))
            return {}

        def fetch_fixture_events_response(self, fixture_id):
            raise AssertionError("events must not be fetched for an invalid fixture")

    def fail_parse(*_args, **_kwargs):
        raise AssertionError("invalid fixture must not reach the parser")

    monkeypatch.setattr(bot, "ENABLE_2H_COLLECTION", True)
    monkeypatch.setattr(
        bot,
        "store_second_half_history_payload",
        fail_parse,
    )

    assert (
        bot.collect_second_half_history_for_fixture(EmptyFixtureClient(), 12345)
        is False
    )
    assert calls == [("fixture", 12345)]
    assert "fixture_payload_missing_or_mismatched" in caplog.text
    assert "fixture_id=12345" in caplog.text
