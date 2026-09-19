from __future__ import annotations

import json
from pathlib import Path

import pytest

from market_benchmark import AppendOnlyMarketJournal, market_journal_paths
from scripts import report_market_benchmark as report


def _write_jsonl(path: Path, records: list[dict]) -> None:
    path.write_text(
        "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records),
        encoding="utf-8",
    )


def _quote(
    fixture_id: int,
    observation_id: str,
    probability: float,
    *,
    captured_at: str = "2026-09-08T12:00:00+00:00",
    bookmaker: bool = False,
) -> dict:
    market = {
        "market_type": "one_more_goal_to90",
        "scope": "normal_time",
        "current_goals": 1,
        "target_line": 1.5,
        "bet_id": 5,
        "bet_name": "Goals Over/Under",
        "api_updated_at_utc": "2026-09-08T11:59:59+00:00",
        "fair_probability_goal_to90": probability,
    }
    if bookmaker:
        market.update({"bookmaker_id": 8, "bookmaker_name": "Example"})
    return {
        "record_type": "market_benchmark_snapshot",
        "schema_version": 1,
        "record_key": f"quote:{fixture_id}:{observation_id}",
        "fixture_id": fixture_id,
        "score_home": 1,
        "score_away": 0,
        "observation_id": observation_id,
        "captured_at_utc": captured_at,
        "provider": "api_football",
        "market": market,
    }


def _model(
    probability_pct: float,
    *,
    observation_id: str,
    fixture_id: int,
    source_name: str,
    cutoff_after_observation: bool = False,
) -> dict:
    fingerprint = f"fingerprint:{source_name}:{fixture_id}:50"
    return {
        "status": "ok",
        "probability_pct": probability_pct,
        "model_data_cutoff_utc": (
            "2026-09-08T12:00:01+00:00"
            if cutoff_after_observation
            else "2026-09-08T10:00:00+00:00"
        ),
        "model_created_at_utc": "2026-09-08T11:00:00+00:00",
        "prediction_created_at_utc": "2026-09-08T12:00:01+00:00",
        "prediction_input_observation_id": observation_id,
        "prediction_input_fixture_id": fixture_id,
        "prediction_input_minute": 50,
        "prediction_input_fingerprint": fingerprint,
        "expected_input_fingerprint": fingerprint,
        "input_identity_method": "predictive_input_fingerprint_v1",
    }


def _decision(
    fixture_id: int,
    observation_id: str,
    *,
    bot: float,
    static: float,
    rolling: float,
    status: str = "available",
    created_at: str = "2026-09-08T12:00:02+00:00",
    quote_key: str | None = None,
    static_cutoff_after_observation: bool = False,
) -> dict:
    static_fingerprint = f"fingerprint:static_ml:{fixture_id}:50"
    rolling_fingerprint = f"fingerprint:rolling_ml:{fixture_id}:50"
    return {
        "record_type": "market_benchmark_decision",
        "schema_version": 1,
        "decision_key": f"benchmark:{observation_id}",
        "fixture_id": fixture_id,
        "observation_id": observation_id,
        "stage": "decision_pipeline",
        "minute": 50,
        "observation_created_at_utc": "2026-09-08T12:00:00+00:00",
        "created_at_utc": created_at,
        "match": {"score_home": 1, "score_away": 0},
        "prediction_input_fingerprints": {
            "static_ml": static_fingerprint,
            "rolling_ml": rolling_fingerprint,
        },
        "probabilities": {
            "bot": {"probability_pct": bot},
            "static_ml": _model(
                static,
                observation_id=observation_id,
                fixture_id=fixture_id,
                source_name="static_ml",
                cutoff_after_observation=static_cutoff_after_observation,
            ),
            "rolling_ml": _model(
                rolling,
                observation_id=observation_id,
                fixture_id=fixture_id,
                source_name="rolling_ml",
            ),
        },
        "market": {
            "status": status,
            "quote_record_key": quote_key or f"quote:{fixture_id}:{observation_id}",
        },
        "shadow_only": True,
        "production_applied": False,
    }


def _outcome(
    fixture_id: int,
    observation_id: str,
    label: bool,
    *,
    schema_version: int = 1,
) -> dict:
    return {
        "record_type": "market_benchmark_outcome",
        "observation_id": observation_id,
        "fixture_id": fixture_id,
        "outcome_schema_version": schema_version,
        "created_at_utc": f"2026-09-09T12:00:0{schema_version}+00:00",
        "outcome": {
            "status": "resolved",
            "outcome_scope": "TO_90_NORMAL_TIME",
            "goal_to90_normal_time": label,
            "resolved_at_utc": "2026-09-09T12:00:00+00:00",
        },
    }


def _delivery(
    fixture_id: int,
    observation_id: str,
    *,
    started_at: str = "2026-09-08T12:00:03+00:00",
    finished_at: str = "2026-09-08T12:00:04+00:00",
    recorded_at: str = "2026-09-08T12:00:04.100000+00:00",
) -> dict:
    return {
        "record_type": "market_benchmark_delivery",
        "schema_version": 1,
        "record_key": f"delivery:{observation_id}",
        "observation_id": observation_id,
        "fixture_id": fixture_id,
        "captured_at_utc": recorded_at,
        "telegram": {
            "send_attempted": True,
            "send_ok": True,
            "message_id": 7,
            "send_started_at_utc": started_at,
            "send_finished_at_utc": finished_at,
        },
        "shadow_only": True,
        "production_applied": False,
    }


def test_reports_all_four_sources_on_exact_same_cohort(tmp_path: Path) -> None:
    journal = tmp_path / "market.jsonl"
    rows: list[dict] = []
    for fixture_id, label, values in (
        (1, True, (80.0, 90.0, 70.0, 0.75)),
        (2, False, (20.0, 10.0, 30.0, 0.25)),
    ):
        observation_id = f"{fixture_id}:50:WINDOW_1:v2"
        rows.extend(
            [
                _quote(fixture_id, observation_id, values[3]),
                _decision(
                    fixture_id,
                    observation_id,
                    bot=values[0],
                    static=values[1],
                    rolling=values[2],
                ),
                _outcome(fixture_id, observation_id, label),
            ]
        )
    _write_jsonl(journal, rows)

    result = report.build_report(journal)
    cohort = result["metrics"]["all_sources_same_cohort"]

    assert result["coverage"]["decision_records"] == 2
    assert result["coverage"]["all_sources_aligned_decisions"] == 2
    assert cohort["sources"]["bot"]["brier"] == pytest.approx(0.04)
    assert cohort["sources"]["static_ml"]["brier"] == pytest.approx(0.01)
    assert cohort["sources"]["rolling_ml"]["brier"] == pytest.approx(0.09)
    assert cohort["sources"]["market"]["brier"] == pytest.approx(0.0625)
    assert result["metrics"]["paired_vs_market"]["static_ml"][
        "lower_log_loss"
    ] == "static_ml"


def test_confirmed_publications_have_a_separate_same_cohort_view(
    tmp_path: Path,
) -> None:
    journal = tmp_path / "market-publications.jsonl"
    sent_id = "1:50:WINDOW_1:ALLOW:v2"
    blocked_id = "2:50:WINDOW_1:BLOCK:v2"
    sent_decision = _decision(
        1,
        sent_id,
        bot=80.0,
        static=90.0,
        rolling=70.0,
    )
    sent_decision["decision"] = {"final_decision": "ALLOW"}
    _write_jsonl(
        journal,
        [
            _quote(1, sent_id, 0.75),
            sent_decision,
            _delivery(1, sent_id),
            _outcome(1, sent_id, True),
            _quote(2, blocked_id, 0.25),
            _decision(
                2,
                blocked_id,
                bot=20.0,
                static=10.0,
                rolling=30.0,
            ),
            _outcome(2, blocked_id, False),
        ],
    )

    result = report.build_report(journal)
    publication = result["metrics"]["confirmed_publications_same_cohort"]

    assert result["coverage"]["decision_records"] == 2
    assert result["coverage"]["publication_intent_decisions"] == 1
    assert result["coverage"]["confirmed_publications"] == 1
    assert result["coverage"]["resolved_confirmed_publications"] == 1
    assert result["coverage"][
        "all_sources_aligned_confirmed_publications"
    ] == 1
    assert publication["rows"] == 1
    assert publication["fixtures"] == 1
    assert publication["sources"]["bot"]["brier"] == pytest.approx(0.04)
    assert publication["sources"]["market"]["brier"] == pytest.approx(0.0625)


def test_unavailable_market_stays_in_coverage_denominator(tmp_path: Path) -> None:
    journal = tmp_path / "market.jsonl"
    first_id = "1:50:WINDOW_1:v2"
    second_id = "2:50:WINDOW_1:v2"
    records = [
        _quote(1, first_id, 0.7),
        _decision(1, first_id, bot=70, static=70, rolling=70),
        _decision(
            2,
            second_id,
            bot=60,
            static=60,
            rolling=60,
            status="no_matching_market",
        ),
        _outcome(1, first_id, True),
        _outcome(2, second_id, False),
    ]
    _write_jsonl(journal, records)

    result = report.build_report(journal)

    assert result["coverage"]["decision_records"] == 2
    assert result["coverage"]["resolved_decisions"] == 2
    assert result["coverage"]["source_available_on_resolved"]["market"] == 1
    assert result["coverage"]["source_coverage_pct_of_all_decisions"]["market"] == 50.0
    assert result["skipped"]["source_reasons_on_resolved"]["market"][
        "no_matching_market"
    ] == 1


def test_pending_outcome_does_not_remove_decision_from_coverage(tmp_path: Path) -> None:
    journal = tmp_path / "market.jsonl"
    observation_id = "1:50:WINDOW_1:v2"
    _write_jsonl(
        journal,
        [
            _quote(1, observation_id, 0.7),
            _decision(1, observation_id, bot=70, static=70, rolling=70),
        ],
    )

    result = report.build_report(journal)

    assert result["coverage"]["decision_records"] == 1
    assert result["coverage"]["resolved_decisions"] == 0
    assert result["coverage"]["source_available_on_all_decisions"] == {
        "bot": 1,
        "static_ml": 1,
        "rolling_ml": 1,
        "market": 1,
    }
    assert result["coverage"]["source_coverage_pct_of_all_decisions"]["market"] == 100.0


def test_earliest_decision_is_frozen_even_if_later_retry_has_market(
    tmp_path: Path,
) -> None:
    journal = tmp_path / "market.jsonl"
    observation_id = "1:50:WINDOW_1:v2"
    unavailable = _decision(
        1,
        observation_id,
        bot=70,
        static=70,
        rolling=70,
        status="unavailable",
        created_at="2026-09-08T12:00:01+00:00",
    )
    available_retry = _decision(
        1,
        observation_id,
        bot=70,
        static=70,
        rolling=70,
        status="available",
        created_at="2026-09-08T12:00:02+00:00",
    )
    _write_jsonl(
        journal,
        [
            _quote(1, observation_id, 0.7),
            unavailable,
            available_retry,
            _outcome(1, observation_id, True),
        ],
    )

    result = report.build_report(journal)

    assert result["skipped"]["decision_duplicates"] == 1
    assert result["coverage"]["source_available_on_resolved"]["market"] == 0
    assert result["methodology"]["selection_uses_outcome"] is False


def test_rejects_ml_whose_training_cutoff_is_after_observation(
    tmp_path: Path,
) -> None:
    journal = tmp_path / "market.jsonl"
    observation_id = "1:50:WINDOW_1:v2"
    _write_jsonl(
        journal,
        [
            # File ordering cannot leak the outcome into record selection.
            _outcome(1, observation_id, True),
            _quote(1, observation_id, 0.7),
            _decision(
                1,
                observation_id,
                bot=70,
                static=99,
                rolling=70,
                static_cutoff_after_observation=True,
            ),
        ],
    )

    result = report.build_report(journal)

    assert result["coverage"]["source_available_on_resolved"]["static_ml"] == 0
    assert result["coverage"]["source_available_on_resolved"]["rolling_ml"] == 1
    assert result["skipped"]["source_reasons_on_resolved"]["static_ml"][
        "model_cutoff_not_before_observation"
    ] == 1
    assert result["coverage"]["all_sources_aligned_decisions"] == 0


def test_quote_outside_alignment_window_is_not_scored(tmp_path: Path) -> None:
    journal = tmp_path / "market.jsonl"
    observation_id = "1:50:WINDOW_1:v2"
    _write_jsonl(
        journal,
        [
            _quote(
                1,
                observation_id,
                0.99,
                captured_at="2026-09-08T11:55:00+00:00",
            ),
            _decision(1, observation_id, bot=70, static=70, rolling=70),
            _outcome(1, observation_id, True),
        ],
    )

    result = report.build_report(journal, max_alignment_seconds=120)

    assert result["coverage"]["source_available_on_resolved"]["market"] == 0
    assert result["skipped"]["source_reasons_on_resolved"]["market"][
        "quote_outside_alignment_window"
    ] == 1


def test_quote_captured_after_observation_is_not_scored(tmp_path: Path) -> None:
    journal = tmp_path / "market.jsonl"
    observation_id = "1:50:WINDOW_1:v2"
    _write_jsonl(
        journal,
        [
            _quote(
                1,
                observation_id,
                0.99,
                captured_at="2026-09-08T12:00:00.001000+00:00",
            ),
            _decision(1, observation_id, bot=70, static=70, rolling=70),
            _outcome(1, observation_id, True),
        ],
    )

    result = report.build_report(journal, future_tolerance_seconds=5)

    assert result["coverage"]["source_available_on_resolved"]["market"] == 0
    assert result["skipped"]["source_reasons_on_resolved"]["market"][
        "quote_after_observation"
    ] == 1


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("stage", None, "missing_stage"),
        ("stage", "wide_monitor", "stage"),
        ("minute", None, "missing_minute"),
        ("minute", "bad", "invalid_minute"),
        ("minute", 61, "minute_outside_46_60"),
    ],
)
def test_decision_scope_fields_are_mandatory(
    tmp_path: Path, field: str, value: object, reason: str
) -> None:
    journal = tmp_path / f"market-{reason}.jsonl"
    observation_id = "1:50:WINDOW_1:v2"
    decision = _decision(1, observation_id, bot=70, static=70, rolling=70)
    if value is None:
        decision.pop(field)
    else:
        decision[field] = value
    _write_jsonl(
        journal,
        [
            _quote(1, observation_id, 0.7),
            decision,
            _outcome(1, observation_id, True),
        ],
    )

    result = report.build_report(journal)

    assert result["coverage"]["decision_records"] == 0
    assert result["skipped"]["decision_scope_excluded"][reason] == 1


def test_outcome_after_observation_but_before_decision_is_not_scored(
    tmp_path: Path,
) -> None:
    journal = tmp_path / "market.jsonl"
    observation_id = "1:50:WINDOW_1:v2"
    outcome = _outcome(1, observation_id, True)
    outcome["outcome"]["resolved_at_utc"] = (
        "2026-09-08T12:00:01.500000+00:00"
    )
    _write_jsonl(
        journal,
        [
            _quote(1, observation_id, 0.7),
            _decision(1, observation_id, bot=70, static=70, rolling=70),
            outcome,
        ],
    )

    result = report.build_report(journal)

    assert result["coverage"]["resolved_decisions"] == 0
    assert result["coverage"]["outcomes"][
        "non_prospective_outcome_timing"
    ] == 1


def test_prediction_one_microsecond_after_decision_is_not_scored(
    tmp_path: Path,
) -> None:
    journal = tmp_path / "market.jsonl"
    observation_id = "1:50:WINDOW_1:v2"
    decision = _decision(1, observation_id, bot=70, static=70, rolling=70)
    decision["probabilities"]["static_ml"][
        "prediction_created_at_utc"
    ] = "2026-09-08T12:00:02.000001+00:00"
    _write_jsonl(
        journal,
        [
            _quote(1, observation_id, 0.7),
            decision,
            _outcome(1, observation_id, True),
        ],
    )

    result = report.build_report(journal)

    assert result["coverage"]["source_available_on_resolved"]["static_ml"] == 0
    assert result["skipped"]["source_reasons_on_resolved"]["static_ml"][
        "prediction_after_frozen_decision"
    ] == 1


@pytest.mark.parametrize(
    ("mutation", "reason"),
    [
        ("missing_provider_time", "provider_update_timestamp_missing"),
        ("provider_after_observation", "api_update_after_observation"),
        ("wrong_market_type", "wrong_market_type"),
        ("wrong_scope", "wrong_market_scope"),
        ("missing_identity", "market_identity_missing"),
    ],
)
def test_market_contract_is_fail_closed(
    tmp_path: Path, mutation: str, reason: str
) -> None:
    journal = tmp_path / f"market-{mutation}.jsonl"
    observation_id = "1:50:WINDOW_1:v2"
    quote = _quote(1, observation_id, 0.7)
    if mutation == "missing_provider_time":
        quote["market"].pop("api_updated_at_utc")
    elif mutation == "provider_after_observation":
        quote["market"]["api_updated_at_utc"] = (
            "2026-09-08T12:00:00.000001+00:00"
        )
    elif mutation == "wrong_market_type":
        quote["market"]["market_type"] = "match_result"
    elif mutation == "wrong_scope":
        quote["market"]["scope"] = "extra_time"
    else:
        quote["market"].pop("target_line")
    _write_jsonl(
        journal,
        [
            quote,
            _decision(1, observation_id, bot=70, static=70, rolling=70),
            _outcome(1, observation_id, True),
        ],
    )

    result = report.build_report(journal)

    assert result["coverage"]["source_available_on_resolved"]["market"] == 0
    assert result["skipped"]["source_reasons_on_resolved"]["market"][reason] == 1


def test_quote_score_must_match_frozen_decision(tmp_path: Path) -> None:
    journal = tmp_path / "market.jsonl"
    observation_id = "1:50:WINDOW_1:v2"
    quote = _quote(1, observation_id, 0.70)
    quote.update({"score_home": 1, "score_away": 0})
    decision = _decision(1, observation_id, bot=70, static=70, rolling=70)
    decision["match"] = {"score_home": 0, "score_away": 1}
    _write_jsonl(
        journal,
        [quote, decision, _outcome(1, observation_id, True)],
    )

    result = report.build_report(journal)

    assert result["coverage"]["source_available_on_resolved"]["market"] == 0
    assert result["skipped"]["source_reasons_on_resolved"]["market"][
        "quote_score_mismatch"
    ] == 1


def test_latest_outcome_schema_wins_without_reselecting_decision(
    tmp_path: Path,
) -> None:
    journal = tmp_path / "market.jsonl"
    observation_id = "1:50:WINDOW_1:v2"
    _write_jsonl(
        journal,
        [
            _quote(1, observation_id, 0.8),
            _decision(1, observation_id, bot=80, static=80, rolling=80),
            _outcome(1, observation_id, False, schema_version=1),
            _outcome(1, observation_id, True, schema_version=2),
        ],
    )

    result = report.build_report(journal)
    market = result["metrics"]["all_sources_same_cohort"]["sources"]["market"]

    assert market["actual_rate"] == 1.0
    assert market["brier"] == pytest.approx(0.04)


def test_supports_separate_outcome_journal_and_optional_bookmaker(
    tmp_path: Path,
) -> None:
    journal = tmp_path / "market.jsonl"
    outcomes = tmp_path / "outcomes.jsonl"
    observation_id = "1:50:WINDOW_1:v2"
    _write_jsonl(
        journal,
        [
            _quote(1, observation_id, 0.7, bookmaker=True),
            _decision(1, observation_id, bot=70, static=70, rolling=70),
        ],
    )
    _write_jsonl(outcomes, [_outcome(1, observation_id, True)])

    result = report.build_report(journal, outcomes)

    assert result["coverage"]["resolved_decisions"] == 1
    assert result["breakdown"]["bookmaker_dimension_available"] is True
    assert result["breakdown"]["by_bookmaker"]["8:Example"]["rows"] == 1
    assert result["breakdown"]["by_provider"]["api_football"]["rows"] == 1
    assert result["breakdown"]["by_market"]["5:Goals Over/Under"]["rows"] == 1


def test_canonical_observation_outcome_recovers_missing_market_outcome(
    tmp_path: Path,
) -> None:
    journal = tmp_path / "market.jsonl"
    outcomes = tmp_path / "observations.jsonl"
    observation_id = "1:50:WINDOW_1:v2"
    _write_jsonl(
        journal,
        [
            _quote(1, observation_id, 0.7),
            _decision(1, observation_id, bot=70, static=70, rolling=70),
        ],
    )
    canonical = _outcome(1, observation_id, True)
    canonical["record_type"] = "observation_outcome"
    _write_jsonl(outcomes, [canonical])

    result = report.build_report(journal, outcomes)

    assert result["coverage"]["resolved_decisions"] == 1
    assert result["coverage"]["all_sources_aligned_decisions"] == 1


def test_canonical_live_schema_does_not_invent_bookmaker(tmp_path: Path) -> None:
    journal = tmp_path / "market.jsonl"
    observation_id = "1:50:WINDOW_1:v2"
    _write_jsonl(
        journal,
        [
            _quote(1, observation_id, 0.7),
            _decision(1, observation_id, bot=70, static=70, rolling=70),
            _outcome(1, observation_id, True),
        ],
    )

    result = report.build_report(journal)

    assert result["breakdown"]["bookmaker_dimension_available"] is False
    assert result["breakdown"]["by_bookmaker"] == {}
    assert "Bookmaker breakdown: unavailable" in report.render_text(result)


def test_cli_refuses_to_overwrite_source(tmp_path: Path) -> None:
    journal = tmp_path / "market.jsonl"
    _write_jsonl(journal, [])

    with pytest.raises(SystemExit):
        report.main(["--journal", str(journal), "--output", str(journal)])


def test_cli_refuses_to_overwrite_rotated_input_archive(tmp_path: Path) -> None:
    journal_path = tmp_path / "market.jsonl"
    journal = AppendOnlyMarketJournal(journal_path, rotate_max_bytes=250)
    records = []
    for index in range(3):
        record = _quote(index + 1, f"{index + 1}:50:WINDOW_1:v2", 0.7)
        record["record_key"] = f"quote:{index}"
        record["shadow_only"] = True
        record["production_applied"] = False
        records.append(record)
    journal.append_many(records)
    archive = next(
        Path(path)
        for path in market_journal_paths(journal_path)
        if path.endswith(".gz")
    )

    with pytest.raises(SystemExit):
        report.main(
            [
                "--journal",
                str(journal_path),
                "--output",
                str(archive),
            ]
        )


def test_report_reads_rotated_journal_archives(tmp_path: Path) -> None:
    journal_path = tmp_path / "market.jsonl"
    observation_id = "1:50:WINDOW_1:v2"
    records = [
        _quote(1, observation_id, 0.7),
        _decision(1, observation_id, bot=70, static=70, rolling=70),
        _outcome(1, observation_id, True),
    ]
    for index, record in enumerate(records):
        record.setdefault("record_key", f"rotated:{index}")
        record["shadow_only"] = True
        record["production_applied"] = False
    journal = AppendOnlyMarketJournal(
        journal_path,
        rotate_max_bytes=450,
    )

    assert journal.append_many(records) == (True, True, True)

    result = report.build_report(journal_path)
    assert result["coverage"]["decision_records"] == 1
    assert result["coverage"]["resolved_decisions"] == 1
    assert result["coverage"]["source_available_on_resolved"]["market"] == 1


def test_reports_later_same_score_market_movement(tmp_path: Path) -> None:
    journal = tmp_path / "market.jsonl"
    observation_id = "1:50:WINDOW_1:v2"
    initial = _quote(
        1,
        observation_id,
        0.70,
        captured_at="2026-09-08T12:00:00+00:00",
    )
    initial["market"]["decimal_odds"] = {"over": 1.50, "under": 2.70}
    later = _quote(
        1,
        "later-snapshot",
        0.75,
        captured_at="2026-09-08T12:05:00+00:00",
    )
    later["market"]["decimal_odds"] = {"over": 1.40, "under": 3.00}
    later["market"]["api_updated_at_utc"] = (
        "2026-09-08T12:04:59+00:00"
    )
    _write_jsonl(
        journal,
        [
            initial,
            later,
            _decision(1, observation_id, bot=70, static=70, rolling=70),
            _outcome(1, observation_id, True),
        ],
    )

    result = report.build_report(journal)
    movement = result["market_movement"]

    assert movement["observed_movements"] == 1
    assert movement["average_fair_probability_delta_pp"] == pytest.approx(5.0)
    assert movement["average_over_decimal_delta"] == pytest.approx(-0.1)
    assert movement["directions"] == {"market_probability_up": 1}
    assert movement["rows"][0]["elapsed_seconds"] == 298.0


def test_market_movement_starts_after_telegram_finished(tmp_path: Path) -> None:
    journal = tmp_path / "market.jsonl"
    observation_id = "1:50:WINDOW_1:v2"
    initial = _quote(1, observation_id, 0.70)
    quote_updated_before_delivery_finished = _quote(
        1,
        "between",
        0.75,
        captured_at="2026-09-08T12:00:05+00:00",
    )
    quote_updated_before_delivery_finished["market"]["api_updated_at_utc"] = (
        "2026-09-08T12:00:03.500000+00:00"
    )
    decision = _decision(1, observation_id, bot=70, static=70, rolling=70)
    decision["decision"] = {"final_decision": "ALLOW"}
    _write_jsonl(
        journal,
        [
            initial,
            quote_updated_before_delivery_finished,
            decision,
            _delivery(1, observation_id),
            _outcome(1, observation_id, True),
        ],
    )

    result = report.build_report(journal)

    assert result["market_movement"]["observed_movements"] == 0
    assert result["market_movement"]["coverage_reasons"] == {
        "no_later_same_state_quote": 1
    }
    assert result["market_movement"]["confirmed_publications"][
        "coverage_reasons"
    ] == {"no_later_same_state_quote": 1}


def test_report_rejects_wrong_prediction_input_fingerprint(
    tmp_path: Path,
) -> None:
    journal = tmp_path / "market.jsonl"
    observation_id = "1:50:WINDOW_1:v2"
    decision = _decision(1, observation_id, bot=70, static=70, rolling=70)
    decision["probabilities"]["static_ml"][
        "prediction_input_fingerprint"
    ] = "different"
    _write_jsonl(
        journal,
        [
            _quote(1, observation_id, 0.7),
            decision,
            _outcome(1, observation_id, True),
        ],
    )

    result = report.build_report(journal)

    assert result["coverage"]["source_available_on_resolved"]["static_ml"] == 0
    assert result["skipped"]["source_reasons_on_resolved"]["static_ml"][
        "prediction_input_fingerprint_mismatch"
    ] == 1


def test_report_rejects_naive_decision_timestamp(tmp_path: Path) -> None:
    journal = tmp_path / "market.jsonl"
    observation_id = "1:50:WINDOW_1:v2"
    decision = _decision(1, observation_id, bot=70, static=70, rolling=70)
    decision["observation_created_at_utc"] = "2026-09-08T12:00:00"
    _write_jsonl(
        journal,
        [
            _quote(1, observation_id, 0.7),
            decision,
            _outcome(1, observation_id, True),
        ],
    )

    result = report.build_report(journal)

    assert result["skipped"]["decision"]["missing_decision_timestamps"] == 1
    assert result["coverage"]["source_available_on_resolved"] == {
        "bot": 0,
        "static_ml": 0,
        "rolling_ml": 0,
        "market": 0,
    }


def test_report_rejects_model_cutoff_after_model_creation(
    tmp_path: Path,
) -> None:
    journal = tmp_path / "market.jsonl"
    observation_id = "1:50:WINDOW_1:v2"
    decision = _decision(1, observation_id, bot=70, static=70, rolling=70)
    decision["probabilities"]["static_ml"][
        "model_data_cutoff_utc"
    ] = "2026-09-08T11:30:00+00:00"
    _write_jsonl(
        journal,
        [
            _quote(1, observation_id, 0.7),
            decision,
            _outcome(1, observation_id, True),
        ],
    )

    result = report.build_report(journal)

    assert result["skipped"]["source_reasons_on_resolved"]["static_ml"][
        "model_cutoff_after_model_creation"
    ] == 1


def test_report_requires_explicit_outcome_scope(tmp_path: Path) -> None:
    journal = tmp_path / "market.jsonl"
    observation_id = "1:50:WINDOW_1:v2"
    outcome = _outcome(1, observation_id, True)
    del outcome["outcome"]["outcome_scope"]
    _write_jsonl(
        journal,
        [
            _quote(1, observation_id, 0.7),
            _decision(1, observation_id, bot=70, static=70, rolling=70),
            outcome,
        ],
    )

    result = report.build_report(journal)

    assert result["coverage"]["resolved_decisions"] == 0
    assert result["coverage"]["outcomes"]["missing_outcome_scope"] == 1


def test_invalid_delivery_does_not_change_frozen_accuracy_cohort(
    tmp_path: Path,
) -> None:
    journal = tmp_path / "market.jsonl"
    observation_id = "1:50:WINDOW_1:v2"
    decision = _decision(1, observation_id, bot=70, static=70, rolling=70)
    decision["decision"] = {"final_decision": "ALLOW"}
    invalid_delivery = _delivery(2, observation_id)
    _write_jsonl(
        journal,
        [
            _quote(1, observation_id, 0.7),
            decision,
            invalid_delivery,
            _outcome(1, observation_id, True),
        ],
    )

    result = report.build_report(journal)

    assert result["coverage"]["all_sources_aligned_decisions"] == 1
    assert result["market_movement"]["coverage_reasons"] == {
        "delivery_fixture_id_mismatch": 1
    }


def test_first_keyed_decision_is_frozen_before_scope_validation(
    tmp_path: Path,
) -> None:
    journal = tmp_path / "market.jsonl"
    observation_id = "1:50:WINDOW_1:v2"
    invalid_first = _decision(
        1,
        observation_id,
        bot=70,
        static=70,
        rolling=70,
    )
    invalid_first["stage"] = "wrong_stage"
    valid_retry = _decision(
        1,
        observation_id,
        bot=70,
        static=70,
        rolling=70,
    )
    _write_jsonl(journal, [invalid_first, valid_retry])

    result = report.build_report(journal)

    assert result["coverage"]["decision_records"] == 0
    assert result["skipped"]["decision_duplicates"] == 1
    assert result["skipped"]["decision_scope_excluded"] == {"stage": 1}


def test_outcome_rank_uses_aware_time_and_falls_back_from_malformed_created(
    tmp_path: Path,
) -> None:
    journal = tmp_path / "market.jsonl"
    observation_id = "1:50:WINDOW_1:v2"
    later = _outcome(1, observation_id, True)
    later["created_at_utc"] = "malformed"
    later["outcome"]["resolved_at_utc"] = "2026-09-09T12:06:00+00:00"
    earlier = _outcome(1, observation_id, False)
    earlier["created_at_utc"] = "2026-09-09T13:05:00+01:00"
    earlier["outcome"]["resolved_at_utc"] = "2026-09-09T13:05:00+01:00"
    _write_jsonl(
        journal,
        [
            _quote(1, observation_id, 0.7),
            _decision(1, observation_id, bot=70, static=70, rolling=70),
            later,
            earlier,
        ],
    )

    result = report.build_report(journal)

    assert result["metrics"]["all_sources_same_cohort"]["sources"]["bot"][
        "actual_rate"
    ] == 1.0


def test_newer_invalid_outcome_revision_cannot_resurrect_old_label(
    tmp_path: Path,
) -> None:
    journal = tmp_path / "market.jsonl"
    observation_id = "1:50:WINDOW_1:v2"
    old_resolved = _outcome(1, observation_id, True, schema_version=3)
    old_resolved["outcome_revision"] = 1
    corrected = _outcome(1, observation_id, False, schema_version=3)
    corrected["outcome_revision"] = 2
    corrected["created_at_utc"] = "malformed"
    corrected["outcome"] = {
        "status": "quarantine",
        "outcome_scope": "TO_90_NORMAL_TIME",
        "goal_to90_normal_time": False,
        "resolved_at_utc": "also-malformed",
        "outcome_integrity_conflict": True,
    }
    _write_jsonl(
        journal,
        [
            _quote(1, observation_id, 0.7),
            _decision(1, observation_id, bot=70, static=70, rolling=70),
            old_resolved,
            corrected,
        ],
    )

    result = report.build_report(journal)

    assert result["coverage"]["resolved_decisions"] == 0
    assert result["coverage"]["outcomes"] == {"invalid_status": 1}


@pytest.mark.parametrize("bad_value", [float("nan"), float("inf"), -float("inf")])
def test_report_rejects_non_finite_timing_limits(
    tmp_path: Path,
    bad_value: float,
) -> None:
    with pytest.raises(ValueError, match="finite and non-negative"):
        report.build_report(
            tmp_path / "market.jsonl",
            max_alignment_seconds=bad_value,
        )


def test_nested_minute_is_validated_and_does_not_crash_report(
    tmp_path: Path,
) -> None:
    journal = tmp_path / "market.jsonl"
    observation_id = "1:50:WINDOW_1:v2"
    decision = _decision(1, observation_id, bot=70, static=70, rolling=70)
    del decision["minute"]
    decision["observation"] = {"minute": 50, "stage": "decision_pipeline"}
    _write_jsonl(
        journal,
        [
            _quote(1, observation_id, 0.7),
            decision,
            _outcome(1, observation_id, True),
        ],
    )

    result = report.build_report(journal)

    assert result["coverage"]["resolved_decisions"] == 1


def test_publication_without_delivery_has_no_market_movement(
    tmp_path: Path,
) -> None:
    journal = tmp_path / "market.jsonl"
    observation_id = "1:50:WINDOW_1:v2"
    later = _quote(
        1,
        "later",
        0.75,
        captured_at="2026-09-08T12:05:00+00:00",
    )
    later["market"]["api_updated_at_utc"] = "2026-09-08T12:04:59+00:00"
    decision = _decision(1, observation_id, bot=70, static=70, rolling=70)
    decision["decision"] = {"final_decision": "ALLOW"}
    _write_jsonl(
        journal,
        [
            _quote(1, observation_id, 0.7),
            later,
            decision,
            _outcome(1, observation_id, True),
        ],
    )

    result = report.build_report(journal)

    assert result["coverage"]["publication_intent_decisions"] == 1
    assert result["coverage"]["confirmed_publications"] == 0
    assert result["market_movement"]["observed_movements"] == 0
    assert result["market_movement"]["coverage_reasons"] == {
        "delivery_missing_for_publication": 1
    }
    assert result["market_movement"]["confirmed_publications"][
        "observed_movements"
    ] == 0


@pytest.mark.parametrize(
    ("send_ok", "message_id", "reason"),
    [
        (False, None, "telegram_send_failed"),
        (True, None, "telegram_message_id_missing"),
    ],
)
def test_failed_or_unconfirmed_delivery_has_no_market_movement(
    tmp_path: Path,
    send_ok: bool,
    message_id: int | None,
    reason: str,
) -> None:
    journal = tmp_path / f"market-{reason}.jsonl"
    observation_id = "1:50:WINDOW_1:v2"
    later = _quote(
        1,
        "later",
        0.75,
        captured_at="2026-09-08T12:05:00+00:00",
    )
    later["market"]["api_updated_at_utc"] = "2026-09-08T12:04:59+00:00"
    decision = _decision(1, observation_id, bot=70, static=70, rolling=70)
    decision["decision"] = {"final_decision": "ALLOW"}
    delivery = _delivery(1, observation_id)
    delivery["telegram"]["send_ok"] = send_ok
    delivery["telegram"]["message_id"] = message_id
    _write_jsonl(
        journal,
        [
            _quote(1, observation_id, 0.7),
            later,
            decision,
            delivery,
            _outcome(1, observation_id, True),
        ],
    )

    result = report.build_report(journal)

    assert result["coverage"]["resolved_decisions"] == 1
    assert result["coverage"]["publication_intent_decisions"] == 1
    assert result["coverage"]["confirmed_publications"] == 0
    assert result["market_movement"]["observed_movements"] == 0
    assert result["market_movement"]["coverage_reasons"] == {reason: 1}
    assert result["metrics"]["confirmed_publications_same_cohort"][
        "rows"
    ] == 0


def test_invalid_utf8_line_is_counted_without_stopping_report(
    tmp_path: Path,
) -> None:
    journal = tmp_path / "market-invalid-utf8.jsonl"
    observation_id = "1:50:WINDOW_1:v2"
    records = [
        _quote(1, observation_id, 0.7),
        _decision(1, observation_id, bot=70, static=70, rolling=70),
        _outcome(1, observation_id, True),
    ]
    valid_payload = b"".join(
        json.dumps(record, separators=(",", ":")).encode("utf-8") + b"\n"
        for record in records
    )
    journal.write_bytes(valid_payload + b'{"record_type":"torn","text":"\xff\n')

    result = report.build_report(journal)

    assert result["coverage"]["resolved_decisions"] == 1
    assert result["skipped"]["malformed_json"] == 1


def test_referenced_quote_with_malformed_time_does_not_use_decision_time(
    tmp_path: Path,
) -> None:
    journal = tmp_path / "market.jsonl"
    observation_id = "1:50:WINDOW_1:v2"
    quote = _quote(1, observation_id, 0.7)
    quote["captured_at_utc"] = "malformed"
    _write_jsonl(
        journal,
        [
            quote,
            _decision(1, observation_id, bot=70, static=70, rolling=70),
            _outcome(1, observation_id, True),
        ],
    )

    result = report.build_report(journal)

    assert result["coverage"]["source_available_on_resolved"]["market"] == 0
    assert result["skipped"]["source_reasons_on_resolved"]["market"][
        "missing_alignment_timestamp"
    ] == 1
