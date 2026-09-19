from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from wide_research.discovery import (
    DISCOVERY_ENGINE_VERSION,
    PURGED_SPLIT_POLICY,
    TEMPORAL_PURGED_ENGINE_SUFFIX,
    DiscoveryConfig,
    JournalJoinStore,
    _Row,
    _discovery_engine_version,
    _eligible_row,
    _split_fixtures,
)


UTC = timezone.utc


def _row(
    fixture_id: int,
    created_at: datetime,
    *,
    label_available_at: datetime | None,
    suffix: str = "first",
) -> _Row:
    return _Row(
        observation_id=f"{fixture_id}:{suffix}",
        fixture_id=fixture_id,
        created_at=created_at,
        minute=50,
        league_key="test",
        label=1,
        features={},
        label_available_at=label_available_at,
    )


def _fixture_rows() -> dict[int, list[_Row]]:
    start = datetime(2026, 9, 1, tzinfo=UTC)
    fixtures: dict[int, list[_Row]] = {}
    for index in range(12):
        observed = start + timedelta(hours=index)
        fixtures[index] = [
            _row(
                index,
                observed,
                label_available_at=observed + timedelta(minutes=20),
            )
        ]

    # Nominal train fixture 4 has a late observation crossing the validation
    # information cutoff.  Fixture 5 has a target that was not known then.
    fixtures[4].append(
        _row(
            4,
            start + timedelta(hours=6),
            label_available_at=start + timedelta(hours=4, minutes=20),
            suffix="late",
        )
    )
    fixtures[5] = [
        _row(
            5,
            start + timedelta(hours=5),
            label_available_at=start + timedelta(hours=6),
        )
    ]

    # Equivalent failure modes at the validation/holdout boundary.
    fixtures[7].append(
        _row(
            7,
            start + timedelta(hours=8, minutes=58),
            label_available_at=start + timedelta(hours=7, minutes=20),
            suffix="late",
        )
    )
    fixtures[8] = [
        _row(
            8,
            start + timedelta(hours=8),
            label_available_at=start + timedelta(hours=9),
        )
    ]
    return fixtures


def _config(**overrides: object) -> DiscoveryConfig:
    values = {
        "train_fraction": 0.5,
        "validation_fraction": 0.25,
        "temporal_purge": True,
        "temporal_embargo_seconds": 300.0,
    }
    values.update(overrides)
    return DiscoveryConfig(**values)


def test_temporal_purge_uses_complete_fixture_and_known_label_times() -> None:
    fixtures = _fixture_rows()
    splits, metadata = _split_fixtures(fixtures, _config())

    assert set(splits["train"]) == {0, 1, 2, 3}
    assert set(splits["validation"]) == {6}
    assert set(splits["holdout"]) == {9, 10, 11}
    all_ids = [set(splits[name]) for name in ("train", "validation", "holdout")]
    assert all_ids[0].isdisjoint(all_ids[1])
    assert all_ids[0].isdisjoint(all_ids[2])
    assert all_ids[1].isdisjoint(all_ids[2])

    audit = metadata["temporal_purge"]
    assert audit["no_fixture_reallocation"] is True
    assert audit["embargo_seconds"] == 300.0
    assert audit["boundaries"]["train_to_validation"]["purge_reasons"] == {
        "fixture_observation_crosses_cutoff": 1,
        "label_not_available_before_cutoff": 1,
    }
    assert audit["boundaries"]["validation_to_holdout"]["purge_reasons"] == {
        "fixture_observation_crosses_cutoff": 1,
        "label_not_available_before_cutoff": 1,
    }

    for earlier, later in (("train", "validation"), ("validation", "holdout")):
        cutoff = min(
            row.created_at for rows in splits[later].values() for row in rows
        ) - timedelta(minutes=5)
        assert all(
            row.created_at < cutoff and row.label_available_at < cutoff
            for rows in splits[earlier].values()
            for row in rows
        )


def test_holdout_labels_do_not_reshape_or_refill_earlier_splits() -> None:
    fixtures = _fixture_rows()
    config = _config()
    first, first_metadata = _split_fixtures(fixtures, config)
    mutated = {fixture_id: list(rows) for fixture_id, rows in fixtures.items()}
    for fixture_id in (9, 10, 11):
        mutated[fixture_id] = [
            replace(row, label=1 - row.label, label_available_at=row.created_at + timedelta(days=30))
            for row in mutated[fixture_id]
        ]

    second, second_metadata = _split_fixtures(mutated, config)
    assert {name: list(values) for name, values in first.items()} == {
        name: list(values) for name, values in second.items()
    }
    assert first_metadata["temporal_purge"] == second_metadata["temporal_purge"]


def test_temporal_purge_reports_short_dataset_instead_of_reshuffling() -> None:
    start = datetime(2026, 9, 1, tzinfo=UTC)
    fixtures = {
        index: [
            _row(
                index,
                start + timedelta(hours=index),
                label_available_at=start + timedelta(hours=index + 2),
            )
        ]
        for index in range(3)
    }
    with pytest.raises(ValueError, match=r"temporal purge left empty split\(s\): train, validation"):
        _split_fixtures(fixtures, _config())


def test_legacy_default_and_engine_identity_are_unchanged() -> None:
    legacy = DiscoveryConfig()
    purged = DiscoveryConfig(temporal_purge=True)
    assert legacy.temporal_purge is False
    assert _discovery_engine_version(legacy) == DISCOVERY_ENGINE_VERSION
    assert _discovery_engine_version(purged) == (
        DISCOVERY_ENGINE_VERSION + TEMPORAL_PURGED_ENGINE_SUFFIX
    )
    assert PURGED_SPLIT_POLICY == "chronological_fixture_group_purged_v1"


def test_joiner_exposes_when_outcome_record_really_became_available() -> None:
    observed = datetime(2026, 9, 1, 10, tzinfo=UTC)
    declared_resolved = datetime(2026, 9, 1, 11, 55, tzinfo=UTC)
    journal_created = datetime(2026, 9, 1, 12, tzinfo=UTC)
    observation_id = "991:50:test"
    store = JournalJoinStore(config=_config())
    try:
        store.ingest_observation_records(
            [
                {
                    "record_type": "observation",
                    "observation_id": observation_id,
                    "fixture_id": 991,
                    "stage": "wide_monitor",
                    "minute": 50,
                    "created_at_utc": observed.isoformat(),
                    "schema_version": 1,
                },
                {
                    "record_type": "observation_outcome",
                    "observation_id": observation_id,
                    "created_at_utc": journal_created.isoformat(),
                    "outcome_schema_version": 1,
                    "outcome": {
                        "status": "resolved",
                        "goal_to90_normal_time": True,
                        "resolved_at_utc": declared_resolved.isoformat(),
                    },
                },
            ]
        )
        joined = list(store.joined_observations())
    finally:
        store.close()

    assert len(joined) == 1
    assert joined[0]["_outcome_record_created_at_utc"] == journal_created.isoformat()


def test_delayed_outcome_journal_does_not_admit_post_resolution_prediction() -> None:
    observed = datetime(2026, 9, 1, 10, tzinfo=UTC)
    declared_resolved = observed + timedelta(minutes=2)
    journal_created = observed + timedelta(hours=1)
    prediction_created = observed + timedelta(minutes=3)
    observation_id = "992:50:test"
    observation = {
        "record_type": "observation",
        "observation_id": observation_id,
        "fixture_id": 992,
        "stage": "wide_monitor",
        "minute": 50,
        "created_at_utc": observed.isoformat(),
        "schema_version": 1,
    }
    outcome = {
        "record_type": "observation_outcome",
        "observation_id": observation_id,
        "created_at_utc": journal_created.isoformat(),
        "outcome_schema_version": 2,
        "outcome_revision": 1,
        "outcome": {
            "status": "resolved",
            "goal_to90_normal_time": True,
            "resolved_at_utc": declared_resolved.isoformat(),
        },
    }
    prediction = {
        "record_type": "shadow_ml_prediction",
        "prediction_key": "post-resolution",
        "observation_id": observation_id,
        "fixture_id": 992,
        "minute": 50,
        "created_at_utc": prediction_created.isoformat(),
        "observation_created_at_utc": observed.isoformat(),
        "model_created_at_utc": (observed - timedelta(minutes=1)).isoformat(),
        "model_data_cutoff_utc": (observed - timedelta(minutes=2)).isoformat(),
        "shadow_only": True,
        "production_applied": False,
        "prediction_status": "ok",
        "predictions": {
            "to90": {
                "status": "ok",
                "production_applied": False,
                "calibrated_probability_pct": 80.0,
            }
        },
    }
    with JournalJoinStore(config=_config()) as store:
        store.ingest_observation_records([observation, outcome])
        store.ingest_predictions([prediction], source="static")
        joined = list(store.joined_observations())[0]

    assert joined["_outcome_record_created_at_utc"] == journal_created.isoformat()
    assert "static" not in joined["_wide_predictions"]


@pytest.mark.parametrize("value", [-1.0, float("nan"), float("inf")])
def test_temporal_embargo_must_be_finite_and_non_negative(value: float) -> None:
    with pytest.raises(ValueError, match="temporal_embargo_seconds"):
        DiscoveryConfig(temporal_embargo_seconds=value)


def test_joiner_ranks_schema_then_revision_then_journal_time() -> None:
    observation = {
        "record_type": "observation", "observation_id": "corrected",
        "fixture_id": 1, "stage": "wide_monitor", "minute": 50,
        "created_at_utc": "2026-09-01T10:00:00+00:00", "schema_version": 1,
    }
    def outcome(schema: int, revision: int, hour: int, label: bool) -> dict:
        return {
            "record_type": "observation_outcome", "observation_id": "corrected",
            "created_at_utc": f"2026-09-01T{hour:02}:00:00+00:00",
            "outcome_schema_version": schema, "outcome_revision": revision,
            "outcome": {
                "status": "resolved", "goal_to90_normal_time": label,
                "resolved_at_utc": "2026-09-01T11:00:00+00:00",
            },
        }
    same_revision_older_offset = outcome(2, 2, 14, False)
    same_revision_older_offset["created_at_utc"] = (
        "2026-09-01T14:00:00+02:00"
    )
    records = [outcome(2, 2, 13, True), outcome(2, 1, 15, False),
               outcome(1, 10, 16, False), outcome(2, 2, 12, False),
               same_revision_older_offset]
    for ordered in (records, list(reversed(records))):
        with JournalJoinStore(config=_config()) as store:
            store.ingest_observation_records([observation, *ordered])
            joined = list(store.joined_observations())[0]
        assert joined["outcome"]["goal_to90_normal_time"] is True
        assert joined["outcome_revision"] == 2
        assert joined["outcome_schema_version"] == 2
        assert joined["_outcome_record_created_at_utc"] == "2026-09-01T13:00:00+00:00"


def test_purge_does_not_infer_label_availability_without_journal_time() -> None:
    observation = {
        "record_type": "observation",
        "observation_id": "missing-journal-time",
        "fixture_id": 1,
        "stage": "wide_monitor",
        "minute": 50,
        "created_at_utc": "2026-09-01T10:00:00+00:00",
        "schema_version": 1,
        "gates": {
            "readiness_passed": True,
            "publication_context_passed": True,
        },
        "publication_policy": {"publication_context_passed": True},
    }
    outcome = {
        "record_type": "observation_outcome",
        "observation_id": "missing-journal-time",
        "outcome_schema_version": 2,
        "outcome_revision": 1,
        "outcome": {
            "status": "resolved",
            "goal_to90_normal_time": True,
            "resolved_at_utc": "2026-09-01T11:00:00+00:00",
        },
    }
    with JournalJoinStore(config=_config()) as store:
        store.ingest_observation_records([observation, outcome])
        joined = list(store.joined_observations())[0]

    row, reason = _eligible_row(joined, _config())
    assert reason == ""
    assert row is not None
    assert row.label_available_at is None
    assert joined["_outcome_record_created_at_utc"] is None
