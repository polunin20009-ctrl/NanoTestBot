from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from wide_research.health import AlertPolicy, AlertTracker, HealthPolicy, evaluate_health


NOW = datetime(2026, 9, 12, 8, 0, tzinfo=timezone.utc)


def _base_snapshot() -> dict:
    return {
        "observations": {
            "expected_eligible": True,
            "eligible_active": 2,
            "last_seen_at": (NOW - timedelta(seconds=30)).isoformat(),
        },
        "outcomes": {"pending": 0, "overdue": 0},
        "discovery": {
            "profiles": {
                name: {
                    "enabled": True,
                    "last_completed_at": (NOW - timedelta(hours=20)).isoformat(),
                    "interval_seconds": 86_400,
                }
                for name in ("primary", "exact4_hardshadow", "precision", "rare_precision_shadow")
            }
        },
        "pools": {
            "profiles": {
                "rare_precision_shadow": {"occupied": 20, "capacity": 64, "not_admitted": 0}
            }
        },
        "retries": {"pending": 0, "failed": 0},
        "disk": {"free_bytes": 5_000_000_000, "total_bytes": 10_000_000_000},
    }


def _by_id(report: dict, check_id: str) -> dict:
    return next(item for item in report["checks"] if item["id"] == check_id)


def test_healthy_snapshot_is_json_serializable_and_covers_four_profiles():
    report = evaluate_health(_base_snapshot(), NOW)
    assert report["overall"] == "ok"
    assert report["counts"]["critical"] == 0
    assert sum(item["id"].startswith("discovery.") for item in report["checks"]) == 4
    json.dumps(report)


def test_stale_observations_are_skipped_when_no_fixture_is_eligible():
    snapshot = _base_snapshot()
    snapshot["observations"] = {
        "expected_eligible": False,
        "eligible_active": 0,
        "last_seen_at": (NOW - timedelta(days=2)).isoformat(),
    }
    check = _by_id(evaluate_health(snapshot, NOW), "observations.flow")
    assert check["level"] == "skipped"


def test_stale_observations_alert_only_when_eligible_fixtures_exist():
    snapshot = _base_snapshot()
    snapshot["observations"]["last_seen_at"] = (NOW - timedelta(minutes=6)).isoformat()
    assert _by_id(evaluate_health(snapshot, NOW), "observations.flow")["level"] == "warning"
    snapshot["observations"]["last_seen_at"] = (NOW - timedelta(minutes=16)).isoformat()
    assert _by_id(evaluate_health(snapshot, NOW), "observations.flow")["level"] == "critical"


def test_outcome_items_respect_finished_time_grace_and_ignore_live_items():
    snapshot = _base_snapshot()
    snapshot["outcomes"] = {
        "grace_seconds": 3_600,
        "items": [
            {"fixture_id": 1, "finished_at": (NOW - timedelta(minutes=59)).isoformat()},
            {"fixture_id": 2, "finished_at": (NOW - timedelta(hours=2)).isoformat()},
            {"fixture_id": 3, "finished_at": None},
            {
                "fixture_id": 4,
                "finished_at": (NOW - timedelta(days=1)).isoformat(),
                "resolved_at": NOW.isoformat(),
            },
        ],
    }
    check = _by_id(evaluate_health(snapshot, NOW), "outcomes.reconciliation")
    assert check["level"] == "warning"
    assert check["metrics"]["pending"] == 2
    assert check["metrics"]["overdue"] == 1


def test_discovery_is_checked_per_profile_with_running_grace():
    snapshot = _base_snapshot()
    profiles = snapshot["discovery"]["profiles"]
    profiles["primary"]["last_completed_at"] = (NOW - timedelta(hours=30)).isoformat()
    profiles["precision"]["running_since"] = (NOW - timedelta(hours=1)).isoformat()
    profiles["rare_precision_shadow"]["failures"] = 3
    report = evaluate_health(snapshot, NOW)
    assert _by_id(report, "discovery.primary")["level"] == "warning"
    assert _by_id(report, "discovery.precision")["level"] == "ok"
    assert _by_id(report, "discovery.rare_precision_shadow")["level"] == "critical"


def test_pool_reports_46_of_64_and_rejected_candidates():
    snapshot = _base_snapshot()
    snapshot["pools"]["profiles"]["rare_precision_shadow"] = {
        "occupied": 46,
        "capacity": 64,
        "not_admitted": 1,
    }
    check = _by_id(evaluate_health(snapshot, NOW), "pools.rare_precision_shadow")
    assert check["level"] == "warning"
    assert check["metrics"]["occupancy_fraction"] == pytest.approx(46 / 64)
    assert check["metrics"]["not_admitted"] == 1


def test_full_pool_without_rejections_is_healthy_bounded_steady_state():
    snapshot = _base_snapshot()
    snapshot["pools"]["profiles"]["primary"] = {
        "occupied": 10,
        "capacity": 10,
        "not_admitted": 0,
    }
    check = _by_id(evaluate_health(snapshot, NOW), "pools.primary")
    assert check["level"] == "ok"
    assert check["metrics"]["capacity_state"] == "full"
    assert check["metrics"]["occupancy_fraction"] == 1.0


def test_pool_alerts_are_driven_by_rejections_or_exceeded_capacity():
    snapshot = _base_snapshot()
    pool = snapshot["pools"]["profiles"]["rare_precision_shadow"]
    pool.update({"occupied": 64, "capacity": 64, "not_admitted": 1})
    assert _by_id(evaluate_health(snapshot, NOW), "pools.rare_precision_shadow")[
        "level"
    ] == "warning"

    pool["not_admitted"] = 5
    assert _by_id(evaluate_health(snapshot, NOW), "pools.rare_precision_shadow")[
        "level"
    ] == "critical"

    pool.update({"occupied": 65, "not_admitted": 0})
    check = _by_id(evaluate_health(snapshot, NOW), "pools.rare_precision_shadow")
    assert check["level"] == "critical"
    assert check["metrics"]["capacity_state"] == "over_capacity"


def test_planned_purge_transition_is_not_a_false_full_pool_alert():
    snapshot = _base_snapshot()
    snapshot["pools"]["profiles"]["primary"] = {
        "occupied": 20,
        "capacity": 20,
        "configured_capacity": 10,
        "effective_capacity": 20,
        "purge_transition_reserve": 10,
        "purge_transition_active": True,
        "purge_transition_portfolio_deferred": False,
        "not_admitted": 0,
    }
    check = _by_id(evaluate_health(snapshot, NOW), "pools.primary")
    assert check["level"] == "ok"
    assert check["metrics"]["configured_capacity"] == 10
    assert check["metrics"]["effective_capacity"] == 20

    snapshot["pools"]["profiles"]["primary"][
        "purge_transition_portfolio_deferred"
    ] = True
    assert _by_id(evaluate_health(snapshot, NOW), "pools.primary")[
        "level"
    ] == "critical"


def test_durable_retry_age_and_failure_thresholds():
    snapshot = _base_snapshot()
    snapshot["retries"] = {
        "pending": 1,
        "failed": 1,
        "oldest_pending_at": (NOW - timedelta(minutes=8)).isoformat(),
    }
    assert _by_id(evaluate_health(snapshot, NOW), "retries.durable")["level"] == "warning"
    snapshot["retries"]["failed"] = 3
    assert _by_id(evaluate_health(snapshot, NOW), "retries.durable")["level"] == "critical"


def test_disk_is_optional_but_low_disk_is_reported():
    snapshot = _base_snapshot()
    snapshot.pop("disk")
    assert _by_id(evaluate_health(snapshot, NOW), "disk.space")["level"] == "skipped"
    snapshot["disk"] = {"free_bytes": 400_000_000, "total_bytes": 10_000_000_000}
    assert _by_id(evaluate_health(snapshot, NOW), "disk.space")["level"] == "critical"


def test_alert_tracker_hysteresis_dedup_escalation_and_recovery():
    tracker = AlertTracker(
        policy=AlertPolicy(warning_after=2, critical_after=1, recover_after=2, reminder_seconds=3_600)
    )
    warning = {"checks": [{"id": "pools.rare", "level": "warning", "message": "near full"}]}
    assert tracker.update(warning, NOW) == []
    fired = tracker.update(warning, NOW + timedelta(seconds=10))
    assert [item["kind"] for item in fired] == ["firing"]
    assert tracker.update(warning, NOW + timedelta(minutes=10)) == []

    critical = {"checks": [{"id": "pools.rare", "level": "critical", "message": "full"}]}
    escalated = tracker.update(critical, NOW + timedelta(minutes=11))
    assert [item["kind"] for item in escalated] == ["escalated"]

    healthy = {"checks": [{"id": "pools.rare", "level": "ok", "message": "space"}]}
    assert tracker.update(healthy, NOW + timedelta(minutes=12)) == []
    resolved = tracker.update(healthy, NOW + timedelta(minutes=13))
    assert [item["kind"] for item in resolved] == ["resolved"]


def test_alert_tracker_critical_fires_immediately_and_reminds_after_cooldown():
    tracker = AlertTracker(policy=AlertPolicy(reminder_seconds=60))
    report = {"checks": [{"id": "retries.durable", "level": "critical", "message": "stuck"}]}
    assert tracker.update(report, NOW)[0]["kind"] == "firing"
    assert tracker.update(report, NOW + timedelta(seconds=59)) == []
    assert tracker.update(report, NOW + timedelta(seconds=60))[0]["kind"] == "reminder"


def test_alert_tracker_state_round_trip_is_atomic(tmp_path):
    path = tmp_path / "research-health.json"
    tracker = AlertTracker()
    critical = {"checks": [{"id": "outcomes.reconciliation", "level": "critical", "message": "late"}]}
    assert tracker.update(critical, NOW)
    tracker.save_atomic(path)
    loaded = AlertTracker.load(path)
    assert loaded.to_dict() == tracker.to_dict()
    assert json.loads(path.read_text(encoding="utf-8"))["schema_version"] == 1
    assert not list(tmp_path.glob("*.tmp"))


def test_corrupt_or_structurally_invalid_alert_state_fails_open(tmp_path):
    path = tmp_path / "research-health.json"
    path.write_text("not-json", encoding="utf-8")
    assert AlertTracker.load(path).to_dict()["checks"] == {}

    path.write_text(
        json.dumps(
            {
                "policy": {"warning_after": -1, "unknown": 5},
                "checks": {
                    "bad": {
                        "active": True,
                        "level": "critical",
                        "last_emitted_at": "not-a-time",
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    restored = AlertTracker.load(path)
    assert restored.policy == AlertPolicy()
    assert restored.to_dict()["checks"]["bad"]["last_emitted_at"] is None


def test_naive_clock_is_rejected_to_prevent_time_boundary_errors():
    with pytest.raises(ValueError, match="timezone"):
        evaluate_health(_base_snapshot(), datetime(2026, 9, 12, 8, 0))


def test_policy_rejects_inverted_thresholds():
    with pytest.raises(ValueError):
        HealthPolicy(observation_warning_seconds=900, observation_critical_seconds=300)
