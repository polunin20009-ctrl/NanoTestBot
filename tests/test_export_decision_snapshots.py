from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from scripts import export_decision_snapshots as exporter


def _record(
    decision_id: str | None,
    *,
    decision_key: str | None = None,
    fixture_id: int = 101,
    status: str = "resolved",
) -> dict:
    record = {
        "record_type": "decision",
        "fixture_id": fixture_id,
        "outcome": {"status": status},
        "decision": {"final_decision": "BLOCK"},
    }
    if decision_id is not None:
        record["decision_id"] = decision_id
    if decision_key is not None:
        record["decision_key"] = decision_key
    return record


def test_prepare_preserves_distinct_minutes_from_same_fixture() -> None:
    records = [
        _record("101:46:WINDOW_1:v2", fixture_id=101),
        _record("101:47:WINDOW_1:v2", fixture_id=101),
    ]

    prepared = exporter._prepare_export_records(records)

    assert len(prepared) == 2
    assert {record["fixture_id"] for record in prepared} == {101}
    assert {record["decision_id"] for record in prepared} == {
        "101:46:WINDOW_1:v2",
        "101:47:WINDOW_1:v2",
    }


def test_prepare_normalizes_a_single_available_canonical_id() -> None:
    prepared = exporter._prepare_export_records(
        [_record(None, decision_key="101:46:WINDOW_1:v2")]
    )

    assert prepared[0]["decision_id"] == "101:46:WINDOW_1:v2"
    assert prepared[0]["decision_key"] == "101:46:WINDOW_1:v2"


@pytest.mark.parametrize(
    ("records", "message"),
    [
        ([_record(None)], "no non-empty"),
        (
            [_record("decision-a", decision_key="decision-b")],
            "mismatched",
        ),
        (
            [_record("decision-a"), _record(None, decision_key="decision-a")],
            "duplicate canonical",
        ),
    ],
)
def test_prepare_rejects_ambiguous_or_duplicate_ids(
    records: list[dict], message: str
) -> None:
    with pytest.raises(exporter.ExportValidationError, match=message):
        exporter._prepare_export_records(records)


def test_resolved_filter_does_not_deduplicate_by_fixture() -> None:
    prepared = exporter._prepare_export_records(
        [
            _record("resolved-1", fixture_id=101),
            _record("pending", fixture_id=101, status="pending"),
            _record("resolved-2", fixture_id=101),
        ],
        resolved_only=True,
    )

    assert [record["decision_id"] for record in prepared] == [
        "resolved-1",
        "resolved-2",
    ]


def test_compact_removes_only_full_legacy_shadow_projection() -> None:
    source = _record("decision-a")
    source["shadow_reputation"] = {"cohorts": {"all_decisions": {"large": True}}}
    source["shadow_reputation_summary"] = {
        "shadow_key": "projection:decision-a:v1",
        "journal_status": "written",
    }

    prepared = exporter._prepare_export_records([source], compact=True)

    assert "shadow_reputation" not in prepared[0]
    assert prepared[0]["shadow_reputation_summary"] == source[
        "shadow_reputation_summary"
    ]
    assert "shadow_reputation" in source


def test_output_cannot_replace_input_or_an_archive(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    active = tmp_path / "decision_snapshots.jsonl"
    archive = tmp_path / "decision_snapshots.20260725T000000Z.jsonl.gz"
    safe_output = tmp_path / "decision_snapshots_joined.jsonl"
    active.write_text("", encoding="utf-8")
    archive.write_bytes(b"archive")
    monkeypatch.setattr(
        exporter.nanotest,
        "_decision_snapshot_paths",
        lambda _path: [str(archive), str(active)],
    )

    with pytest.raises(exporter.ExportValidationError, match="journal or archive"):
        exporter._assert_safe_output(active, active)
    with pytest.raises(exporter.ExportValidationError, match="journal or archive"):
        exporter._assert_safe_output(active, archive)

    exporter._assert_safe_output(active, safe_output)


def test_atomic_writer_replaces_complete_output_and_fsyncs(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    output = tmp_path / "joined.jsonl"
    output.write_text("old\n", encoding="utf-8")
    real_fsync = os.fsync
    fsync_calls: list[int] = []

    def tracked_fsync(file_descriptor: int) -> None:
        fsync_calls.append(file_descriptor)
        real_fsync(file_descriptor)

    monkeypatch.setattr(exporter.os, "fsync", tracked_fsync)

    written = exporter._write_jsonl([_record("decision-a")], output)

    assert written == 1
    assert json.loads(output.read_text(encoding="utf-8"))["decision_id"] == "decision-a"
    assert fsync_calls
    assert list(tmp_path.glob(f".{output.name}.*.tmp")) == []


def test_atomic_writer_keeps_previous_output_when_serialization_fails(
    tmp_path: Path,
) -> None:
    output = tmp_path / "joined.jsonl"
    output.write_text("previous-complete-export\n", encoding="utf-8")

    with pytest.raises(TypeError):
        exporter._write_jsonl([{"decision_id": {"not-json-serializable"}}], output)

    assert output.read_text(encoding="utf-8") == "previous-complete-export\n"
    assert list(tmp_path.glob(f".{output.name}.*.tmp")) == []


def test_main_writes_valid_compact_joined_export(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    active = tmp_path / "decision_snapshots.jsonl"
    output = tmp_path / "joined.jsonl"
    active.write_text("", encoding="utf-8")
    records = [
        {
            **_record("101:46:WINDOW_1:v2"),
            "shadow_reputation": {"large": True},
            "shadow_reputation_summary": {"shadow_key": "projection:a:v1"},
        },
        _record("101:47:WINDOW_1:v2"),
    ]
    monkeypatch.setattr(
        exporter.nanotest,
        "load_joined_decision_snapshots",
        lambda _path, **_kwargs: records,
    )
    monkeypatch.setattr(
        exporter.nanotest, "_decision_snapshot_paths", lambda _path: [str(active)]
    )

    assert exporter.main(
        [
            "--input",
            str(active),
            "--output",
            str(output),
            "--resolved-only",
            "--compact",
        ]
    ) == 0

    exported = [
        json.loads(line)
        for line in output.read_text(encoding="utf-8").splitlines()
    ]
    assert len(exported) == 2
    assert "shadow_reputation" not in exported[0]
    assert exported[0]["shadow_reputation_summary"] == {
        "shadow_key": "projection:a:v1"
    }
