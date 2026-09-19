from __future__ import annotations

import gzip
import json
import math
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from market_benchmark import (
    AppendOnlyMarketJournal,
    iter_market_records,
    market_journal_paths,
)


def _record(
    key: str,
    *,
    fixture_id: int = 123,
    captured: str = "2026-09-08T12:00:00Z",
    payload: str = "",
) -> dict:
    return {
        "schema_version": 1,
        "record_type": "market_odds_snapshot",
        "record_key": key,
        "fixture_id": fixture_id,
        "captured_at_utc": captured,
        "shadow_only": True,
        "production_applied": False,
        "payload": payload,
    }


def test_append_deduplicates_across_restart_without_ram_key_set(
    tmp_path: Path,
) -> None:
    path = tmp_path / "market.jsonl"
    first = AppendOnlyMarketJournal(path)

    assert first.append(_record("same"))
    assert not first.append(_record("same"))
    restarted = AppendOnlyMarketJournal(path)
    assert not restarted.append(_record("same"))
    assert restarted.memory_stats() == {
        "dedupe_backend": "sqlite",
        "indexed_records": 1,
        "in_memory_record_keys": 0,
    }
    assert [record["record_key"] for record in iter_market_records(path)] == [
        "same"
    ]


def test_append_many_reports_invalid_duplicate_and_new_records(
    tmp_path: Path,
) -> None:
    journal = AppendOnlyMarketJournal(tmp_path / "market.jsonl")
    invalid = _record("invalid")
    invalid["production_applied"] = True

    result = journal.append_many(
        (_record("a"), _record("a"), invalid, _record("b"))
    )

    assert result == (True, False, False, True)
    assert {item["record_key"] for item in iter_market_records(journal.path)} == {
        "a",
        "b",
    }


def test_storage_enforces_shadow_only_metadata(tmp_path: Path) -> None:
    journal = AppendOnlyMarketJournal(tmp_path / "market.jsonl")
    missing_shadow = _record("a")
    missing_shadow.pop("shadow_only")
    missing_production = _record("b")
    missing_production.pop("production_applied")

    assert not journal.append(missing_shadow)
    assert not journal.append(missing_production)
    assert not journal.append({"record_key": "c"})
    assert not Path(journal.path).exists()


def test_rotates_to_gzip_and_reads_archives_in_order(tmp_path: Path) -> None:
    path = tmp_path / "market.jsonl"
    journal = AppendOnlyMarketJournal(path, rotate_max_bytes=280)
    records = [
        _record(f"key-{index}", payload=str(index) * 220)
        for index in range(3)
    ]

    assert journal.append_many(records) == (True, True, True)

    archives = sorted(tmp_path.glob("market.*.jsonl.gz"))
    assert len(archives) == 2
    with gzip.open(archives[0], "rt", encoding="utf-8") as handle:
        assert json.loads(handle.readline())["record_key"] == "key-0"
    assert [record["record_key"] for record in iter_market_records(path)] == [
        "key-0",
        "key-1",
        "key-2",
    ]


def test_append_is_thread_safe_for_same_key(tmp_path: Path) -> None:
    journal = AppendOnlyMarketJournal(tmp_path / "market.jsonl")

    with ThreadPoolExecutor(max_workers=12) as executor:
        results = list(executor.map(lambda _: journal.append(_record("same")), range(40)))

    assert results.count(True) == 1
    assert results.count(False) == 39
    assert len(list(iter_market_records(journal.path))) == 1


def test_index_repairs_after_external_append(tmp_path: Path) -> None:
    path = tmp_path / "market.jsonl"
    journal = AppendOnlyMarketJournal(path)
    assert journal.append(_record("first"))
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(_record("external")) + "\n")

    restarted = AppendOnlyMarketJournal(path)

    assert not restarted.append(_record("external"))
    assert restarted.memory_stats()["indexed_records"] == 2


def test_latest_for_fixture_is_causal_and_age_bounded(tmp_path: Path) -> None:
    journal = AppendOnlyMarketJournal(tmp_path / "market.jsonl")
    assert journal.append_many(
        (
            _record("early", captured="2026-09-08T11:59:00Z"),
            _record("latest", captured="2026-09-08T11:59:50Z"),
            _record("future", captured="2026-09-08T12:00:01Z"),
            _record(
                "other-fixture",
                fixture_id=456,
                captured="2026-09-08T11:59:59Z",
            ),
        )
    ) == (True, True, True, True)

    selected = journal.latest_for_fixture(
        123,
        at_or_before_utc="2026-09-08T12:00:00Z",
        max_age_seconds=30,
    )

    assert selected is not None
    assert selected["record_key"] == "latest"
    assert (
        journal.latest_for_fixture(
            123,
            at_or_before_utc="2026-09-08T12:00:00Z",
            max_age_seconds=5,
        )
        is None
    )


def test_reader_isolates_invalid_lines_and_non_finite_json_is_rejected(
    tmp_path: Path,
    caplog,
) -> None:
    path = tmp_path / "market.jsonl"
    path.write_text("{bad-json\n" + json.dumps(_record("valid")) + "\n", encoding="utf-8")

    records = list(iter_market_records(path))

    assert [record["record_key"] for record in records] == ["valid"]
    assert "MARKET_BENCHMARK_INVALID_LINE" in caplog.text
    journal = AppendOnlyMarketJournal(path)
    invalid = _record("nan")
    invalid["probability"] = math.nan
    with pytest.raises(ValueError):
        journal.append(invalid)


def test_mixed_archive_timestamp_formats_sort_by_actual_time(
    tmp_path: Path,
) -> None:
    path = tmp_path / "market.jsonl"
    older = tmp_path / "market.20260908T120000Z.jsonl.gz"
    newer = tmp_path / "market.20260908T120000500000Z.jsonl.gz"
    for archive, key in ((older, "older"), (newer, "newer")):
        with gzip.open(archive, "wt", encoding="utf-8") as handle:
            handle.write(json.dumps(_record(key)) + "\n")
    path.write_text(json.dumps(_record("active")) + "\n", encoding="utf-8")

    assert market_journal_paths(path) == [
        str(older),
        str(newer),
        str(path),
    ]
    assert [record["record_key"] for record in iter_market_records(path)] == [
        "older",
        "newer",
        "active",
    ]
