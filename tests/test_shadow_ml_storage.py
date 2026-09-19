from __future__ import annotations

import gzip
import json
import math
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import shadow_ml.storage as storage
import pytest


def test_model_save_is_atomic_and_loads_round_trip(
    monkeypatch,
    tmp_path: Path,
) -> None:
    path = tmp_path / "model.json"
    path.write_text('{"old":true}\n', encoding="utf-8")
    real_replace = storage.os.replace
    replace_calls: list[tuple[str, str]] = []
    real_fsync = storage.os.fsync
    fsync_calls: list[int] = []

    def replace_spy(source: str, target: str) -> None:
        replace_calls.append((source, target))
        real_replace(source, target)

    def fsync_spy(descriptor: int) -> None:
        fsync_calls.append(descriptor)
        real_fsync(descriptor)

    monkeypatch.setattr(storage.os, "replace", replace_spy)
    monkeypatch.setattr(storage.os, "fsync", fsync_spy)

    storage.save_model(path, {"schema_version": 1, "name": "модель"})

    assert storage.load_model(path) == {
        "schema_version": 1,
        "name": "модель",
    }
    assert len(replace_calls) == 1
    temporary, target = replace_calls[0]
    assert Path(temporary).parent == tmp_path
    assert target == str(path.resolve())
    assert fsync_calls
    assert not Path(temporary).exists()


def test_model_load_returns_empty_mapping_for_missing_or_invalid(
    tmp_path: Path,
) -> None:
    path = tmp_path / "model.json"

    assert storage.load_model(path) == {}
    path.write_text("{not-json", encoding="utf-8")
    assert storage.load_model(path) == {}
    path.write_text("[1,2,3]", encoding="utf-8")
    assert storage.load_model(path) == {}


def test_model_serialization_failure_preserves_previous_artifact(
    tmp_path: Path,
) -> None:
    path = tmp_path / "model.json"
    path.write_text('{"stable":true}\n', encoding="utf-8")

    try:
        storage.save_model(path, {"invalid": math.nan})
    except ValueError:
        pass
    else:
        raise AssertionError("NaN artifact must be rejected")

    assert storage.load_model(path) == {"stable": True}
    assert list(tmp_path.glob(".model.*.tmp")) == []


def test_predictions_rotate_to_gzip_and_deduplicate_after_cache_reset(
    tmp_path: Path,
) -> None:
    path = tmp_path / "predictions.jsonl"
    first = {"prediction_key": "prediction:a:model-1", "payload": "x" * 200}
    second = {"prediction_key": "prediction:b:model-1", "payload": "y" * 200}

    assert storage.append_prediction_record(
        path,
        first,
        rotate_max_bytes=250,
    )
    assert storage.append_prediction_record(
        path,
        second,
        rotate_max_bytes=250,
    )
    assert not storage.append_prediction_record(
        path,
        first,
        rotate_max_bytes=250,
    )

    archives = list(tmp_path.glob("predictions.*.jsonl.gz"))
    assert len(archives) == 1
    with gzip.open(archives[0], "rt", encoding="utf-8") as handle:
        assert json.loads(handle.readline())["prediction_key"] == first["prediction_key"]
    assert json.loads(path.read_text(encoding="utf-8"))["prediction_key"] == second["prediction_key"]

    storage.reset_prediction_cache(path)
    assert not storage.append_prediction_record(
        path,
        first,
        rotate_max_bytes=250,
    )
    assert [item["prediction_key"] for item in storage.iter_prediction_records(path)] == [
        first["prediction_key"],
        second["prediction_key"],
    ]


def test_prediction_reader_accepts_timestamp_archives_and_isolates_bad_lines(
    tmp_path: Path,
    caplog,
) -> None:
    path = tmp_path / "predictions.jsonl"
    legacy = tmp_path / "predictions.20260725T010203Z.jsonl"
    compressed = tmp_path / "predictions.20260725T010204123456Z.jsonl.gz"
    distractor = tmp_path / "predictions.joined.jsonl"

    legacy.write_text(
        "\n".join(
            [
                json.dumps({"prediction_key": "legacy"}, separators=(",", ":")),
                "{bad-json",
                json.dumps(["not", "an", "object"]),
                "",
            ]
        ),
        encoding="utf-8",
    )
    with gzip.open(compressed, "wt", encoding="utf-8") as handle:
        handle.write(json.dumps({"prediction_key": "gzip"}) + "\n")
    path.write_text(json.dumps({"prediction_key": "active"}) + "\n", encoding="utf-8")
    distractor.write_text(json.dumps({"prediction_key": "ignore"}) + "\n", encoding="utf-8")

    records = list(storage.iter_prediction_records(path))

    assert [record["prediction_key"] for record in records] == [
        "legacy",
        "gzip",
        "active",
    ]
    assert "SHADOW_ML_PREDICTION_INVALID_LINE" in caplog.text


def test_prediction_append_is_thread_safe_for_same_key(tmp_path: Path) -> None:
    path = tmp_path / "predictions.jsonl"
    record = {"prediction_key": "same", "probability": 0.75}
    storage.reset_prediction_cache(path)

    with ThreadPoolExecutor(max_workers=12) as executor:
        results = list(
            executor.map(
                lambda _: storage.append_prediction_record(path, record),
                range(50),
            )
        )

    assert results.count(True) == 1
    assert results.count(False) == 49
    assert len(list(storage.iter_prediction_records(path))) == 1


def test_rotation_never_deletes_existing_archives(tmp_path: Path) -> None:
    path = tmp_path / "predictions.jsonl"
    records = [
        {"prediction_key": f"key-{index}", "payload": str(index) * 220}
        for index in range(3)
    ]

    for record in records:
        assert storage.append_prediction_record(
            path,
            record,
            rotate_max_bytes=250,
        )

    archives = sorted(tmp_path.glob("predictions.*.jsonl.gz"))
    assert len(archives) == 2
    assert all(archive.exists() for archive in archives)
    assert {
        record["prediction_key"]
        for record in storage.iter_prediction_records(path)
    } == {"key-0", "key-1", "key-2"}


def test_prediction_without_key_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "predictions.jsonl"

    assert not storage.append_prediction_record(path, {"probability": 50.0})
    assert not path.exists()


def test_prediction_with_non_finite_number_is_rejected(
    tmp_path: Path,
) -> None:
    path = tmp_path / "predictions.jsonl"

    with pytest.raises(ValueError):
        storage.append_prediction_record(
            path,
            {"prediction_key": "invalid", "probability": math.nan},
        )

    assert not path.exists()


def teardown_function() -> None:
    storage.reset_prediction_cache()
