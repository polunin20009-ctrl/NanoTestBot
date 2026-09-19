from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Sequence


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

os.environ["GOALBOT_LIBRARY_MODE"] = "1"

import NanoTest as bot
from signal_reputation import build_reputation_model, save_reputation_model


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Build the signal reputation artifact in an isolated worker."
    )
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)

    input_path = os.path.abspath(args.input)
    output_path = os.path.abspath(os.fspath(args.output))
    protected = {
        os.path.normcase(os.path.realpath(path))
        for path in bot._decision_snapshot_paths(input_path)
    }
    if os.path.normcase(os.path.realpath(output_path)) in protected:
        parser.error("model output cannot replace the decision journal")

    records = bot.load_joined_decision_snapshots(
        input_path,
        include_pending=False,
    )
    model = build_reputation_model(
        records,
        config=bot._signal_reputation_config(),
        now=datetime.now(timezone.utc),
    )
    save_reputation_model(output_path, model)
    print(
        json.dumps(
            {
                "status": "rebuilt",
                "rows": len(records),
                "generated_at_utc": model.get("generated_at_utc"),
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
