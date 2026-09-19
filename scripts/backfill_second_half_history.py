from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from NanoTest import API_FOOTBALL_HOST, API_FOOTBALL_KEY, APISportsMetricsClient
from second_half.backfill import backfill_second_half_history


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backfill second-half finished-match history from API-Football")
    parser.add_argument("--league", type=int, action="append", required=True, help="League id, repeatable")
    parser.add_argument("--season", type=int, action="append", required=True, help="Season, repeatable")
    parser.add_argument("--max-pages", type=int, default=None, help="Optional page cap per league/season")
    parser.add_argument("--limit-fixtures", type=int, default=None, help="Optional fixture cap per league/season")
    parser.add_argument(
        "--sleep-ms",
        type=int,
        default=150,
        help="Sleep between fixture event calls, defaults to 150ms; raise it if your key hits rate limits",
    )
    parser.add_argument("--aggregate", action="store_true", help="Run aggregation after backfill")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    logger = logging.getLogger("second_half_backfill")
    client = APISportsMetricsClient(api_key=API_FOOTBALL_KEY, host=API_FOOTBALL_HOST)

    result = backfill_second_half_history(
        client=client,
        league_ids=args.league,
        seasons=args.season,
        max_pages=args.max_pages,
        limit_fixtures=args.limit_fixtures,
        sleep_seconds=max(0.0, float(args.sleep_ms) / 1000.0),
        aggregate_after=bool(args.aggregate),
        logger=logger,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())