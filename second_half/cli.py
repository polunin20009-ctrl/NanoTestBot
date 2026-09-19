from __future__ import annotations

import argparse
import json
from typing import Any, Dict, Iterable, Tuple

from .factors import load_league_2h_stats, load_team_2h_stats


def _team_items() -> Dict[str, Any]:
    return (load_team_2h_stats() or {}).get("teams", {})


def _league_items() -> Dict[str, Any]:
    return (load_league_2h_stats() or {}).get("leagues", {})


def _print_json(payload: Dict[str, Any]) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def _summary_team(team_id: int) -> int:
    team = _team_items().get(str(int(team_id)))
    if not isinstance(team, dict):
        print(f"Team {team_id} not found in stats.")
        return 1
    _print_json(team)
    return 0


def _summary_league(league_id: int) -> int:
    league = _league_items().get(str(int(league_id)))
    if not isinstance(league, dict):
        print(f"League {league_id} not found in stats.")
        return 1
    _print_json(league)
    return 0


def _top_teams(limit: int) -> int:
    teams = [team for team in _team_items().values() if isinstance(team, dict)]
    teams.sort(key=lambda item: float(item.get("weighted_2h_scored_avg_final", 0.0)), reverse=True)
    for index, team in enumerate(teams[: max(1, limit)], start=1):
        print(
            f"{index:02d}. team_id={team.get('team_id')} name={team.get('team_name')} "
            f"2h_scored={float(team.get('weighted_2h_scored_avg_final', 0.0)):.3f} "
            f"2h_conceded={float(team.get('weighted_2h_conceded_avg_final', 0.0)):.3f} "
            f"sample={int(team.get('sample_matches', 0))}"
        )
    return 0


def _top_leagues(limit: int) -> int:
    leagues = [league for league in _league_items().values() if isinstance(league, dict)]
    leagues.sort(key=lambda item: float(item.get("avg_2h_goals", 0.0)), reverse=True)
    for index, league in enumerate(leagues[: max(1, limit)], start=1):
        print(
            f"{index:02d}. league_id={league.get('league_id')} name={league.get('league_name')} "
            f"avg_2h={float(league.get('avg_2h_goals', 0.0)):.3f} "
            f"after60={float(league.get('avg_goals_after_60', 0.0)):.3f} "
            f"sample={int(league.get('sample_matches', 0))}"
        )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Second-half analytics CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    summary_parser = subparsers.add_parser("summary", help="Show team or league summary")
    group = summary_parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--team", type=int, help="Team id")
    group.add_argument("--league", type=int, help="League id")

    top_teams_parser = subparsers.add_parser("top-teams", help="Show top teams")
    top_teams_parser.add_argument("--limit", type=int, default=10)

    top_leagues_parser = subparsers.add_parser("top-leagues", help="Show top leagues")
    top_leagues_parser.add_argument("--limit", type=int, default=10)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "summary":
        if args.team is not None:
            return _summary_team(args.team)
        return _summary_league(args.league)

    if args.command == "top-teams":
        return _top_teams(args.limit)

    if args.command == "top-leagues":
        return _top_leagues(args.limit)

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
