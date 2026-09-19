"""Snapshot-only Telegram UI for explaining ordinary 45+ signals."""
from __future__ import annotations

import hashlib
import html
import json
import logging
import os
import re
import tempfile
import time
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional, Tuple
from urllib.parse import quote

LOGGER = logging.getLogger(__name__)
MAX_TEXT_LENGTH = 3900
VALID_SIGNAL_KEY = re.compile(r"^[a-zA-Z0-9_-]{6,32}$")
SCREENS = {"overview", "live", "teams", "season", "2h", "technical"}


def _dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _float(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        return float(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _e(value: Any, default: str = "нет данных") -> str:
    raw = default if value in (None, "") else str(value)
    return html.escape(raw, quote=True)


def _fmt(value: Any, digits: int = 2, default: str = "нет данных") -> str:
    number = _float(value)
    return default if number is None else f"{number:.{digits}f}"


def _trim(text: str) -> str:
    return text if len(text) <= MAX_TEXT_LENGTH else text[: MAX_TEXT_LENGTH - 1] + "…"


def render_bar(probability: Any, segments: int = 10) -> str:
    value = max(0.0, min(100.0, _float(probability, 0.0) or 0.0))
    filled = int(round(value / 100.0 * segments))
    return "█" * filled + "░" * (segments - filled)


def interpret_probability(value: Any) -> str:
    number = _float(value, 0.0) or 0.0
    if number >= 85:
        return "очень высокая"
    if number >= 75:
        return "высокая"
    if number >= 55:
        return "умеренная"
    return "невысокая"


def interpret_factor(value: Any) -> str:
    factor = _float(value, 1.0) or 1.0
    if factor < 0.85:
        return "значительно ниже среднего"
    if factor < 0.95:
        return "ниже среднего"
    if factor <= 1.05:
        return "близко к среднему"
    if factor < 1.15:
        return "выше среднего"
    if factor < 1.25:
        return "сильный показатель"
    return "очень сильный показатель"


def _relative(value: Any) -> int:
    return int(round(((_float(value, 1.0) or 1.0) - 1.0) * 100))


def interpret_attack_factor(value: Any) -> str:
    factor = _float(value, 1.0) or 1.0
    delta = _relative(factor)
    direction = "выше" if delta >= 0 else "ниже"
    return f"{interpret_factor(factor).capitalize()} — примерно на {abs(delta)}% {direction} среднего по используемой моделью сезонной оценке."


def interpret_defense_factor(value: Any) -> str:
    factor = _float(value, 1.0) or 1.0
    delta = _relative(factor)
    if delta < 0:
        return f"Надёжная — команда пропускает примерно на {abs(delta)}% меньше базового уровня по оценке модели."
    if delta > 0:
        return f"Уязвимая — команда пропускает примерно на {delta}% больше базового уровня по оценке модели."
    return "Близкая к средней по используемой моделью сезонной оценке."


def interpret_goal_probability_factor(value: Any) -> str:
    factor = _float(value, 1.0) or 1.0
    delta = _relative(factor)
    if delta > 0:
        return f"Положительное влияние на ожидаемую интенсивность — примерно +{delta}%."
    if delta < 0:
        return f"Сдерживает ожидаемую интенсивность — примерно {delta}%."
    return "Нейтральное влияние на ожидаемую интенсивность."


def interpret_context_factor(value: Any) -> str:
    return interpret_goal_probability_factor(value)


def signal_strength(snapshot: Dict[str, Any]) -> str:
    decision = _dict(snapshot.get("decision"))
    factors = _dict(snapshot.get("factors"))
    gates = _dict(snapshot.get("gates"))
    margin = _float(decision.get("decision_margin_to90"), -1.0) or 0.0
    levels = ["пограничная", "умеренная", "сильная", "очень сильная"]
    if margin < 0:
        return "не прошёл порог"
    index = 0 if margin < 1.5 else 1 if margin < 4 else 2 if margin < 8 else 3
    live_intensity = _float(factors.get("live_intensity"))
    barely_live = gates.get("live_gate_required") and (_int(gates.get("live_gate_passed_count")) <= 2)
    if (live_intensity is not None and live_intensity < 0.35) or barely_live:
        index = max(0, index - 1)
    return levels[index]


def _two_h_missing(snapshot: Dict[str, Any]) -> bool:
    factors = _dict(snapshot.get("factors"))
    context = _dict(snapshot.get("second_half"))
    reason = str(context.get("fallback_reason") or factors.get("fallback_reason") or "").lower()
    samples = sum(_int(context.get(k, factors.get(k, 0))) for k in ("team_sample_home", "team_sample_away", "league_sample"))
    return samples == 0 or "missing" in reason


def build_human_summary(snapshot: Dict[str, Any]) -> str:
    metrics, factors = _dict(snapshot.get("live_metrics")), _dict(snapshot.get("factors"))
    supports, restraints = [], []
    pressure = _float(metrics.get("pressure_index"), 0.0) or 0.0
    sot = _float(metrics.get("shots_on_target_total"), 0.0) or 0.0
    xg = _float(metrics.get("xg_total"), 0.0) or 0.0
    if pressure >= 15:
        supports.append("давление остаётся высоким")
    if sot >= 4:
        supports.append("команды регулярно бьют в створ")
    if (_float(factors.get("season_context_factor_45p"), 1.0) or 1.0) > 1.05:
        supports.append("сезонный контекст поддерживает интенсивность")
    if xg and xg < 0.8:
        restraints.append("качество созданных моментов пока невысокое")
    if not supports:
        supports.append("совокупность live-показателей прошла фильтры модели")
    text = "Матч поддерживают " + ", ".join(supports[:3]) + ". "
    if restraints:
        text += "Основной сдерживающий фактор — " + restraints[0] + ". "
    if _two_h_missing(snapshot):
        text += "Статистика второго тайма не повлияла на расчёт из-за недостатка данных."
    else:
        text += "Контекст второго тайма учтён с доступной надёжностью выборки."
    return text


def _header(snapshot: Dict[str, Any], title: str) -> str:
    match = _dict(snapshot.get("match"))
    return (
        f"<b>{title}</b>\n\n"
        f"<b>{_e(match.get('home_team_name'), 'Home')} — {_e(match.get('away_team_name'), 'Away')}</b>\n"
        f"{_e(match.get('score_state'), '—')} • {_int(snapshot.get('minute'))} мин • {_e(match.get('league_name'))}\n"
    )


def build_analysis_overview(snapshot: Dict[str, Any]) -> str:
    probabilities, decision = _dict(snapshot.get("probabilities")), _dict(snapshot.get("decision"))
    p15, p90 = probabilities.get("prob_next_15"), probabilities.get("prob_until_end_decision", probabilities.get("prob_to90"))
    text = _header(snapshot, "🧠 РАЗБОР СИГНАЛА")
    text += "\n" + _e(build_human_summary(snapshot)) + "\n\n<b>🎯 Оценка модели</b>\n\n"
    text += f"Гол в следующие 15 минут\n<code>{render_bar(p15)}</code> {_fmt(p15, 0)}%\n\n"
    text += f"Гол до конца матча\n<code>{render_bar(p90)}</code> {_fmt(p90, 0)}%\n\n"
    text += f"Сила сигнала: <b>{_e(signal_strength(snapshot).capitalize())}</b>\n"
    margin = _float(decision.get("decision_margin_to90"))
    if margin is not None:
        text += f"Запас над порогом: <code>{margin:+.2f}</code> п.п.\n"
    return _trim(text)


def build_analysis_live(snapshot: Dict[str, Any]) -> str:
    m, f = _dict(snapshot.get("live_metrics")), _dict(snapshot.get("factors"))
    pressure, xg = _float(m.get("pressure_index")), _float(m.get("xg_total"))
    pressure_status = "Высокое" if pressure is not None and pressure >= 15 else "Умеренное" if pressure is not None and pressure >= 8 else "Невысокое"
    xg_status = "Высокое" if xg is not None and xg >= 1.5 else "Среднее" if xg is not None and xg >= 0.8 else "Ниже среднего"
    text = _header(snapshot, "🔥 LIVE-КАРТИНА")
    text += f"\n<b>Давление</b>\n{pressure_status}\n{'Регулярные опасные подходы поддерживают прогноз.' if pressure_status == 'Высокое' else 'Давление не даёт сильного самостоятельного усиления.'}\n<code>Значение модели: {_fmt(pressure)}</code>\n"
    text += f"\n<b>Качество моментов</b>\n{xg_status}\n{'Созданные моменты поддерживают вероятность гола.' if xg_status != 'Ниже среднего' else 'Удары есть, но качество моментов остаётся сдерживающим фактором.'}\n<code>xG: {_fmt(xg)}</code>\n"
    text += f"\nУдары в створ: <code>{_fmt(m.get('shots_on_target_total'), 0)}</code>\nУдары из штрафной: <code>{_fmt(m.get('shots_in_box_total'), 0)}</code>\nНагрузка на вратарей: <code>{_fmt(m.get('save_stress'), 3)}</code>\n"
    tempo = _float(m.get("tempo"))
    text += "Темп: данные недоступны или не подтверждены источником.\n" if not tempo else f"Темп атак: <code>{tempo:.2f}</code>\n"
    text += f"Общая live intensity: <code>{_fmt(f.get('live_intensity'), 3)}</code>"
    return _trim(text)


def _team_block(name: str, attack: Any, defense: Any, scored: Any, conceded: Any, sample: Any) -> str:
    return (
        f"\n<b>{_e(name)}</b>\n"
        f"Атака: { _e(interpret_attack_factor(attack)) }\n<code>×{_fmt(attack, 3)}</code>\n"
        f"Оборона: { _e(interpret_defense_factor(defense)) }\n<code>×{_fmt(defense, 3)}</code>\n"
        f"Средние голы: {_fmt(scored)} заб / {_fmt(conceded)} проп\n"
        f"Выборка: {_int(sample)} матчей\n"
    )


def build_analysis_teams(snapshot: Dict[str, Any]) -> str:
    match, season = _dict(snapshot.get("match")), _dict(snapshot.get("season_context"))
    text = _header(snapshot, "⚔️ КОМАНДЫ")
    text += _team_block(match.get("home_team_name", "Home"), season.get("home_attack_factor"), season.get("home_defense_factor"), season.get("home_avg_scored"), season.get("home_avg_conceded"), season.get("home_matches"))
    text += _team_block(match.get("away_team_name", "Away"), season.get("away_attack_factor"), season.get("away_defense_factor"), season.get("away_avg_scored"), season.get("away_avg_conceded"), season.get("away_matches"))
    return _trim(text)


def build_analysis_season(snapshot: Dict[str, Any]) -> str:
    match, season, factors = _dict(snapshot.get("match")), _dict(snapshot.get("season_context")), _dict(snapshot.get("factors"))
    avg = _float(season.get("league_avg_goals"))
    label = "высокая" if avg is not None and avg >= 3.0 else "низкая" if avg is not None and avg < 2.2 else "средняя"
    factor = factors.get("season_context_factor_45p", season.get("season_context_factor_45p"))
    text = _header(snapshot, "🏆 СЕЗОН И ТУРНИР")
    text += f"\nТурнир: <b>{_e(match.get('league_name'))}</b>\nТип: {_e(match.get('league_type'))}\nКубковый матч: {'да' if match.get('is_cup') is True else 'нет' if match.get('is_cup') is False else 'не определено'}\n"
    text += f"\n<b>Результативность турнира</b>\n{label.capitalize()}\nСреднее: {_fmt(avg)} гола за матч\nМножитель лиги: <code>×{_fmt(season.get('league_factor'), 3)}</code>\n"
    text += f"\n<b>Сезонный контекст</b>\n{_e(interpret_context_factor(factor))}\nМножитель: <code>×{_fmt(factor, 3)}</code>\nTeam mix: <code>×{_fmt(season.get('team_mix_factor'), 3)}</code>"
    return _trim(text)


def build_analysis_second_half(snapshot: Dict[str, Any]) -> str:
    f, c = _dict(snapshot.get("factors")), _dict(snapshot.get("second_half"))
    text = _header(snapshot, "⏱ ВТОРОЙ ТАЙМ")
    if _two_h_missing(snapshot):
        text += "\n<b>Статистика второго тайма</b>\nНедостаточно данных\n\nДля этого матча модель использовала нейтральное базовое значение. Этот блок не изменил итоговую вероятность.\n"
    else:
        text += f"\nКомандный фактор: {interpret_factor(f.get('team_2h_factor'))} (<code>×{_fmt(f.get('team_2h_factor'), 3)}</code>)\n"
        text += f"Фактор лиги: {interpret_factor(f.get('league_2h_factor'))} (<code>×{_fmt(f.get('league_2h_factor'), 3)}</code>)\n"
    confidence = _float(c.get("sample_confidence", f.get("sample_confidence")), 0.0) or 0.0
    confidence_label = "низкая" if confidence < 0.25 else "умеренная" if confidence < 0.60 else "высокая"
    text += f"\nНадёжность данных: <b>{confidence_label}</b>\n<code>confidence={confidence:.3f}</code> (это не вероятность гола)\n"
    text += f"Выборки: home {_int(c.get('team_sample_home'))}, away {_int(c.get('team_sample_away'))}, league {_int(c.get('league_sample'))}\n"
    text += f"Контекст счёта: <code>×{_fmt(f.get('score_state_factor'), 3)}</code>\nContext multiplier: <code>×{_fmt(f.get('context_multiplier'), 3)}</code>"
    return _trim(text)


def build_analysis_technical(snapshot: Dict[str, Any]) -> str:
    p, d, g, f = (_dict(snapshot.get(k)) for k in ("probabilities", "decision", "gates", "factors"))
    lines = [_header(snapshot, "🔬 ДАННЫЕ МОДЕЛИ"), ""]
    for label, value in (
        ("live_intensity", f.get("live_intensity")), ("adjusted_intensity", f.get("adjusted_intensity")),
        ("lambda_2h", f.get("lambda_2h")), ("prob_next_15", p.get("prob_next_15")),
        ("prob_next_25", p.get("prob_next_25")), ("prob_to75", p.get("prob_to75")), ("prob_to90", p.get("prob_to90")),
        ("selected_threshold", d.get("selected_prob_to90_threshold")), ("decision_margin", d.get("decision_margin_to90")),
        ("threshold_source", d.get("threshold_source")), ("minute_bucket", d.get("minute_bucket")),
        ("game_state_factor", f.get("game_state_factor")), ("goal_xg_gap_factor", f.get("goal_xg_gap_factor")),
        ("xg_delta_factor", f.get("xg_delta_factor")), ("urgency_factor", f.get("urgency_factor")),
        ("season_factor", f.get("season_context_factor_45p")), ("team_2h_factor", f.get("team_2h_factor")),
        ("league_2h_factor", f.get("league_2h_factor")), ("readiness", g.get("readiness_passed")),
        ("live_gate", g.get("live_gate_passed")), ("rule_path", g.get("rule_path")),
        ("decision_id", snapshot.get("decision_id")),
    ):
        lines.append(f"<code>{_e(label)}={_e(value)}</code>")
    return _trim("\n".join(lines))


def build_screen(snapshot: Dict[str, Any], screen: str) -> str:
    renderers = {
        "overview": build_analysis_overview, "live": build_analysis_live,
        "teams": build_analysis_teams, "season": build_analysis_season,
        "2h": build_analysis_second_half, "technical": build_analysis_technical,
    }
    return renderers.get(screen, build_analysis_overview)(deepcopy(snapshot))


def build_navigation_keyboard(signal_key: str, prefix: str = "analysis") -> Dict[str, Any]:
    cb = lambda screen: f"{prefix}:{screen}:{signal_key}"
    return {"inline_keyboard": [
        [{"text": "🔥 Live", "callback_data": cb("live")}, {"text": "⚔️ Команды", "callback_data": cb("teams")}],
        [{"text": "🏆 Сезон", "callback_data": cb("season")}, {"text": "⏱ Второй тайм", "callback_data": cb("2h")}],
        [{"text": "🔬 Данные модели", "callback_data": cb("technical")}],
        [{"text": "🔄 Обзор", "callback_data": cb("overview")}],
    ]}


def make_signal_key(decision_id: str) -> str:
    return hashlib.sha256(str(decision_id).encode("utf-8")).hexdigest()[:12]


def build_signal_keyboard(enabled: bool, bot_username: Optional[str], signal_key: str, mode: str = "private_deeplink") -> Optional[Dict[str, Any]]:
    if not enabled or mode != "private_deeplink" or not bot_username or not VALID_SIGNAL_KEY.fullmatch(str(signal_key)):
        return None
    username = str(bot_username).lstrip("@").strip()
    if not username:
        return None
    return {"inline_keyboard": [[{"text": "🧠 Разбор сигнала", "url": f"https://t.me/{quote(username)}?start=analysis_{signal_key}"}]]}


def _atomic_json(path: str, payload: Dict[str, Any]) -> None:
    parent = os.path.dirname(os.path.abspath(path))
    os.makedirs(parent, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".analysis_", suffix=".tmp", dir=parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, separators=(",", ":"))
            fh.flush()
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def save_analysis_snapshot(snapshot: Dict[str, Any], original_text: str, index_file: str, ttl_seconds: int) -> Optional[str]:
    try:
        record = deepcopy(snapshot)
        decision_id = str(record.get("decision_id") or record.get("decision_key") or "")
        if not decision_id:
            return None
        key = make_signal_key(decision_id)
        base = os.path.dirname(os.path.abspath(index_file))
        snapshot_path = os.path.join(base, "signal_analysis_snapshots", f"{key}.json")
        record["original_text"] = str(original_text or "")
        record["analysis_created_at_utc"] = datetime.now(timezone.utc).isoformat()
        _atomic_json(snapshot_path, record)
        index: Dict[str, Any] = {"schema_version": 1, "signals": {}}
        if os.path.exists(index_file):
            try:
                with open(index_file, "r", encoding="utf-8") as fh:
                    loaded = json.load(fh)
                    if isinstance(loaded, dict):
                        index = loaded
            except Exception:
                LOGGER.exception("[ANALYSIS_UI_ERROR] stage=load_index signal_key=%s error_type=index_read", key)
        signals = index.setdefault("signals", {})
        now = time.time()
        for old_key, meta in list(signals.items()):
            if now - float(_dict(meta).get("created_at_ts", 0) or 0) > max(ttl_seconds * 2, 86400):
                signals.pop(old_key, None)
        signals[key] = {
            "signal_key": key, "fixture_id": record.get("fixture_id"), "decision_id": decision_id,
            "snapshot_file": os.path.relpath(snapshot_path, base), "created_at_utc": record["analysis_created_at_utc"],
            "created_at_ts": now,
        }
        _atomic_json(index_file, index)
        return key
    except Exception:
        LOGGER.exception("[ANALYSIS_UI_ERROR] stage=save_snapshot signal_key=unknown error_type=write")
        return None


def load_analysis_snapshot(signal_key: str, index_file: str, ttl_seconds: int) -> Tuple[Optional[Dict[str, Any]], str]:
    if not VALID_SIGNAL_KEY.fullmatch(str(signal_key)):
        return None, "malformed"
    try:
        with open(index_file, "r", encoding="utf-8") as fh:
            index = json.load(fh)
        meta = _dict(_dict(index.get("signals")).get(signal_key))
        if not meta:
            return None, "not_found"
        if time.time() - float(meta.get("created_at_ts", 0) or 0) > ttl_seconds:
            return None, "expired"
        base = os.path.dirname(os.path.abspath(index_file))
        path = os.path.abspath(os.path.join(base, str(meta.get("snapshot_file") or "")))
        if os.path.commonpath([base, path]) != base:
            return None, "malformed"
        with open(path, "r", encoding="utf-8") as fh:
            snapshot = json.load(fh)
        return (snapshot, "ok") if isinstance(snapshot, dict) else (None, "outdated_schema")
    except FileNotFoundError:
        return None, "not_found"
    except Exception:
        LOGGER.exception("[ANALYSIS_UI_ERROR] stage=load_snapshot signal_key=%s error_type=read", signal_key)
        return None, "error"


def parse_analysis_start(text: str) -> Optional[str]:
    match = re.fullmatch(r"/start(?:@[A-Za-z0-9_]+)?\s+analysis_([A-Za-z0-9_-]{6,32})", str(text or "").strip())
    return match.group(1) if match else None


def can_access_analysis(user_id: int, snapshot: Dict[str, Any]) -> bool:
    return True


def is_analysis_callback(data: Any, prefix: str = "analysis") -> bool:
    return isinstance(data, str) and data.startswith(prefix + ":")


def handle_analysis_callback(
    callback_query: Dict[str, Any], *, prefix: str, index_file: str, ttl_seconds: int,
    answer_callback: Callable[[str, str, bool], Any], edit_message: Callable[[int, int, str, Dict[str, Any]], Any],
    logger: Optional[logging.Logger] = None,
) -> bool:
    log = logger or LOGGER
    callback_id = str(callback_query.get("id") or "")
    data = str(callback_query.get("data") or "")
    user_id = _int(_dict(callback_query.get("from")).get("id"))
    try:
        answer_callback(callback_id, "", False)
    except Exception:
        log.exception("[ANALYSIS_UI_ERROR] stage=answer_callback signal_key=unknown error_type=telegram")
    try:
        parts = data.split(":")
        if len(parts) != 3 or parts[0] != prefix or parts[1] not in SCREENS or not VALID_SIGNAL_KEY.fullmatch(parts[2]):
            log.warning("[ANALYSIS_UI_ERROR] stage=parse_callback signal_key=unknown error_type=malformed")
            return True
        _, screen, key = parts
        snapshot, status = load_analysis_snapshot(key, index_file, ttl_seconds)
        log.info("[ANALYSIS_UI_NAV] user_id=%s signal_key=%s from_screen=unknown to_screen=%s", user_id, key, screen)
        if snapshot is None:
            try:
                message = _dict(callback_query.get("message"))
                chat_id = _int(_dict(message.get("chat")).get("id"))
                message_id = _int(message.get("message_id"))
                edit_message(
                    chat_id,
                    message_id,
                    "Аналитика этого сигнала уже недоступна. Основной сигнал остаётся в канале.",
                    {"inline_keyboard": []},
                )
            except Exception:
                pass
            log.info("[ANALYSIS_UI_OPEN] user_id=%s signal_key=%s screen=%s snapshot_found=False", user_id, key, screen)
            return True
        if not can_access_analysis(user_id, snapshot):
            answer_callback(callback_id, "Доступ к аналитике ограничен.", True)
            return True
        message = _dict(callback_query.get("message"))
        chat_id, message_id = _int(_dict(message.get("chat")).get("id")), _int(message.get("message_id"))
        edit_message(chat_id, message_id, build_screen(snapshot, screen), build_navigation_keyboard(key, prefix))
        log.info("[ANALYSIS_UI_OPEN] user_id=%s signal_key=%s screen=%s snapshot_found=True", user_id, key, screen)
        return True
    except Exception as exc:
        log.exception("[ANALYSIS_UI_ERROR] stage=callback signal_key=unknown error_type=%s", type(exc).__name__)
        return True
