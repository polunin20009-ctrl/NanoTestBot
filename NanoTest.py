#!/usr/bin/env python3
# coding: utf-8
"""
Goal Predictor Telegram Bot (API-Football powered)
Monitors live matches and sends goal prediction signals.
"""
from __future__ import annotations
import os
import sys
import io
import time
import math
import json
import shutil
import hashlib
import logging
import threading
import argparse
import requests
from collections import deque
from contextlib import contextmanager
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional, Set, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, FIRST_COMPLETED, TimeoutError

# Google Sheets integration
try:
    import gspread
    from google.oauth2.service_account import Credentials
    GSHEETS_AVAILABLE = True
except ImportError:
    GSHEETS_AVAILABLE = False
    logger_warning = "Google Sheets libraries not installed. Run: pip install gspread google-auth"

# Ensure UTF-8 stdout on Windows (best-effort)
# Skip when running under pytest to avoid capture errors.
try:
    if "pytest" not in sys.modules and "PYTEST_CURRENT_TEST" not in os.environ:
        sys.stdout = io.TextIOWrapper(sys.stdout.detach(), encoding="utf-8")
        sys.stderr = io.TextIOWrapper(sys.stderr.detach(), encoding="utf-8")
except Exception:
    pass

# -------------------------
# Configuration (embedded per user request)
# -------------------------
API_FOOTBALL_KEY = "fc7e650f8109ac4ad77e446b26a363b9"
TELEGRAM_CHAT_ID = -1002161710363  # Numeric ID for private channel (was @xtrzv)
TELEGRAM_TOKEN = "7794472947:AAG3NpELJvpk4gG7EI5dfWUpvSR7LZFtXnM"

def save_snapshot(snapshot: dict) -> None:
    os.makedirs("zzz.json", exist_ok=True)

    file_path = os.path.join("zzz.json", "match_snapshots.jsonl")

    with open(file_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(snapshot, ensure_ascii=False) + "\n")

def save_outcome(outcome: dict) -> None:
    os.makedirs("zzz.json", exist_ok=True)

    file_path = os.path.join("zzz.json", "match_outcomes.jsonl")

    with open(file_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(outcome, ensure_ascii=False) + "\n")

def load_existing_outcome_keys() -> set:
    """
    Load existing outcome keys as set of (match_id, signal_ts_utc) tuples.
    """
    file_path = os.path.join("zzz.json", "match_outcomes.jsonl")
    if not os.path.exists(file_path):
        return set()
    
    keys = set()
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        outcome = json.loads(line)
                        match_id = outcome.get("match_id")
                        signal_ts_utc = outcome.get("signal_ts_utc")
                        if match_id is not None and signal_ts_utc is not None:
                            keys.add((match_id, signal_ts_utc))
                    except json.JSONDecodeError:
                        continue
    except Exception as e:
        logger.error(f"[OUTCOME] Failed to load existing outcome keys: {e}")
    
    return keys

# -------------------------
# Configuration Loading from config.json
# -------------------------
def load_config() -> Dict[str, Any]:
    """
    Load configuration from config.json.
    
    Returns:
        Dictionary with configuration, or empty dict if file doesn't exist.
    """
    config_file = "config.json"
    
    if not os.path.exists(config_file):
        logging.error(f"[CONFIG] File '{config_file}' not found. Review mode will be disabled.")
        return {}
    
    try:
        with open(config_file, "r", encoding="utf-8") as f:
            config = json.load(f)
        return config
    except json.JSONDecodeError as e:
        logging.error(f"[CONFIG] Failed to parse '{config_file}': {e}. Review mode will be disabled.")
        return {}
    except Exception as e:
        logging.error(f"[CONFIG] Error reading '{config_file}': {e}. Review mode will be disabled.")
        return {}


def _get_admin_user_id(config: Dict[str, Any]) -> Optional[int]:
    """
    Extract and validate admin_user_id from config.
    
    Args:
        config: Configuration dictionary from config.json
        
    Returns:
        Admin user ID as int, or None if not found or invalid
    """
    if not config:
        logging.error("[CONFIG] Config is empty. Review mode will be disabled.")
        return None
    
    admin_id_value = config.get("admin_user_id")
    
    if admin_id_value is None:
        logging.error("[CONFIG] admin_user_id not found in config.json. Review mode will be disabled.")
        return None
    
    # Handle both int and string formats
    try:
        admin_id = int(admin_id_value)
        if admin_id <= 0:
            logging.error(f"[CONFIG] admin_user_id must be positive integer, got {admin_id}")
            return None
        logging.info(f"[CONFIG] Admin user id loaded: {admin_id}")
        return admin_id
    except (ValueError, TypeError) as e:
        logging.error(f"[CONFIG] admin_user_id is not a valid integer: {admin_id_value} ({e})")
        return None


def _get_auto_approve_threshold(config: Dict[str, Any]) -> float:
    """
    Extract and validate auto_approve_threshold from config.

    Returns:
        Threshold as float, defaults to 7.0 on invalid/missing value.
    """
    default_threshold = 7.0
    if not isinstance(config, dict):
        return default_threshold

    raw_value = config.get("auto_approve_threshold", default_threshold)
    try:
        threshold = float(raw_value)
        if threshold < 0:
            logging.warning(f"[CONFIG] auto_approve_threshold must be non-negative, got {threshold}. Using default {default_threshold}")
            return default_threshold
        logging.info(f"[CONFIG] Auto approve threshold loaded: {threshold}")
        return threshold
    except (ValueError, TypeError):
        logging.warning(f"[CONFIG] auto_approve_threshold is invalid: {raw_value}. Using default {default_threshold}")
        return default_threshold


# Load config and set ADMIN_USER_ID at startup
CONFIG = load_config()
ADMIN_USER_ID = _get_admin_user_id(CONFIG)
AUTO_APPROVE_THRESHOLD = _get_auto_approve_threshold(CONFIG)

# Channel configuration for daily stats (legacy, use TELEGRAM_CHAT_ID instead)
CHANNEL_ID = -1002161710363  # Numeric ID of the channel
STATS_MESSAGE_ID = 316         # ID of the pinned stats message to edit daily

API_FOOTBALL_HOST = os.environ.get("API_FOOTBALL_HOST", "v3.football.api-sports.io")
CHECK_INTERVAL = int(os.environ.get("CHECK_INTERVAL", "60"))
MONITOR_INTERVAL = int(os.environ.get("MONITOR_INTERVAL", "60"))  # per-minute monitor interval
UPDATE_INTERVAL_AFTER_SEND = int(os.environ.get("UPDATE_INTERVAL_AFTER_SEND", "60"))
MATCH_MIN_MINUTE = int(os.environ.get("MATCH_MIN_MINUTE", "30"))
MATCH_MAX_MINUTE = int(os.environ.get("MATCH_MAX_MINUTE", "45"))
PROB_SEND_THRESHOLD = float(os.environ.get("PROB_SEND_THRESHOLD", "80"))
REVIEW_MIN_THRESHOLD = 50.0  # Minimum probability for REVIEW mode
REVIEW_MAX_THRESHOLD = 79.999  # Maximum probability for REVIEW mode (exclusive of 80% send)
REVIEW_TARGET_CHAT = "@OrgazmDonor500"  # REVIEW recipient username
REVIEW_TIMEOUT_MINUTES = 10  # Auto-skip review after this many minutes
ADMIN_REVIEW_INTERVAL = 60  # seconds between admin review updates
ADMIN_REVIEW_EDIT_GUARD = 55  # minimum seconds between edits
TG_EDIT_GLOBAL_LIMIT_PER_MIN = int(os.environ.get("TG_EDIT_GLOBAL_LIMIT_PER_MIN", "30"))
TG_EDIT_PER_MESSAGE_SECONDS = int(os.environ.get("TG_EDIT_PER_MESSAGE_SECONDS", "60"))
PERSIST_DIR = os.environ.get("PERSIST_DIR", "zzz.json")
CORE_PATH = os.path.join(PERSIST_DIR, "persist_state.json")
MATCHES_PATH = os.path.join(PERSIST_DIR, "persist_matches.json")
PERSIST_LEAGUES_PATH = os.path.join(PERSIST_DIR, "persist_leagues.json")
PERSIST_TEAMS_PATH = os.path.join(PERSIST_DIR, "persist_teams.json")
LEAGUES_PATH = PERSIST_LEAGUES_PATH
PERSIST_STATE_PATH = CORE_PATH
TEMP_STATE_PATH = "bot_state.json"
STATE_FILE = os.environ.get("STATE_FILE", TEMP_STATE_PATH)
GOAL_CONFIRM_SECONDS = int(os.environ.get("GOAL_CONFIRM_SECONDS", "25"))
FINISHED_STATUSES = {"FT", "AET", "PEN"}

LEAGUE_FACTOR_TTL_DAYS = 7
LEAGUE_REFRESH_SAMPLE_TARGET = 200
LEAGUE_FACTOR_BASELINE_GOALS = 2.65
LEAGUE_FACTOR_CLAMP_MIN = 0.85
LEAGUE_FACTOR_CLAMP_MAX = 1.20
LEAGUE_REFRESH_MAX_PER_CYCLE = 5
LEAGUE_REFRESH_COOLDOWN_HOURS = 6
LEAGUE_QUEUE_PROCESS_INTERVAL_SECONDS = 60
LEAGUE_CUP_SAMPLE_TARGET = 30
LEAGUE_CUP_MIN_SAMPLE = 10
LEAGUE_CUP_MAX_SEASONS_BACK = 6
TEAM_STATS_SAMPLE_SIZE = 10
TEAM_STATS_TTL_DAYS = 8
TEAM_STATS_COOLDOWN_HOURS = 24

MSK = ZoneInfo("Europe/Moscow")


def now_msk() -> datetime:
    return datetime.now(MSK)


def clamp(x: float, a: float, b: float) -> float:
    """Clamp numeric value x to inclusive range [a, b]."""
    return max(a, min(b, x))


ACTIVE_FIXTURES: Set[int] = set()

API_RETRY_COUNT = int(os.environ.get("API_RETRY_COUNT", "3"))
API_BACKOFF_FACTOR = float(os.environ.get("API_BACKOFF_FACTOR", "1.0"))
CACHE_TTL = int(os.environ.get("CACHE_TTL", "300"))
SAVE_INTERVAL = int(os.environ.get("SAVE_INTERVAL", "30"))

# Temporary global switch to disable any auto-cleanup behavior
ENABLE_CLEANUP = False

HEURISTIC_BOOST = float(os.environ.get("HEURISTIC_BOOST", "1.6"))

# -------------------------
# Goal Probability Model Constants
# -------------------------
# Базовая интенсивность голов
BASE_LAMBDA = 0.6

# Коэффициенты влияния метрик на lambda
ALPHA = 1.0      # вклад xG (expected goals)
BETA = 0.3       # вклад ударов в створ
GAMMA = 0.05     # вклад угловых
DELTA = 0.05     # вклад pressure_index
EPSILON = 0.5    # вклад нагрузки на вратаря (save_stress)

# Ограничения вероятности
MIN_PROBABILITY = 0.01
MAX_PROBABILITY = 0.99

# Cup league name patterns (for fallback type detection)
CUP_NAME_PATTERNS = {
    "cup", "copa", "coppa", "cupe", "taça", "taca",
    "pokal", "کوپا", "kupa", "cupei", "coupe",
    "fa cup", "league cup", "super cup", "supercup",
    "champions trophy", "charity shield", "trophy",
    "cup final", "cup competition", "knockout",
    "carabao", "capital one", "ligacup", "dfb pokal",
}

# League names that should be treated as Cup despite automatic detection
CUP_LEAGUE_EXCEPTIONS = {
    "CAF Champions League",
    "UEFA Europa League",
    "UEFA Europa Conference League",
    "UEFA Champions League",
    "KNVB Beker",
    "AFC Champions League",
    "CONMEBOL Libertadores",
    "CONMEBOL Sudamericana",
    "CONCACAF Champions League",
}

# Persist-only league factors are keyed by league_id in persist_leagues.json.
# Default average is used only as neutral baseline in formulas when persist avg is unavailable.
DEFAULT_LEAGUE_AVG_GOALS = LEAGUE_FACTOR_BASELINE_GOALS

# Boost V2 constants (persist-only factors)
GLOBAL_GOALS_BASE = 2.70
SHRINK_K = 6
WL = 0.5
WT = 0.5
FINAL_PROB_CAP = 0.97

# -------------------------
# Fallback Mode Constants (incomplete statistics handling)
# -------------------------
FALLBACK_PENALTY_MAX = 0.25           # максимальный штраф к порогу при низком coverage
FALLBACK_MIN_COVERAGE = 0.35          # ниже этого блокируем матч (слишком мало данных)
ESTIMATED_XG_GATE = 3.2               # жесткий шлюз: если estimated_total_xg >= этого, отправляем сигнал
FALLBACK_XG_CLAMP_MAX = 4.5           # макс xG для одной команды
FALLBACK_TOTAL_XG_CLAMP_MAX = 7.5     # макс суммарный xG

# Google Sheets configuration
GOOGLE_SHEETS_URL = "https://docs.google.com/spreadsheets/d/1QWzm00fLgw6W_MykUWAL2c6j23-ATvf1xWB5jyLvqv4/edit?gid=0"
GOOGLE_SERVICE_ACCOUNT_FILE = "goal-signal-bot-xxxx.json"

# -------------------------
# Logging Setup
# -------------------------
def setup_logging():
    """
    Setup logging for the bot.
    
    Configures logging to output to both stdout (for pm2) and a log file.
    Prevents duplicate handlers if called multiple times.
    
    Returns:
        logger: Configured logger instance
    """
    logger = logging.getLogger("mat")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    
    # Avoid adding duplicate handlers
    if logger.handlers:
        return logger
    
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s: %(message)s"
    )
    
    # Console handler (stdout for pm2)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    
    # File handler
    file_handler = logging.FileHandler(
        "goal_predictor.log",
        encoding="utf-8"
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    return logger

# Initialize logger (will be properly configured when setup_logging() is called)
logger = logging.getLogger("mat")

# -------------------------
# Google Sheets integration
# -------------------------
_gsheets_client = None
_gsheets_sheet = None
_gsheets_lock = threading.RLock()

# Required headers for Google Sheets (exact names from the table)
# Format: 1 row = 1 signal with data frozen at signal time
# СТРОГИЙ ПОРЯДОК КОЛОНОК - НЕ МЕНЯТЬ!
REQUIRED_HEADERS = [
    "ID матча",                        # 1
    "ID лиги",                         # 2
    "Лига",                            # 3
    "Сезон",                           # 4
    "ID хозяев",                       # 5
    "Хозяева",                         # 6
    "ID гостей",                       # 7
    "Гости",                           # 8
    "Минута сигнала",                  # 9
    "Статус матча",                    # 10
    "Время сигнала (UTC)",             # 11
    "Голы хозяев",                     # 12
    "Голы гостей",                     # 13
    "xG хозяев",                       # 14
    "xG гостей",                       # 15
    "Разница xG",                      # 16
    "Удары хозяев",                    # 17
    "Удары гостей",                    # 18
    "В створ хозяева",                 # 19
    "В створ гости",                   # 20
    "Удары из штрафной хозяева",       # 21
    "Удары из штрафной гости",         # 22
    "Сейвы хозяев",                    # 23
    "Сейвы гостей",                    # 24
    "Нагрузка на вратаря",             # 25
    "Владение хозяев",                 # 26
    "Владение гостей",                 # 27
    "Угловые хозяев",                  # 28
    "Угловые гостей",                  # 29
    "Давление через владение",         # 30
    "Индекс давления",                 # 31
    "Соотношение ударов",              # 32
    "Вероятность гола до 75",          # 33
    "Вероятность гола до 90",          # 34
    "Фактор лиги",                     # 35
    "Фактор команд",                   # 36
    "Общий буст",                      # 37
    "Оценка матча",                    # 38
    "Источник сигнала",                # 39
    "Версия схемы",                    # 40
    "Версия модели вероятности",       # 41
    "Версия оценки матча",             # 42
    "Гол после 15 минут",              # 43
    "Гол после 30 минут",              # 44
    "Гол после 45 минут",              # 45
    "Гол до 75 минуты",                # 46
    "Гол до конца матча",              # 47
    "Минута первого гола",             # 48
    "Время до следующего гола",        # 49
    "Команда следующего гола",         # 50
    "Время финализации",               # 51
]

# Mapping from internal English keys to Russian headers
ENGLISH_TO_RUSSIAN_HEADERS = {
    "match_id": "ID матча",
    "league_id": "ID лиги",
    "league_name": "Лига",
    "season": "Сезон",
    "home_team_id": "ID хозяев",
    "home_team_name": "Хозяева",
    "away_team_id": "ID гостей",
    "away_team_name": "Гости",
    "minute": "Минута сигнала",
    "status_short": "Статус матча",
    "signal_ts_utc": "Время сигнала (UTC)",
    "score_home": "Голы хозяев",
    "score_away": "Голы гостей",
    "xg_home": "xG хозяев",
    "xg_away": "xG гостей",
    "xg_delta": "Разница xG",
    "shots_home": "Удары хозяев",
    "shots_away": "Удары гостей",
    "shots_on_target_home": "В створ хозяева",
    "shots_on_target_away": "В створ гости",
    "shots_in_box_home": "Удары из штрафной хозяева",
    "shots_in_box_away": "Удары из штрафной гости",
    "saves_home": "Сейвы хозяев",
    "saves_away": "Сейвы гостей",
    "save_stress": "Нагрузка на вратаря",
    "possession_home": "Владение хозяев",
    "possession_away": "Владение гостей",
    "corners_home": "Угловые хозяев",
    "corners_away": "Угловые гостей",
    "possession_pressure": "Давление через владение",
    "pressure_index": "Индекс давления",
    "shots_ratio": "Соотношение ударов",
    "prob_goal_75": "Вероятность гола до 75",
    "prob_goal_90": "Вероятность гола до 90",
    "league_factor": "Фактор лиги",
    "team_mix_factor": "Фактор команд",
    "combined_M": "Общий буст",
    "match_score": "Оценка матча",
    "signal_source": "Источник сигнала",
    "schema_version": "Версия схемы",
    "prob_model_version": "Версия модели вероятности",
    "match_score_version": "Версия оценки матча",
    "goal_after_15": "Гол после 15 минут",
    "goal_after_30": "Гол после 30 минут",
    "goal_after_45": "Гол после 45 минут",
    "goal_to_75": "Гол до 75 минуты",
    "goal_to_ft": "Гол до конца матча",
    "first_goal_minute": "Минута первого гола",
    "time_to_next_goal_min": "Время до следующего гола",
    "next_goal_team": "Команда следующего гола",
    "labels_finalized_ts_utc": "Время финализации",
}

# Mapping from internal keys to Russian headers (for backwards compatibility in code)
HEADER_MAPPING = {
    "timestamp": "дата",
    "match_id": "id_матча",
    "match": "матч",
    "league": "лига",
    "signal_minute": "минута_сигнала",
    "score_at_signal": "счет",
    "xg_home": "xg_дом",
    "xg_away": "xg_гости",
    "shots_total_home": "удары_дом",
    "shots_total_away": "удары_гости",
    "shots_on_target_home": "удары_в_створ_дом",
    "shots_on_target_away": "удары_в_створ_гости",
    "saves_home": "сэйвы_дом",
    "saves_away": "сэйвы_гости",
    "shots_in_box_home": "удары_из_штрафной_дом",
    "shots_in_box_away": "удары_из_штрафной_гости",
    "corners_home": "угловые_дом",
    "corners_away": "угловые_гости",
    "possession_home": "владение_дом",
    "possession_away": "владение_гости",
    "prob_until_end": "вероятность_до_конца",
    "prob_next_15": "вероятность_15_минут",
    "pressure_index": "индекс_давления",
    "xg_delta": "xg_delta",
    "shots_ratio": "shots_ratio",
    "save_stress": "save_stress",
    "possession_pressure": "possession_pressure",
    "goal_next_15": "гол_после_15_минут",
    "goal_next_30": "гол_после_30_минут",
    "goal_next_45": "гол_после_45_минут",
    "first_goal_minute_after_signal": "минута_первого_гола",
    "time_to_first_goal": "время_первого_гола",
    "goal_until_end": "гол_до_конца_матча",
    "signal_source": "signal_source",
    "league_factor": "league_factor",
    "match_score": "оценка_матча",
}

def extract_value(x: Any, default: Any = 0) -> Any:
    """
    Extract numeric value from API response.
    
    API-Football sometimes returns numbers directly, sometimes as dict {"value": N, "source": "..."}.
    This function normalizes both cases to a simple numeric value.
    
    Args:
        x: Value to extract (can be int, float, dict, or None)
        default: Default value if extraction fails
        
    Returns:
        Extracted numeric value or default
    """
    if x is None:
        return default
    
    if isinstance(x, dict):
        v = x.get("value", default)
        try:
            return int(v)
        except:
            try:
                return float(v)
            except:
                return default
    
    # If already a number or string, try to convert
    try:
        return int(x)
    except:
        try:
            return float(x)
        except:
            return default

def gsheets_scalar(v: Any) -> Any:
    """
    Convert any value to a Google Sheets safe scalar.
    
    Ensures that complex types (dict, list) are converted to strings,
    preventing APIError 400 when writing to Google Sheets.
    
    Args:
        v: Any value to convert
        
    Returns:
        Google Sheets compatible scalar value (str, int, float, or "")
    """
    # None -> empty string
    if v is None:
        return ""
    
    # Handle dict - extract value/name or serialize to JSON
    if isinstance(v, dict):
        # Try common keys
        if "value" in v:
            result = v["value"]
            # Recursively convert in case value is also complex
            return gsheets_scalar(result)
        elif "name" in v:
            result = v["name"]
            return gsheets_scalar(result)
        else:
            # Serialize to JSON as fallback
            serialized = json.dumps(v, ensure_ascii=False)
            logger.debug(f"[GSHEETS] Converted dict to JSON: {serialized[:100]}")
            return serialized
    
    # Handle list/tuple/set - join with separator or serialize
    if isinstance(v, (list, tuple, set)):
        if len(v) == 0:
            return ""
        # Try to join scalars
        try:
            scalar_items = [str(gsheets_scalar(item)) for item in v]
            result = " | ".join(scalar_items)
            logger.debug(f"[GSHEETS] Converted list to string: {result[:100]}")
            return result
        except:
            # Fallback to JSON
            serialized = json.dumps(list(v), ensure_ascii=False)
            logger.debug(f"[GSHEETS] Converted list to JSON: {serialized[:100]}")
            return serialized
    
    # Handle bool -> convert to int (0 or 1)
    if isinstance(v, bool):
        return int(v)
    
    # Handle float NaN/Inf -> empty string
    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return ""
    
    # All other types (str, int, float) - return as-is
    return v

def safe_float(value: Any, default: Any = "") -> Any:
    """
    Safely convert value to float, return default if conversion fails.
    
    Handles: None, "", "N/A", "%", dict objects, etc.
    
    Args:
        value: Value to convert
        default: Default value to return on failure (use "" not 0)
        
    Returns:
        Float value or default
    """
    if value is None or value == "" or value == "N/A":
        return default
    
    # Handle dict objects (e.g., {"value": 25.5, "source": "api"})
    if isinstance(value, dict):
        value = value.get("value", default)
        if value is None or value == "" or value == "N/A":
            return default
    
    # Remove % sign if present
    if isinstance(value, str):
        value = value.replace("%", "").strip()
        if not value:
            return default
    
    try:
        return float(value)
    except (ValueError, TypeError):
        return default

def calculate_goal_probability(
    xg_home: float,
    xg_away: float,
    shots_on_target_home: float,
    shots_on_target_away: float,
    corners_home: float,
    corners_away: float,
    pressure_index: float,
    save_stress: float,
    current_minute: int
) -> Dict[str, float]:
    """
    Расчёт вероятности гола до 75 и до 90 минут.
    
    Математическая модель основана на модели Пуассона с линейной комбинацией метрик.
    Формула: lambda = base + alpha*xg + beta*shots + gamma*corners + delta*pressure + epsilon*save_stress
    Вероятность: P(гол) = 1 - exp(-lambda * time_factor)
    
    Args:
        xg_home: Expected goals домашней команды
        xg_away: Expected goals гостевой команды
        shots_on_target_home: Удары в створ домашней команды
        shots_on_target_away: Удары в створ гостевой команды
        corners_home: Угловые домашней команды
        corners_away: Угловые гостевой команды
        pressure_index: Индекс давления матча
        save_stress: Нагрузка на вратаря (saves / shots_on_target)
        current_minute: Текущая минута матча
    
    Returns:
        Словарь с вероятностями:
        - prob_until_75: Вероятность гола до 75 минуты (0.0-1.0)
        - prob_until_90: Вероятность гола до 90 минуты (0.0-1.0)
    """
    # ШАГ 1. Суммарные значения
    xg_total = xg_home + xg_away
    shots_on_target_total = shots_on_target_home + shots_on_target_away
    corners_total = corners_home + corners_away
    
    # ШАГ 2. Расчёт базового lambda (ожидаемое количество голов)
    lambda_base = (
        BASE_LAMBDA +
        ALPHA * xg_total +
        BETA * shots_on_target_total +
        GAMMA * corners_total +
        DELTA * pressure_index +
        EPSILON * save_stress
    )
    
    # ШАГ 3. Корректировка lambda по оставшемуся времени
    
    # Для 75 минут
    if current_minute >= 75:
        prob_until_75 = 0.0
    else:
        remaining_minutes_75 = max(75 - current_minute, 1)
        time_factor_75 = remaining_minutes_75 / 60.0
        lambda_75 = lambda_base * time_factor_75
        
        # ШАГ 4. Перевод lambda → вероятность (модель Пуассона)
        # P(хотя бы 1 гол) = 1 - P(0 голов) = 1 - exp(-lambda)
        prob_until_75 = 1.0 - math.exp(-lambda_75)
    
    # Для 90 минут
    if current_minute >= 90:
        prob_until_90 = 0.0
    else:
        remaining_minutes_90 = max(90 - current_minute, 1)
        time_factor_90 = remaining_minutes_90 / 75.0
        lambda_90 = lambda_base * time_factor_90
        
        # ШАГ 4. Перевод lambda → вероятность (модель Пуассона)
        prob_until_90 = 1.0 - math.exp(-lambda_90)
    
    # ШАГ 5. Ограничения и защита от мусора
    prob_until_75 = max(MIN_PROBABILITY, min(MAX_PROBABILITY, prob_until_75))
    prob_until_90 = max(MIN_PROBABILITY, min(MAX_PROBABILITY, prob_until_90))
    
    # ШАГ 6. Возврат результата
    return {
        "prob_until_75": prob_until_75,
        "prob_until_90": prob_until_90
    }

def calculate_save_stress(saves_home: float, saves_away: float, current_home_score: float, current_away_score: float, shots_on_target_home: float, shots_on_target_away: float) -> float:
    """
    Calculate the main save_stress value used across all scoring and admin logic.
    
    Formula:
    - total_saves = saves_home + saves_away
    - total_goals = current_home_score + current_away_score
    - total_sot = shots_on_target_home + shots_on_target_away
    
    if total_sot <= 1.0:
        save_stress = 0.0
    else:
        save_raw = (total_saves + 0.5 * total_goals) / 4.0
        volume_factor_saves = min(1.0, total_sot / 4.0)
        save_stress = min(save_raw * volume_factor_saves, 1.0)
    
    Returns:
        float save_stress value (0.0 to 1.0)
    """
    total_saves = max(0.0, float(saves_home) + float(saves_away))
    total_goals = max(0.0, float(current_home_score) + float(current_away_score))
    total_sot = max(0.0, float(shots_on_target_home) + float(shots_on_target_away))
    
    if total_sot <= 1.0:
        return 0.0
    else:
        save_raw = (total_saves + 0.5 * total_goals) / 4.0
        volume_factor_saves = min(1.0, total_sot / 4.0)
        return min(save_raw * volume_factor_saves, 1.0)


def calculate_derived_metrics(
    xg_home: Any, xg_away: Any,
    shots_on_home: Any, shots_on_away: Any,
    shots_in_box_home: Any, shots_in_box_away: Any,
    saves_home: Any,
    saves_away: Any,
    current_home_score: Any,
    current_away_score: Any,
    possession_home: Any,
    possession_away: Any,
    pressure_index: Any
) -> Dict[str, str]:
    """
    Calculate 4 derived metrics from base statistics.
    
    Returns dictionary with string values (for Google Sheets).
    If calculation fails, returns empty string "" (not 0).
    
    Metrics:
    - xg_delta: difference in expected goals
    - shots_ratio: ratio of shots on target (home vs away)
    - save_stress: goalkeeper stress from saves/goals, volume-adjusted by total shots on target
    - possession_pressure: normalized possession dominance (0..1 range for ML)
    """
    # Safely parse all inputs
    xg_h = safe_float(xg_home, 0)
    xg_a = safe_float(xg_away, 0)
    shots_on_h = safe_float(shots_on_home, 0)
    shots_on_a = safe_float(shots_on_away, 0)
    shots_box_h = safe_float(shots_in_box_home, 0)
    shots_box_a = safe_float(shots_in_box_away, 0)
    saves_h = safe_float(saves_home, 0)
    saves_a = safe_float(saves_away, 0)
    score_h = safe_float(current_home_score, 0)
    score_a = safe_float(current_away_score, 0)
    
    # Parse possession (remove % sign if present)
    def parse_possession(val: Any) -> Any:
        if val == "" or val is None:
            return ""
        # Convert to string and remove % if present
        s = str(val).strip().replace("%", "")
        try:
            return float(s)
        except Exception:
            return ""
    
    poss_h = parse_possession(possession_home)
    poss_a = parse_possession(possession_away)
    pressure = safe_float(pressure_index, 0)
    
    # Calculate derived metrics
    try:
        # xg_delta = xg_home - xg_away
        if xg_h == "" or xg_a == "":
            xg_delta = ""
        else:
            xg_delta = f"{(xg_h - xg_a):.4f}"
        
        # shots_ratio = (shots_on_target_home + 1) / (shots_on_target_away + 1)
        if shots_on_h == "" or shots_on_a == "":
            shots_ratio = ""
        else:
            shots_ratio = f"{((shots_on_h + 1) / (shots_on_a + 1)):.4f}"
        
        # save_stress = volume-adjusted goalkeeper stress from total saves + goals
        # Guard against missing values: if any input is unavailable, default to 0.0.
        required_save_stress_inputs = [
            saves_home, saves_away,
            shots_on_home, shots_on_away,
            current_home_score, current_away_score,
        ]
        if any(v is None or v == "" for v in required_save_stress_inputs):
            save_stress_value = 0.0
        else:
            save_stress_value = calculate_save_stress(
                saves_home=saves_h,
                saves_away=saves_a,
                current_home_score=score_h,
                current_away_score=score_a,
                shots_on_target_home=shots_on_h,
                shots_on_target_away=shots_on_a
            )

        save_stress = f"{save_stress_value:.4f}"
        logger.info(
            "[SAVE_STRESS] "
            f"saves={saves_h + saves_a}, "
            f"goals={score_h + score_a}, "
            f"sot_total={shots_on_h + shots_on_a}, "
            f"raw={raw:.4f}, "
            f"volume_factor={volume_factor:.4f}, "
            f"save_stress={save_stress}"
        )
        
        # possession_pressure = normalized possession dominance (0..1 range for ML)
        # Formula: abs(possession_home - possession_away) / 100.0
        # Examples: 50/50 => 0.000, 60/40 => 0.200, 70/30 => 0.400
        if poss_h == "" or poss_a == "":
            possession_pressure = ""
        else:
            possession_pressure = f"{(abs(poss_h - poss_a) / 100.0):.3f}"
        
        logger.info(
            f"[GSHEETS] derived: xg_delta={xg_delta}, shots_ratio={shots_ratio}, "
            f"save_stress={save_stress}, possession_pressure={possession_pressure}"
        )
        
        return {
            "xg_delta": xg_delta,
            "shots_ratio": shots_ratio,
            "save_stress": save_stress,
            "possession_pressure": possession_pressure
        }
        
    except Exception as e:
        logger.exception(f"[GSHEETS] Failed to calculate derived metrics: {e}")
        return {
            "xg_delta": "",
            "shots_ratio": "",
            "save_stress": "",
            "possession_pressure": ""
        }

def _unwrap_id(value: Any) -> Optional[int]:
    if isinstance(value, dict) and "value" in value:
        value = value.get("value")
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None


def get_fixture_id(obj: Any) -> Optional[int]:
    """
    Extract fixture_id from API-Sports objects.
    Supports shapes:
    - obj["fixture"]["id"]
    - obj["fixture_id"]
    - obj["id"]
    - normalized dicts with {"fixture_id": {"value": ...}}
    """
    if not obj or not isinstance(obj, dict):
        return None

    for key in ("fixture_id", "id"):
        fid = _unwrap_id(obj.get(key))
        if fid is not None:
            return fid

    fixture = obj.get("fixture")
    if isinstance(fixture, dict):
        for key in ("id", "fixture_id"):
            fid = _unwrap_id(fixture.get(key))
            if fid is not None:
                return fid

    return None


def has_stats_data(fixture_metrics: Dict[str, Any]) -> bool:
    if not fixture_metrics:
        return False
    stat_keys = (
        "total_shots_home",
        "total_shots_away",
        "shots_on_target_home",
        "shots_on_target_away",
        "shots_inside_box_home",
        "shots_inside_box_away",
        "ball_possession_home",
        "ball_possession_away",
        "corner_kicks_home",
        "corner_kicks_away",
        "expected_goals_home",
        "expected_goals_away",
    )
    return any(k in fixture_metrics for k in stat_keys)


def _normalize_headers(headers: List[Any]) -> List[str]:
    normalized = []
    for h in headers:
        if h is None:
            normalized.append("")
        else:
            normalized.append(str(h).strip())
    return normalized


def _headers_match_required(headers: List[str], required: List[str]) -> bool:
    current = [h for h in headers if h]
    return set(current) == set(required) and len(current) == len(required)


def ensure_gsheets_headers(sheet) -> List[str]:
    """
    Ensure Google Sheets has correct headers in first row.
    
    If headers are missing or don't match expected, updates row 1.
    This guarantees all new data will be written to correct columns.
    
    Args:
        sheet: gspread worksheet object
    """
    try:
        current_headers = _normalize_headers(sheet.row_values(1))
        
        non_empty_headers = [h for h in current_headers if h]

        # If headers exist, preserve them and append only missing required columns.
        if non_empty_headers:
            missing_required = [h for h in REQUIRED_HEADERS if h not in non_empty_headers]
            if not missing_required:
                logger.info("[GSHEETS] Headers already contain all required columns")
                return list(non_empty_headers)

            start_col = len(non_empty_headers) + 1
            end_col = len(non_empty_headers) + len(missing_required)
            range_name = f"{col_index_to_letter(start_col)}1:{col_index_to_letter(end_col)}1"
            sheet.update(range_name=range_name, values=[missing_required], value_input_option="RAW")
            logger.info(f"[GSHEETS] Appended missing headers: {missing_required}")
            return list(non_empty_headers) + list(missing_required)

        # Empty sheet: initialize full required header row.
        sheet.update(range_name="A1", values=[REQUIRED_HEADERS], value_input_option="RAW")
        logger.info(f"[GSHEETS] Headers initialized ({len(REQUIRED_HEADERS)} columns)")
        return list(REQUIRED_HEADERS)
        
    except Exception as e:
        logger.exception(f"[GSHEETS] Failed to ensure headers: {e}")
        return []

def get_header_map(sheet) -> Dict[str, int]:
    """
    Read header row from Google Sheets and return mapping.
    
    Args:
        sheet: Google Sheets worksheet object
        
    Returns:
        Dictionary mapping column name to column index (0-based)
    """
    try:
        headers = ensure_gsheets_headers(sheet)
        header_map = {name: idx for idx, name in enumerate(headers) if name}
        return header_map
    except Exception as e:
        logger.warning(f"[GSHEETS] Failed to read headers: {e}")
        return {}


def safe_reset_gsheets_schema(sheet) -> bool:
    """
    Safely reset Google Sheets schema to clean state.
    - Clears all data rows (keeps header)
    - Writes fresh header from REQUIRED_HEADERS
    - Logs diagnostics
    
    Use only when sheet is desynchronized (mixed schemas).
    
    Args:
        sheet: Google Sheets worksheet object
        
    Returns:
        True if successful, False otherwise
    """
    try:
        logger.warning("[GSHEETS_RESET] Starting schema reset")
        
        # Delete all rows except header
        row_count = int(getattr(sheet, "row_count", 0) or 0)
        if row_count > 1:
            sheet.delete_rows(2, row_count)
            logger.info(f"[GSHEETS_RESET] Deleted rows 2-{row_count}")
        
        # Write fresh header
        sheet.update(range_name="A1", values=[REQUIRED_HEADERS], value_input_option="RAW")
        logger.info(f"[GSHEETS_RESET] Wrote {len(REQUIRED_HEADERS)} headers: {REQUIRED_HEADERS[:3]}... (showing first 3)")
        
        # Verify
        headers_after = _normalize_headers(sheet.row_values(1))
        headers_ok = len(headers_after) >= len(REQUIRED_HEADERS)
        logger.info(f"[GSHEETS_RESET] Verification: headers_count={len(headers_after)} ok={headers_ok}")
        
        return headers_ok
    except Exception as e:
        logger.exception(f"[GSHEETS_RESET] Failed during schema reset: {e}")
        return False

def ensure_required_headers(sheet, required_headers: List[str]) -> Dict[str, int]:
    """
    Ensure required headers exist in Google Sheets.
    Restores row 1 to required_headers when empty or mismatched.
    
    Args:
        sheet: Google Sheets worksheet object
        required_headers: List of required column names
        
    Returns:
        Dictionary mapping column name to column index (0-based)
    """
    try:
        headers = _normalize_headers(sheet.row_values(1))
        non_empty_headers = [h for h in headers if h]

        if non_empty_headers:
            missing_required = [h for h in required_headers if h not in non_empty_headers]
            if missing_required:
                start_col = len(non_empty_headers) + 1
                end_col = len(non_empty_headers) + len(missing_required)
                range_name = f"{col_index_to_letter(start_col)}1:{col_index_to_letter(end_col)}1"
                sheet.update(range_name=range_name, values=[missing_required], value_input_option="RAW")
                logger.info(f"[GSHEETS] Appended required headers: {missing_required}")
                non_empty_headers.extend(missing_required)
            return {name: idx for idx, name in enumerate(non_empty_headers) if name}

        sheet.update(range_name="A1", values=[required_headers], value_input_option="RAW")
        logger.info(f"[GSHEETS] Headers initialized ({len(required_headers)} columns)")
        return {name: idx for idx, name in enumerate(required_headers) if name}
    except Exception as e:
        logger.warning(f"[GSHEETS] Failed to ensure headers: {e}")
        return {}

def init_google_sheets():
    """Initialize Google Sheets connection."""
    global _gsheets_client, _gsheets_sheet
    
    if not GSHEETS_AVAILABLE:
        logger.warning("Google Sheets libraries not available. Install: pip install gspread google-auth")
        return False
    
    if not os.path.exists(GOOGLE_SERVICE_ACCOUNT_FILE):
        logger.warning(f"Google service account file not found: {GOOGLE_SERVICE_ACCOUNT_FILE}")
        return False
    
    try:
        scope = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = Credentials.from_service_account_file(GOOGLE_SERVICE_ACCOUNT_FILE, scopes=scope)
        _gsheets_client = gspread.authorize(creds)
        _gsheets_sheet = _gsheets_client.open_by_url(GOOGLE_SHEETS_URL).sheet1
        logger.info("[GSHEETS] Successfully connected to Google Sheets")
        
        # Ensure correct headers in row 1 (auto-initialize/update if needed)
        initialized_headers = ensure_gsheets_headers(_gsheets_sheet)
        logger.info(f"[GSHEETS_HEADER] Initialized {len(initialized_headers)} columns: {initialized_headers[:5]}... (showing first 5)")
        
        return True
    except Exception as e:
        logger.exception(f"[GSHEETS] Failed to initialize Google Sheets: {e}")
        return False

def col_index_to_letter(col_idx: int) -> str:
    """
    Convert 1-based column index to Excel column letter(s).
    
    Examples:
        1 -> A, 2 -> B, 26 -> Z, 27 -> AA, etc.
    """
    result = ""
    while col_idx > 0:
        col_idx -= 1
        result = chr(65 + (col_idx % 26)) + result
        col_idx //= 26
    return result

def a1_notation(col_idx: int, row_idx: int) -> str:
    """
    Convert column and row indices to A1 notation.
    
    Args:
        col_idx: 1-based column index
        row_idx: 1-based row index
        
    Returns:
        A1 notation string (e.g., "M123")
    """
    return f"{col_index_to_letter(col_idx)}{row_idx}"

# Google Sheets constant - sheet name without quotes or exclamation mark
SHEET_NAME = "Лист1"

def a1(sheet_name: str, cell_or_range: str) -> str:
    """
    Create A1 range with sheet name.
    
    Args:
        sheet_name: Sheet name (e.g., "Лист1")
        cell_or_range: Cell or range in A1 notation (e.g., "AB2" or "A2:AI2")
        
    Returns:
        Full A1 range with sheet name (e.g., "'Лист1'!AB2")
    """
    return f"'{sheet_name}'!{cell_or_range}"

def save_match_to_sheet(match_id: int, data: Dict[str, Any], minute: int, prob_next_15: float, prob_until_end: float, signal_sent: bool = False, pressure_index: float = 0.0, momentum: float = 0.0, signal_source: str = "bot", league_factor: float = 1.0, match_score: Any = 0):
    """
    Save match signal data to Google Sheets (ONE ROW = ONE SIGNAL).
    
    Called ONLY when signal_sent=True. Adds a single row with frozen data at signal time.
    Outcome columns (goal_next_15, goal_until_end, etc.) are set to "N/A" and will be
    updated later by finalize_match_outcomes() when match finishes.
    
    NOTE: momentum parameter is IGNORED (removed from table structure).
    
    Args:
        match_id: Match ID
        data: Collected match data from API
        minute: Signal minute (frozen at signal time)
        prob_next_15: Probability of goal in next 15 minutes (%)
        prob_until_end: Probability of goal until match end (%)
        signal_sent: Must be True (only call when sending signal)
        pressure_index: PressureIndex at signal time
        momentum: IGNORED (deprecated, column removed)
        signal_source: Source of signal in channel ("admin" or "bot")
        league_factor: League factor used in signal calculation (clamped 0.85..1.20)
        match_score: Match score value (0..12), defaults to 0 if unavailable
    """
    global _gsheets_sheet
    
    if not GSHEETS_AVAILABLE or _gsheets_sheet is None:
        return
    
    # Only save when signal is sent (not on updates)
    if not signal_sent:
        return
    
    try:
        fixture = data.get("fixture", {})
        
        # === EXTRACT TEAM NAMES (safe conversion from dict to string) ===
        home_team_raw = fixture.get("team_home_name", "Unknown")
        if isinstance(home_team_raw, dict):
            home_team = str(home_team_raw.get("name") or home_team_raw.get("value") or "Unknown")
        else:
            home_team = str(home_team_raw) if home_team_raw else "Unknown"
        
        away_team_raw = fixture.get("team_away_name", "Unknown")
        if isinstance(away_team_raw, dict):
            away_team = str(away_team_raw.get("name") or away_team_raw.get("value") or "Unknown")
        else:
            away_team = str(away_team_raw) if away_team_raw else "Unknown"
        
        # === EXTRACT LEAGUE (safe conversion from dict to string) ===
        league_raw = fixture.get("league_name", "Unknown")
        if isinstance(league_raw, dict):
            league = str(league_raw.get("name") or league_raw.get("value") or "Unknown")
        else:
            league = str(league_raw) if league_raw else "Unknown"
        
        country_raw = fixture.get("country", "")
        if isinstance(country_raw, dict):
            country = str(country_raw.get("name") or country_raw.get("value") or "")
        else:
            country = str(country_raw) if country_raw else ""
        
        if country:
            league = f"{league}, {country}"
        
        # === SCORE AT SIGNAL ===
        home_score = extract_value(fixture.get("score_home", 0), 0)
        away_score = extract_value(fixture.get("score_away", 0), 0)
        score_at_signal = f"{home_score}-{away_score}"
        
        # === XG VALUES ===
        home_xg = get_any_metric(fixture, ["expected_goals"], "home") or 0.0
        away_xg = get_any_metric(fixture, ["expected_goals"], "away") or 0.0
        
        if home_xg == 0:
            home_xg = estimate_xg_from_metrics_combined(fixture, "home")
        if away_xg == 0:
            away_xg = estimate_xg_from_metrics_combined(fixture, "away")
        
        # === SHOTS STATISTICS ===
        shots_total_h = int(get_any_metric(fixture, ["total_shots"], "home") or 0)
        shots_total_a = int(get_any_metric(fixture, ["total_shots"], "away") or 0)
        shots_on_h = int(get_any_metric(fixture, ["shots_on_target"], "home") or 0)
        shots_on_a = int(get_any_metric(fixture, ["shots_on_target"], "away") or 0)
        
        # === SAVES ===
        saves_h = int(get_any_metric(fixture, ["saves", "goalkeeper_saves"], "home") or 0)
        saves_a = int(get_any_metric(fixture, ["saves", "goalkeeper_saves"], "away") or 0)
        
        # === SHOTS IN BOX ===
        shots_in_box_h = int(get_any_metric(fixture, ["shots_insidebox", "inside_box"], "home") or 0)
        shots_in_box_a = int(get_any_metric(fixture, ["shots_insidebox", "inside_box"], "away") or 0)
        
        # === CORNERS ===
        corners_h = int(get_any_metric(fixture, ["corner_kicks", "corners"], "home") or 0)
        corners_a = int(get_any_metric(fixture, ["corner_kicks", "corners"], "away") or 0)
        
        # === POSSESSION ===
        poss_h = float(get_any_metric(fixture, ["ball_possession", "passes_%"], "home") or 0)
        poss_a = float(get_any_metric(fixture, ["ball_possession", "passes_%"], "away") or 0)
        
        # === CALCULATE DERIVED METRICS ===
        derived = calculate_derived_metrics(
            xg_home=home_xg,
            xg_away=away_xg,
            shots_on_home=shots_on_h,
            shots_on_away=shots_on_a,
            shots_in_box_home=shots_in_box_h,
            shots_in_box_away=shots_in_box_a,
            saves_home=saves_h,
            saves_away=saves_a,
            current_home_score=home_score,
            current_away_score=away_score,
            possession_home=poss_h,
            possession_away=poss_a,
            pressure_index=pressure_index
        )
        
        # === TIMESTAMP ===
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        try:
            league_factor_value = float(league_factor)
        except Exception:
            league_factor_value = 1.0
        league_factor_value = float(max(LEAGUE_FACTOR_CLAMP_MIN, min(LEAGUE_FACTOR_CLAMP_MAX, league_factor_value)))

        try:
            match_score_value = int(round(float(match_score))) if match_score not in (None, "", "N/A") else 0
        except Exception:
            match_score_value = 0
        match_score_value = int(clamp(match_score_value, 0, 12))

        # === CALCULATE TEAM FACTORS ===
        try:
            team_boost = calc_match_team_boost_v2(fixture)
            team_mix_factor_value = float(team_boost.get("team_mix_factor", 1.0))
            combined_m_value = float(team_boost.get("combined_m", 1.0))
        except Exception as e:
            logger.warning(f"[GSHEETS] Failed to calculate team factors for match {match_id}: {e}")
            team_mix_factor_value = 1.0
            combined_m_value = 1.0
        
        # === BUILD ROW BY HEADER NAMES ===
        row_values = {
            "match_id": str(match_id),
            "league_id": str(fixture.get("league_id", "")),
            "league_name": league,
            "season": str(fixture.get("season", "")),
            "home_team_id": str(fixture.get("team_home_id", "")),
            "home_team_name": home_team,
            "away_team_id": str(fixture.get("team_away_id", "")),
            "away_team_name": away_team,
            "minute": str(minute),
            "status_short": str(fixture.get("status_short", "")),
            "signal_ts_utc": timestamp,
            "score_home": str(home_score),
            "score_away": str(away_score),
            "xg_home": f"{home_xg:.4f}",
            "xg_away": f"{away_xg:.4f}",
            "xg_delta": derived["xg_delta"],
            "shots_home": str(shots_total_h),
            "shots_away": str(shots_total_a),
            "shots_on_target_home": str(shots_on_h),
            "shots_on_target_away": str(shots_on_a),
            "shots_in_box_home": str(shots_in_box_h),
            "shots_in_box_away": str(shots_in_box_a),
            "saves_home": str(saves_h),
            "saves_away": str(saves_a),
            "save_stress": derived["save_stress"],
            "possession_home": f"{poss_h:.1f}",
            "possession_away": f"{poss_a:.1f}",
            "corners_home": str(corners_h),
            "corners_away": str(corners_a),
            "possession_pressure": derived["possession_pressure"],
            "pressure_index": f"{pressure_index:.4f}",
            "shots_ratio": derived["shots_ratio"],
            "prob_goal_75": f"{prob_until_end:.2f}",
            "prob_goal_90": f"{prob_next_15:.2f}",
            "league_factor": f"{league_factor_value:.3f}",
            "team_mix_factor": f"{team_mix_factor_value:.3f}",
            "combined_M": f"{combined_m_value:.3f}",
            "match_score": str(match_score_value),
            "signal_source": "admin" if str(signal_source).strip().lower() == "admin" else "bot",
            "schema_version": "1.0",
            "prob_model_version": "v2.5",
            "match_score_version": "v2",
            "goal_after_15": "N/A",
            "goal_after_30": "N/A",
            "goal_after_45": "N/A",
            "goal_to_75": "N/A",
            "goal_to_ft": "N/A",
            "first_goal_minute": "N/A",
            "time_to_next_goal_min": "N/A",
            "next_goal_team": "N/A",
            "labels_finalized_ts_utc": "N/A",
        }
        
        with _gsheets_lock:
            headers = ensure_gsheets_headers(_gsheets_sheet)
            header_map = {name: idx for idx, name in enumerate(headers) if name}
        
        missing_headers = [h for h in REQUIRED_HEADERS if h not in header_map]
        if missing_headers:
            logger.error(f"[GSHEETS_HEADER] Missing required headers: {missing_headers}")
            return
        
        row = [""] * len(headers)
        for english_key, value in row_values.items():
            russian_header = ENGLISH_TO_RUSSIAN_HEADERS.get(english_key)
            if russian_header and russian_header in header_map:
                row[header_map[russian_header]] = gsheets_scalar(value)
        
        # === VALIDATE ROW BEFORE WRITE ===
        non_empty_cols = sum(1 for v in row if str(v).strip())
        match_check = (
            row[header_map.get("ID матча", 0)] == str(match_id) if "ID матча" in header_map else False
        )
        logger.debug(f"[GSHEETS_ROW_WRITE] row_len={len(row)} non_empty={non_empty_cols} match_ok={match_check} headers={len(header_map)}")
        
        # === APPEND ROW TO GOOGLE SHEETS ===
        with _gsheets_lock:
            _gsheets_sheet.append_row(row, value_input_option="RAW")
        
        logger.info(f"[GSHEETS] Added signal row: match_id={match_id}, signal_minute={minute}")
        
    except Exception as e:
        logger.exception(f"[GSHEETS] Failed to save signal for match {match_id}: {e}")

def _extract_counted_goals_after_signal(events: List[Dict[str, Any]], signal_minute: int, match_id: int) -> Tuple[List[int], Optional[int], Optional[str]]:
    """
    Extract counted goal minutes after signal_minute from match events.
    
    Rules:
    - Only goals with type containing "goal" (case-insensitive)
    - Exclude goals with cancellation keywords (VAR, offside, disallowed, etc.)
    - Correctly handle extra time (90+4 -> minute 94)
    - Only goals at or after signal_minute
    
    Args:
        events: List of event dictionaries from API-Sports
        signal_minute: Signal minute (goals before this are ignored)
        match_id: Match ID for logging
        
    Returns:
        Tuple of (sorted_goal_minutes_list, first_goal_minute_or_None, first_goal_team_or_None)
    """
    goal_minutes = []
    goal_teams = []
    
    # Log sample of raw events for debugging
    if events:
        logger.info(f"[OUTCOME] match={match_id} Total events: {len(events)}")
        sample_events = events[:10]
        for i, ev in enumerate(sample_events):
            ev_type = ev.get("type", "")
            ev_detail = ev.get("detail", "")
            ev_time = ev.get("time", {})
            logger.debug(f"[OUTCOME] match={match_id} Event #{i}: type='{ev_type}' detail='{ev_detail}' time={ev_time}")
    else:
        logger.warning(f"[OUTCOME] match={match_id} NO EVENTS returned from API")
    
    # Process all events
    for ev in events:
        # Check if this is a valid counted goal
        if not _is_valid_counted_goal(ev):
            continue
        
        # Extract minute using correct field structure
        ev_minute = _extract_event_minute(ev)
        
        if ev_minute is None:
            logger.warning(f"[OUTCOME] match={match_id} Goal event has no minute: {ev}")
            continue
        
        # Only count goals at or after signal minute
        if ev_minute >= signal_minute:
            goal_minutes.append(ev_minute)
            # Extract team: "home" or "away"
            team = ev.get("team", "")
            if isinstance(team, dict):
                team = team.get("name", "") or team.get("value", "")
            team_str = "home" if "home" in str(team).lower() else "away"
            goal_teams.append(team_str)
            logger.info(f"[OUTCOME] match={match_id} Counted goal at minute {ev_minute} by {team_str} (signal={signal_minute})")
    
    # Sort goal minutes and corresponding teams
    if goal_minutes:
        sorted_indices = sorted(range(len(goal_minutes)), key=lambda i: goal_minutes[i])
        goal_minutes = [goal_minutes[i] for i in sorted_indices]
        goal_teams = [goal_teams[i] for i in sorted_indices]
    
    # Find first goal
    first_goal_minute = goal_minutes[0] if goal_minutes else None
    first_goal_team = goal_teams[0] if goal_teams else None
    
    # Log if no goals found
    if not goal_minutes:
        logger.warning(f"[OUTCOME] match={match_id} NO GOALS after filter. signal_minute={signal_minute}")
        # Log all goal-type events for debugging
        goal_events = [ev for ev in events if "goal" in ev.get("type", "").lower()]
        if goal_events:
            logger.info(f"[OUTCOME] match={match_id} Found {len(goal_events)} goal-type events (including cancelled):")
            for ev in goal_events[:10]:
                ev_type = ev.get("type", "")
                ev_detail = ev.get("detail", "")
                ev_minute = _extract_event_minute(ev)
                logger.info(f"  - minute={ev_minute} type='{ev_type}' detail='{ev_detail}'")
    
    return goal_minutes, first_goal_minute, first_goal_team

def finalize_match_outcomes(match_id: int, client):
    """
    Update outcome columns in Google Sheets when match finishes.
    
    Called when match status is FT/AET/PEN or elapsed >= 90 with finished status.
    Finds first counted goal after signal minute and updates outcome columns.
    
    Args:
        match_id: Match ID
        client: API client for fetching events
    """
    global _gsheets_sheet
    
    if not GSHEETS_AVAILABLE or _gsheets_sheet is None:
        return
    
    try:
        # Resolve headers and match rows using Russian column names
        with _gsheets_lock:
            headers = ensure_gsheets_headers(_gsheets_sheet)
            header_map = {name: idx for idx, name in enumerate(headers) if name}

        if not header_map:
            logger.error("[GSHEETS_HEADER] finalize: no header_map available")
            return

        # Required columns in Russian (from REQUIRED_HEADERS)
        required_cols_rus = [
            "ID матча",
            "Минута сигнала",
            "Гол после 15 минут",
            "Гол после 30 минут",
            "Гол после 45 минут",
            "Гол до 75 минуты",
            "Гол до конца матча",
            "Минута первого гола",
            "Время до следующего гола",
            "Команда следующего гола",
            "Время финализации",
        ]
        missing_cols = [c for c in required_cols_rus if c not in header_map]
        if missing_cols:
            logger.error(f"[GSHEETS_HEADER] finalize: missing columns {missing_cols}")
            return

        logger.info(f"[GSHEETS_HEADER] finalize headers matched: {len(header_map)} columns")
        
        id_col = header_map["ID матча"]
        minute_col = header_map["Минута сигнала"]

        with _gsheets_lock:
            id_col_values = _gsheets_sheet.col_values(id_col + 1)
            minute_col_values = _gsheets_sheet.col_values(minute_col + 1)

        match_rows = []
        for row_idx, value in enumerate(id_col_values[1:], start=2):
            if str(value).strip() == str(match_id):
                match_rows.append(row_idx)

        if not match_rows:
            logger.debug(f"[GSHEETS] finalize: match {match_id} not found in sheet")
            return

        # Fetch match events once
        try:
            events = client.fetch_fixture_events(match_id)
            if not events:
                events = []
        except Exception as e:
            logger.error(f"[GSHEETS] Failed to fetch events for match {match_id}: {e}")
            events = []

        # Get outcome column indices (Russian names)
        col_goal_after_15 = header_map.get("Гол после 15 минут")
        col_goal_after_30 = header_map.get("Гол после 30 минут")
        col_goal_after_45 = header_map.get("Гол после 45 минут")
        col_goal_to_75 = header_map.get("Гол до 75 минуты")
        col_goal_to_ft = header_map.get("Гол до конца матча")
        col_first_goal = header_map.get("Минута первого гола")
        col_time_to_goal = header_map.get("Время до следующего гола")
        col_next_goal_team = header_map.get("Команда следующего гола")
        col_labels_finalized = header_map.get("Время финализации")

        for row_number in match_rows:
            signal_value = minute_col_values[row_number - 1] if row_number - 1 < len(minute_col_values) else ""
            try:
                signal_minute = int(signal_value)
            except (ValueError, TypeError):
                logger.warning(f"[GSHEETS] finalize: invalid signal minute in row {row_number}: {signal_value}")
                continue

            logger.info(f"[OUTCOME] Starting finalize for match={match_id} signal_minute={signal_minute} row={row_number}")

            goal_minutes, first_goal_minute, first_goal_team = _extract_counted_goals_after_signal(events, signal_minute, match_id)

            if first_goal_minute is not None:
                goal_to_ft = 1
                goal_after_15 = 1 if first_goal_minute <= signal_minute + 15 else 0
                goal_after_30 = 1 if first_goal_minute <= signal_minute + 30 else 0
                goal_after_45 = 1 if first_goal_minute <= signal_minute + 45 else 0
                goal_to_75 = 1 if first_goal_minute <= 75 else 0
                first_goal_str = str(first_goal_minute)
                time_to_goal_str = str(first_goal_minute - signal_minute)
                next_goal_team = first_goal_team or ""
            else:
                goal_to_ft = 0
                goal_after_15 = 0
                goal_after_30 = 0
                goal_after_45 = 0
                goal_to_75 = 0
                first_goal_str = ""
                time_to_goal_str = ""
                next_goal_team = ""

            labels_finalized_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

            logger.info(
                f"[GSHEETS_STATS] match={match_id} signal_minute={signal_minute} "
                f"goals={goal_minutes} first_goal={first_goal_minute} "
                f"after15={goal_after_15} after30={goal_after_30} after45={goal_after_45} to75={goal_to_75} to_ft={goal_to_ft} "
                f"time_to_goal={time_to_goal_str} next_team={next_goal_team}"
            )

            updates = {}
            if col_goal_after_15 is not None:
                updates[a1_notation(col_goal_after_15 + 1, row_number)] = str(goal_after_15)
            if col_goal_after_30 is not None:
                updates[a1_notation(col_goal_after_30 + 1, row_number)] = str(goal_after_30)
            if col_goal_after_45 is not None:
                updates[a1_notation(col_goal_after_45 + 1, row_number)] = str(goal_after_45)
            if col_goal_to_75 is not None:
                updates[a1_notation(col_goal_to_75 + 1, row_number)] = str(goal_to_75)
            if col_goal_to_ft is not None:
                updates[a1_notation(col_goal_to_ft + 1, row_number)] = str(goal_to_ft)
            if col_first_goal is not None:
                updates[a1_notation(col_first_goal + 1, row_number)] = first_goal_str
            if col_time_to_goal is not None:
                updates[a1_notation(col_time_to_goal + 1, row_number)] = time_to_goal_str
            if col_next_goal_team is not None:
                updates[a1_notation(col_next_goal_team + 1, row_number)] = next_goal_team
            if col_labels_finalized is not None:
                updates[a1_notation(col_labels_finalized + 1, row_number)] = labels_finalized_ts

            if updates:
                with _gsheets_lock:
                    for cell_range, value in updates.items():
                        try:
                            _gsheets_sheet.update(range_name=cell_range, values=[[value]], value_input_option="RAW")
                        except Exception as e:
                            logger.error(f"[GSHEETS] Failed to update {cell_range}: {e}")

            logger.info(
                f"[GSHEETS_ROW_WRITE] match={match_id} row={row_number} signal_min={signal_minute} first_goal={first_goal_minute} "
                f"goal_to_ft={goal_to_ft} after15={goal_after_15} after30={goal_after_30} after45={goal_after_45} to75={goal_to_75}"
            )
        
    except Exception as e:
        logger.exception(f"[GSHEETS] finalize_match_outcomes failed for match {match_id}: {e}")

def process_match_outcomes_for_jsonl(match_id: int, client):
    """
    Process outcomes for all signals of a finished match and save to JSONL.
    
    Reads snapshots from match_snapshots.jsonl and creates outcomes in match_outcomes.jsonl.
    """
    try:
        # Load existing outcome keys to avoid duplicates
        existing_keys = load_existing_outcome_keys()
        
        # Load snapshots for this match
        snapshots_file = os.path.join("zzz.json", "match_snapshots.jsonl")
        if not os.path.exists(snapshots_file):
            logger.debug(f"[OUTCOME_JSONL] No snapshots file for match {match_id}")
            return
        
        match_snapshots = []
        with open(snapshots_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        snapshot = json.loads(line)
                        if snapshot.get("match_id") == match_id:
                            match_snapshots.append(snapshot)
                    except json.JSONDecodeError:
                        continue
        
        if not match_snapshots:
            logger.debug(f"[OUTCOME_JSONL] No snapshots found for match {match_id}")
            return
        
        # Fetch match events
        try:
            events = client.fetch_fixture_events(match_id)
            if not events:
                events = []
        except Exception as e:
            logger.error(f"[OUTCOME_JSONL] Failed to fetch events for match {match_id}: {e}")
            events = []
        
        # Process each snapshot
        for snapshot in match_snapshots:
            signal_ts_utc = snapshot.get("signal_ts_utc")
            minute_at_signal = snapshot.get("minute")
            
            if signal_ts_utc is None or minute_at_signal is None:
                logger.warning(f"[OUTCOME_JSONL] Invalid snapshot for match {match_id}: missing ts or minute")
                continue
            
            # Check if outcome already exists
            outcome_key = (match_id, signal_ts_utc)
            if outcome_key in existing_keys:
                logger.debug(f"[OUTCOME_JSONL] Outcome already exists for match {match_id} at {signal_ts_utc}")
                continue
            
            # Extract goals after signal
            goal_minutes, first_goal_minute, first_goal_team = _extract_counted_goals_after_signal(events, minute_at_signal, match_id)
            
            # Determine next goal team
            next_goal_team = first_goal_team or "none"
            
            # Calculate flags
            if first_goal_minute is not None:
                time_to_next_goal_min = first_goal_minute - minute_at_signal
                goal_after_15 = time_to_next_goal_min <= 15
                goal_after_30 = time_to_next_goal_min <= 30
                goal_after_45 = time_to_next_goal_min <= 45
                goal_to_75 = first_goal_minute <= 75
                goal_to_ft = True
            else:
                time_to_next_goal_min = None
                goal_after_15 = False
                goal_after_30 = False
                goal_after_45 = False
                goal_to_75 = False
                goal_to_ft = False
            
            # Create outcome
            outcome = {
                "match_id": match_id,
                "signal_ts_utc": signal_ts_utc,
                "minute_at_signal": minute_at_signal,
                "goal_after_15": goal_after_15,
                "goal_after_30": goal_after_30,
                "goal_after_45": goal_after_45,
                "goal_to_75": goal_to_75,
                "goal_to_ft": goal_to_ft,
                "first_goal_minute": first_goal_minute,
                "time_to_next_goal_min": time_to_next_goal_min,
                "next_goal_team": next_goal_team,
                "labels_finalized_ts_utc": datetime.utcnow().isoformat()
            }
            
            # Save outcome
            save_outcome(outcome)
            logger.info(f"[OUTCOME_JSONL] Saved outcome for match {match_id} signal at {signal_ts_utc}: first_goal={first_goal_minute}, team={next_goal_team}")
            
    except Exception as e:
        logger.exception(f"[OUTCOME_JSONL] Failed to process outcomes for match {match_id}: {e}")

def label_match_rows(match_id: int, goals_minutes: List[int]):
    """
    Label goal_next_15 column for all rows of a finished match in Google Sheets.
    
    For each row with this match_id:
    - If a goal occurred within (minute, minute+15], set goal_next_15 = 1
    - Otherwise set goal_next_15 = 0
    
    Args:
        match_id: Match ID to label
        goals_minutes: List of all goal minutes in the match
    """
    global _gsheets_sheet
    
    if not GSHEETS_AVAILABLE or _gsheets_sheet is None:
        logger.warning("[GSHEETS] Cannot label match rows - Google Sheets not available")
        return
    
    try:
        with _gsheets_lock:
            headers = ensure_gsheets_headers(_gsheets_sheet)
            header_map = {name: idx for idx, name in enumerate(headers) if name}
            
            required_cols = ["ID матча", "Минута сигнала", "Гол после 15 минут"]
            missing_cols = [col for col in required_cols if col not in header_map]
            if missing_cols:
                logger.error(f"[GSHEETS] Required columns missing: {missing_cols}")
                return
            
            # Get all data from sheet
            all_values = _gsheets_sheet.get_all_values()
            if not all_values or len(all_values) < 2:
                logger.warning(f"[GSHEETS] No data to label for match {match_id}")
                return
            
            match_id_col = header_map["ID матча"]
            minute_col = header_map["Минута сигнала"]
            goal_next_15_col = header_map["Гол после 15 минут"]
            
            # Find all rows for this match_id that need labeling
            rows_to_update = []
            for row_idx, row in enumerate(all_values[1:], start=2):  # start=2 because row 1 is header
                # Check if this row is for our match_id
                if len(row) <= match_id_col or str(row[match_id_col]) != str(match_id):
                    continue
                
                # Check if already labeled (not N/A)
                if len(row) > goal_next_15_col and row[goal_next_15_col] not in ["", "N/A"]:
                    continue
                
                # Get minute from row
                if len(row) <= minute_col:
                    continue
                
                try:
                    row_minute = int(row[minute_col])
                except (ValueError, TypeError):
                    logger.warning(f"[GSHEETS] Invalid minute value in row {row_idx}: {row[minute_col]}")
                    continue
                
                # Check if any goal occurred within (row_minute, row_minute+15]
                has_goal_next_15 = any(
                    row_minute < goal_min <= row_minute + 15
                    for goal_min in goals_minutes
                )
                
                label_value = 1 if has_goal_next_15 else 0
                rows_to_update.append((row_idx, goal_next_15_col + 1, label_value))
            
            if not rows_to_update:
                logger.info(f"[GSHEETS] No rows to label for match {match_id}")
                return
            
            # Batch update all rows
            updates = []
            for row_idx, col_idx, value in rows_to_update:
                # Create cell address (e.g., "AB2") without sheet name
                # Since we're using _gsheets_sheet.batch_update(), we don't need sheet name prefix
                cell_address = a1_notation(col_idx, row_idx)
                updates.append({"range": cell_address, "values": [[value]]})
            
            if updates:
                logger.info(f"[GSHEETS] Batch updating {len(updates)} cells for match {match_id}")
                logger.debug(f"[GSHEETS] Sample ranges: {[u['range'] for u in updates[:3]]}")
                _gsheets_sheet.batch_update(updates)
                logger.info(f"[GSHEETS] Labeled {len(updates)} rows for match {match_id}")
            
    except Exception as e:
        logger.exception(f"[GSHEETS] Failed to label match rows for match {match_id}: {e}")

# -------------------------
# Goal validation helper (used by labeler and monitoring)
# -------------------------
def _extract_event_minute(ev: Dict[str, Any]) -> Optional[int]:
    """
    Extract event minute from API-Sports event data.
    
    Handles both regular time and extra time correctly:
    - Regular time: ev["time"]["elapsed"]
    - Extra time: ev["time"]["elapsed"] + ev["time"]["extra"]
    
    Examples:
    - 45th minute: {"time": {"elapsed": 45}} -> 45
    - 90+4 minute: {"time": {"elapsed": 90, "extra": 4}} -> 94
    
    Args:
        ev: Event dictionary from API-Sports
        
    Returns:
        Event minute as integer, or None if cannot be determined
    """
    try:
        time_data = ev.get("time", {})
        
        # Try multiple fields for elapsed time
        elapsed = time_data.get("elapsed")
        if elapsed is None:
            elapsed = time_data.get("value")
        if elapsed is None:
            elapsed = ev.get("minute")  # Fallback for normalized events
        
        if elapsed is None:
            return None
        
        try:
            elapsed = int(elapsed)
        except (ValueError, TypeError):
            return None
        
        # Check for extra time
        extra = time_data.get("extra")
        if extra is not None:
            try:
                extra = int(extra)
                return elapsed + extra
            except (ValueError, TypeError):
                pass
        
        return elapsed
        
    except Exception:
        return None

def _is_valid_counted_goal(ev: Dict[str, Any]) -> bool:
    """
    Check if event is a valid counted goal (not cancelled by VAR/offside).
    
    A goal is counted ONLY if:
    - event.type is "Goal" or contains "goal" (case-insensitive)
    - event detail does NOT contain cancellation keywords
    
    Args:
        ev: Event dictionary with normalized fields
        
    Returns:
        True if goal should be counted in statistics, False otherwise
    """
    typ = ev.get("type", "")
    typ_lower = typ.lower()
    
    # Must be a goal event
    # Accept: "Goal", "goal", "own_goal", "Own Goal", etc.
    if "goal" not in typ_lower:
        return False
    
    # Collect all detail/comment fields for checking
    detail_fields = [
        ev.get("detail", ""),
        ev.get("details", ""),
        ev.get("comment", ""),
        ev.get("comments", ""),
        ev.get("note", ""),
    ]
    
    # Also check raw data if available
    raw = ev.get("raw") or {}
    if raw:
        detail_fields.append(str(raw.get("detail", "")))
        detail_fields.append(str(raw.get("comment", "")))
        detail_fields.append(str(raw.get("comments", "")))
    
    # Combine all detail fields
    combined_detail = " ".join(str(d) for d in detail_fields).lower()
    
    # Exclude cancelled goals
    cancellation_keywords = [
        "cancelled", "canceled", "no goal", "var", "offside", 
        "disallowed", "goal disallowed", "annulé", "ruled out", "annul"
    ]
    for keyword in cancellation_keywords:
        if keyword in combined_detail:
            return False
    
    return True

# -------------------------
# Google Sheets goal_next_15 labeler (backfill daemon)
# -------------------------
def get_sheet_header_map(sheet) -> Dict[str, int]:
    """
    Get mapping of column names to column indices (1-based).
    
    Returns:
        Dictionary mapping column name to column index
    """
    try:
        headers = _normalize_headers(sheet.row_values(1))
        return {name: idx for idx, name in enumerate(headers) if name}
    except Exception as e:
        logger.warning(f"[GSHEETS_LABELER] Failed to get header map: {e}")
        return {}

def fetch_counted_goal_minutes(match_id: int, client: APISportsMetricsClient) -> List[int]:
    """
    Fetch all counted goal minutes for a match from API-Football.
    Only includes goals that were NOT cancelled/disallowed.
    
    Args:
        match_id: Match ID
        client: API client instance
        
    Returns:
        Sorted list of goal minutes (counted goals only)
    """
    try:
        # Fetch events using existing client
        events = client.fetch_fixture_events(match_id)
        if not events:
            return []
        
        # Normalize if needed (events might already be normalized)
        if events and not isinstance(events[0].get("type"), str):
            events = client._normalize_events(events)
        
        goal_minutes = []
        for ev in events:
            # Use existing validation function
            if _is_valid_counted_goal(ev):
                # Use new extraction function that handles extra time
                minute = _extract_event_minute(ev)
                if minute is not None:
                    goal_minutes.append(minute)
        
        return sorted(goal_minutes)
    
    except Exception as e:
        logger.warning(f"[GSHEETS_LABELER] Failed to fetch goals for match {match_id}: {e}")
        return []

def get_match_status_and_minute(match_id: int, client: APISportsMetricsClient) -> Tuple[str, int]:
    """
    Get current match status and minute.
    
    Returns:
        Tuple of (status_string, current_minute)
        Status can be: "FT", "Live", "1H", "2H", "HT", etc.
    """
    try:
        fixture = client.fetch_fixture(match_id)
        if not fixture:
            return "Unknown", 0
        
        fixture_info = fixture.get("fixture", {})
        status = fixture_info.get("status", {})
        
        if isinstance(status, dict):
            status_short = status.get("short", "")
            elapsed = status.get("elapsed", 0)
        else:
            status_short = str(status)
            elapsed = fixture_info.get("elapsed", 0)
        
        try:
            minute = int(elapsed) if elapsed else 0
        except (ValueError, TypeError):
            minute = 0
        
        return status_short, minute
    
    except Exception as e:
        logger.warning(f"[GSHEETS_LABELER] Failed to get status for match {match_id}: {e}")
        return "Unknown", 0

def compute_goal_next_15(row_minute: int, goal_minutes: List[int], match_status: str, live_minute: int) -> Optional[int]:
    """
    Compute goal_next_15 value for a row.
    
    Args:
        row_minute: Minute from the sheet row
        goal_minutes: List of all counted goal minutes in match
        match_status: Match status (FT, Live, etc.)
        live_minute: Current live minute of match
        
    Returns:
        1 if goal in next 15 min, 0 if no goal, None if window not closed yet
    """
    window_start = row_minute
    window_end = row_minute + 15
    
    # Check if match is finished
    is_finished = match_status in ("FT", "AET", "PEN", "CANC", "ABD", "AWD", "WO")
    
    # If match is finished OR live minute >= window_end, we can determine the value
    if is_finished or live_minute >= window_end:
        # Check if any goal occurred in (row_minute, row_minute + 15]
        has_goal = any(window_start < gm <= window_end for gm in goal_minutes)
        return 1 if has_goal else 0
    
    # Window not closed yet
    return None

def compute_goal_until_end(row_minute: int, goal_minutes: List[int], match_status: str) -> Optional[int]:
    """
    Compute goal_until_end value for a row.
    
    Args:
        row_minute: Minute from the sheet row
        goal_minutes: List of all counted goal minutes in match
        match_status: Match status (FT, Live, etc.)
        
    Returns:
        1 if at least one goal after row_minute until match end, 0 if no goal, None if match not finished yet
    """
    # Check if match is finished
    is_finished = match_status in ("FT", "AET", "PEN", "CANC", "ABD", "AWD", "WO")
    
    # Only determine value when match is finished
    if is_finished:
        # Check if any goal occurred after row_minute (strictly greater)
        has_goal = any(gm > row_minute for gm in goal_minutes)
        return 1 if has_goal else 0
    
    # Match not finished yet
    return None

def gsheets_labeler_daemon(client: APISportsMetricsClient, interval: int = 60):
    """
    Background daemon that periodically fills goal_next_15 and goal_until_end columns in Google Sheets.
    Processes rows with "N/A" or empty values and updates them when conditions are met.
    Uses header-based mapping to avoid column order issues.
    """
    global _gsheets_sheet
    
    logger.info(f"[GSHEETS_LABELER] Daemon started, interval={interval}s")
    
    while True:
        try:
            time.sleep(interval)
            
            if not GSHEETS_AVAILABLE or _gsheets_sheet is None:
                continue
            
            with _gsheets_lock:
                try:
                    # Get header mapping using ensure_required_headers
                    header_map = ensure_required_headers(_gsheets_sheet, REQUIRED_HEADERS)
                    if not header_map:
                        logger.warning("[GSHEETS_LABELER] No header_map available, skipping cycle")
                        continue
                    
                    # Check if required columns exist in header_map
                    required_cols = ["ID матча", "Минута сигнала", "Гол после 15 минут", "Гол до конца матча"]
                    missing_cols = [col for col in required_cols if col not in header_map]
                    if missing_cols:
                        logger.warning(f"[GSHEETS_LABELER] Missing required columns: {missing_cols}, skipping cycle")
                        continue
                    
                    # Get column indices from header_map (0-based)
                    match_id_col = header_map["ID матча"]
                    minute_col = header_map["Минута сигнала"]
                    goal_next_15_col = header_map["Гол после 15 минут"]
                    goal_until_end_col = header_map["Гол до конца матча"]
                    
                    # Get all rows
                    all_rows = _gsheets_sheet.get_all_values()
                    if len(all_rows) <= 1:  # Only headers or empty
                        continue
                    
                    # Find rows with N/A in either target column
                    rows_to_process = []
                    for row_idx, row in enumerate(all_rows[1:], start=2):  # Skip header, start at row 2
                        if row_idx - 2 >= len(all_rows) - 1:
                            break
                        
                        needs_processing = False
                        
                        # Check if goal_next_15 is N/A or empty
                        if goal_next_15_col is not None:
                            goal_next_15_value = row[goal_next_15_col] if len(row) > goal_next_15_col else ""
                            if goal_next_15_value.strip().upper() in ("N/A", ""):
                                needs_processing = True
                        
                        # Check if goal_until_end is N/A or empty
                        if goal_until_end_col is not None:
                            goal_until_end_value = row[goal_until_end_col] if len(row) > goal_until_end_col else ""
                            if goal_until_end_value.strip().upper() in ("N/A", ""):
                                needs_processing = True
                        
                        if needs_processing:
                            rows_to_process.append((row_idx, row))
                    
                    if not rows_to_process:
                        logger.debug("[GSHEETS_LABELER] No rows with N/A to process")
                        continue
                    
                    logger.info(f"[GSHEETS_LABELER] Found {len(rows_to_process)} rows with N/A")
                    
                    # Group by match_id
                    matches_to_check = {}
                    for row_idx, row in rows_to_process:
                        try:
                            mid = int(row[match_id_col]) if len(row) > match_id_col else None
                            minute = int(row[minute_col]) if len(row) > minute_col else None
                            
                            if mid and minute is not None:
                                if mid not in matches_to_check:
                                    matches_to_check[mid] = []
                                matches_to_check[mid].append((row_idx, minute, row))
                        except (ValueError, IndexError):
                            continue
                    
                    # Process each match
                    updates = []  # List of (row, col, value) tuples for batch update
                    next_15_count = 0
                    until_end_count = 0
                    
                    for match_id, row_data in matches_to_check.items():
                        try:
                            # Fetch match data
                            status, live_minute = get_match_status_and_minute(match_id, client)
                            goal_minutes = fetch_counted_goal_minutes(match_id, client)
                            
                            logger.debug(f"[GSHEETS_LABELER] Match {match_id}: status={status}, live={live_minute}, goals={goal_minutes}")
                            
                            # Process each row for this match
                            for row_idx, row_minute, row in row_data:
                                # Process goal_next_15
                                if goal_next_15_col is not None:
                                    goal_next_15_value = row[goal_next_15_col] if len(row) > goal_next_15_col else ""
                                    if goal_next_15_value.strip().upper() in ("N/A", ""):
                                        value = compute_goal_next_15(row_minute, goal_minutes, status, live_minute)
                                        if value is not None:
                                            updates.append((row_idx, goal_next_15_col + 1, value))
                                            next_15_count += 1
                                
                                # Process goal_until_end
                                if goal_until_end_col is not None:
                                    goal_until_end_value = row[goal_until_end_col] if len(row) > goal_until_end_col else ""
                                    if goal_until_end_value.strip().upper() in ("N/A", ""):
                                        value = compute_goal_until_end(row_minute, goal_minutes, status)
                                        if value is not None:
                                            updates.append((row_idx, goal_until_end_col + 1, value))
                                            until_end_count += 1
                        
                        except Exception as e:
                            logger.warning(f"[GSHEETS_LABELER] Error processing match {match_id}: {e}")
                            continue
                    
                    # Apply updates in batch
                    if updates:
                        try:
                            batch_data = []
                            for row_idx, col_idx, value in updates:
                                # Create cell address (e.g., "AB2") without sheet name
                                # Since we're using _gsheets_sheet.batch_update(), we don't need sheet name prefix
                                cell_address = a1_notation(col_idx, row_idx)
                                # Normalize value to Google Sheets safe scalar
                                safe_value = gsheets_scalar(value)
                                batch_data.append({
                                    'range': cell_address,
                                    'values': [[safe_value]]
                                })
                            
                            if batch_data:
                                # Log first few ranges for debugging
                                sample_ranges = [item['range'] for item in batch_data[:3]]
                                logger.info(f"[GSHEETS_LABELER] Updating {len(batch_data)} cells. Sample ranges: {sample_ranges}")
                                _gsheets_sheet.batch_update(batch_data)
                                logger.info(f"[GSHEETS_LABELER] Labeler updated {len(updates)} cells: goal_next_15={next_15_count}, goal_until_end={until_end_count}")
                        
                        except Exception as e:
                            logger.warning(f"[GSHEETS_LABELER] Batch update failed: {e}")
                
                except Exception as e:
                    logger.warning(f"[GSHEETS_LABELER] Error in labeler cycle: {e}")
        
        except Exception as e:
            logger.exception(f"[GSHEETS_LABELER] Daemon error: {e}")

_gsheets_labeler_thread: Optional[threading.Thread] = None

def start_gsheets_labeler_daemon(client: APISportsMetricsClient):
    """Start the Google Sheets labeler daemon if sheets are available."""
    global _gsheets_labeler_thread
    
    if not GSHEETS_AVAILABLE or _gsheets_sheet is None:
        logger.info("[GSHEETS_LABELER] Daemon not started (Google Sheets unavailable)")
        return
    
    if _gsheets_labeler_thread and _gsheets_labeler_thread.is_alive():
        return
    
    t = threading.Thread(target=gsheets_labeler_daemon, args=(client,), daemon=True)
    _gsheets_labeler_thread = t
    t.start()
    logger.info("[GSHEETS_LABELER] Daemon thread started")

# -------------------------
# Daily cleanup functions
# -------------------------
MAINTENANCE_LOOP_SECONDS = 60
MAINTENANCE_PENDING_CHECK_SECONDS = 5 * 60
MAINTENANCE_TARGET_HOUR = 4
MAINTENANCE_TARGET_MINUTE = 0
MAINTENANCE_MAX_RUNTIME_SECONDS = 60
MAINTENANCE_PROGRESS_LOG_SECONDS = 5
MAINTENANCE_STEP_TIMEOUT_SECONDS = 20
MAINTENANCE_WATCHDOG_SECONDS = 10 * 60
LIVE_STATUS_SHORTS = {"1H", "2H", "ET", "LIVE", "HT"}

_maintenance_runtime_lock = threading.RLock()
cleanup_requested = False
cleanup_in_progress = False
cleanup_requested_at: Optional[float] = None


def _is_live_fixture_active(fixture: Dict[str, Any]) -> bool:
    try:
        fx = fixture.get("fixture") if isinstance(fixture, dict) else None
        if not isinstance(fx, dict):
            fx = fixture if isinstance(fixture, dict) else {}

        status_obj = fx.get("status") or fixture.get("status") or {}
        status_short = ""
        elapsed = 0

        if isinstance(status_obj, dict):
            status_short = str(status_obj.get("short") or status_obj.get("value") or "").upper()
            elapsed_raw = status_obj.get("elapsed")
            try:
                elapsed = int(float(elapsed_raw)) if elapsed_raw is not None else 0
            except Exception:
                elapsed = 0
        else:
            status_short = str(status_obj or "").upper()

        if status_short not in LIVE_STATUS_SHORTS:
            return False

        return elapsed < 130
    except Exception:
        return False


def _get_maintenance_state() -> Dict[str, Any]:
    with persistent_state_lock:
        maintenance = persistent_state.setdefault("maintenance", {})
        if not isinstance(maintenance, dict):
            maintenance = {}
            persistent_state["maintenance"] = maintenance
        maintenance.setdefault("last_cleanup_date_msk", None)
        maintenance.setdefault("pending_cleanup", False)
        maintenance.setdefault("cleanup_requested", False)
        maintenance.setdefault("cleanup_requested_at", None)
        maintenance.setdefault("cleanup_in_progress", False)
        return dict(maintenance)


def _set_pending_cleanup(value: bool) -> None:
    with persistent_state_lock:
        maintenance = persistent_state.setdefault("maintenance", {})
        if not isinstance(maintenance, dict):
            maintenance = {}
            persistent_state["maintenance"] = maintenance
        maintenance["pending_cleanup"] = bool(value)
    save_persistent_state()


def _set_cleanup_runtime_flags(
    requested: Optional[bool] = None,
    in_progress: Optional[bool] = None,
    requested_at_ts: Optional[float] = None,
) -> None:
    global cleanup_requested, cleanup_in_progress, cleanup_requested_at
    with _maintenance_runtime_lock:
        if requested is not None:
            cleanup_requested = bool(requested)
        if in_progress is not None:
            cleanup_in_progress = bool(in_progress)
        if requested_at_ts is not None:
            cleanup_requested_at = float(requested_at_ts)
        elif requested is False:
            cleanup_requested_at = None


def _persist_cleanup_runtime_flags() -> None:
    with _maintenance_runtime_lock:
        req = bool(cleanup_requested)
        inp = bool(cleanup_in_progress)
        req_at = cleanup_requested_at

    with persistent_state_lock:
        maintenance = persistent_state.setdefault("maintenance", {})
        if not isinstance(maintenance, dict):
            maintenance = {}
            persistent_state["maintenance"] = maintenance
        maintenance["cleanup_requested"] = req
        maintenance["cleanup_in_progress"] = inp
        maintenance["cleanup_requested_at"] = req_at
        maintenance["pending_cleanup"] = req
    save_persistent_state()


def _request_cleanup(reason: str, now_ts: Optional[float] = None) -> None:
    now_val = float(now_ts if now_ts is not None else time.time())
    with _maintenance_runtime_lock:
        already_requested = bool(cleanup_requested)
    if not already_requested:
        logger.info(f"[MAINT] cleanup requested: {reason}")
    _set_cleanup_runtime_flags(requested=True, requested_at_ts=now_val)
    _persist_cleanup_runtime_flags()


def _clear_cleanup_request(reason: str) -> None:
    logger.info(f"[MAINT] cleanup request cleared: {reason}")
    _set_cleanup_runtime_flags(requested=False, in_progress=False)
    _persist_cleanup_runtime_flags()


def _set_cleanup_date_today_and_clear_pending() -> None:
    today_msk = now_msk().strftime("%Y-%m-%d")
    with persistent_state_lock:
        maintenance = persistent_state.setdefault("maintenance", {})
        if not isinstance(maintenance, dict):
            maintenance = {}
            persistent_state["maintenance"] = maintenance
        maintenance["last_cleanup_date_msk"] = today_msk
        maintenance["pending_cleanup"] = False
        maintenance["cleanup_requested"] = False
        maintenance["cleanup_requested_at"] = None
        maintenance["cleanup_in_progress"] = False
    save_persistent_state()


def _count_active_matches() -> int:
    with state_lock:
        active_ids: Set[int] = set()

        for match_id in state.get("monitored_matches", []):
            try:
                active_ids.add(int(match_id))
            except Exception:
                continue

        for match_id, info in (state.get("match_signal_info", {}) or {}).items():
            try:
                if not bool((info or {}).get("is_finished", False)):
                    active_ids.add(int(match_id))
            except Exception:
                continue

        for fixture_id in ACTIVE_FIXTURES:
            try:
                active_ids.add(int(fixture_id))
            except Exception:
                continue

        return len(active_ids)


def has_active_matches() -> bool:
    return _count_active_matches() > 0


def ensure_daily_stats_sent_for_prev_day() -> bool:
    prev_day_str = (now_msk().date() - timedelta(days=1)).strftime("%Y-%m-%d")

    with persistent_state_lock:
        stats_state = persistent_state.setdefault("stats", {})
        if not isinstance(stats_state, dict):
            stats_state = {}
            persistent_state["stats"] = stats_state
        last_stats_date = stats_state.get("last_daily_stats_date_msk")
        if last_stats_date == prev_day_str:
            return True

    try:
        check_and_send_daily_stats_if_ready(prev_day_str)
    except Exception:
        logger.exception(f"[MAINT] Failed to ensure daily stats for {prev_day_str}")
        return False

    with persistent_state_lock:
        stats_state = persistent_state.setdefault("stats", {})
        if not isinstance(stats_state, dict):
            return False
        return stats_state.get("last_daily_stats_date_msk") == prev_day_str


def _run_cleanup_step_with_timeout(step_name: str, fn, timeout_seconds: float, cleanup_started_at: float, total_budget_seconds: float) -> bool:
    result: Dict[str, Any] = {"done": False, "ok": False, "error": None}

    def _runner() -> None:
        try:
            out = fn()
            result["ok"] = bool(True if out is None else out)
        except Exception as e:
            result["error"] = e
        finally:
            result["done"] = True

    logger.info(f"[MAINT] {step_name} start")
    thread = threading.Thread(target=_runner, daemon=True)
    thread.start()

    step_started_at = time.time()
    next_progress_log_at = step_started_at + MAINTENANCE_PROGRESS_LOG_SECONDS

    while not result.get("done", False):
        now_ts = time.time()
        total_elapsed = now_ts - cleanup_started_at
        step_elapsed = now_ts - step_started_at

        if total_elapsed > total_budget_seconds:
            logger.warning(
                "[MAINT] %s timeout by total budget: elapsed=%.1fs budget=%.1fs",
                step_name,
                total_elapsed,
                total_budget_seconds,
            )
            return False

        if step_elapsed > timeout_seconds:
            logger.warning(
                "[MAINT] %s timeout: elapsed=%.1fs step_timeout=%.1fs",
                step_name,
                step_elapsed,
                timeout_seconds,
            )
            return False

        if now_ts >= next_progress_log_at:
            logger.info(
                "[MAINT] %s in progress: step_elapsed=%.1fs total_elapsed=%.1fs",
                step_name,
                step_elapsed,
                total_elapsed,
            )
            next_progress_log_at = now_ts + MAINTENANCE_PROGRESS_LOG_SECONDS

        thread.join(timeout=1.0)

    if result.get("error") is not None:
        logger.exception("[MAINT] %s failed", step_name, exc_info=result.get("error"))
        return False

    if not result.get("ok", False):
        logger.warning(f"[MAINT] {step_name} returned unsuccessful result")
        return False

    logger.info(f"[MAINT] {step_name} end")
    return True


def _rotate_logs_safe() -> bool:
    log_file = "goal_predictor.log"
    logger_obj = logging.getLogger("mat")
    for handler in list(logger_obj.handlers):
        try:
            handler.flush()
        except Exception:
            continue
    try:
        with open(log_file, "a", encoding="utf-8"):
            pass
        with open(log_file, "r+", encoding="utf-8") as fh:
            fh.truncate(0)
        return True
    except Exception:
        logger.exception("[MAINT] Failed to rotate logs safely")
        return False


def _state_cleanup_safe() -> bool:
    try:
        save_json_atomic(TEMP_STATE_PATH, {})
        return True
    except Exception:
        logger.exception("[MAINT] Failed to reset bot_state.json")
        return False


def run_cleanup_with_time_limit(max_seconds: int = MAINTENANCE_MAX_RUNTIME_SECONDS, safe_mode: bool = False) -> bool:
    budget = max(10.0, float(max_seconds or MAINTENANCE_MAX_RUNTIME_SECONDS))
    cleanup_started_at = time.time()

    logger.info("[MAINT] start cleanup (safe_mode=%s, budget=%ss)", safe_mode, int(budget))
    active_count = _count_active_matches()
    logger.info("[MAINT] active_matches count=%s", active_count)

    if active_count > 0 and not safe_mode:
        logger.info("[MAINT] cleanup deferred: active_live_matches=%s", active_count)
        return False

    if not safe_mode:
        ok_stats = _run_cleanup_step_with_timeout(
            "ensure daily stats before cleanup",
            ensure_daily_stats_sent_for_prev_day,
            timeout_seconds=min(MAINTENANCE_STEP_TIMEOUT_SECONDS, budget),
            cleanup_started_at=cleanup_started_at,
            total_budget_seconds=budget,
        )
        if not ok_stats:
            logger.warning("[MAINT] cleanup aborted: daily stats step failed or timed out")
            return False
    else:
        logger.warning("[MAINT] safe cleanup mode: skipping daily stats pre-check")

    ok_rotate = _run_cleanup_step_with_timeout(
        "rotate logs",
        _rotate_logs_safe,
        timeout_seconds=min(MAINTENANCE_STEP_TIMEOUT_SECONDS, budget),
        cleanup_started_at=cleanup_started_at,
        total_budget_seconds=budget,
    )
    if not ok_rotate:
        logger.warning("[MAINT] cleanup aborted: rotate logs step failed or timed out")
        return False

    ok_state = _run_cleanup_step_with_timeout(
        "state cleanup",
        _state_cleanup_safe,
        timeout_seconds=min(MAINTENANCE_STEP_TIMEOUT_SECONDS, budget),
        cleanup_started_at=cleanup_started_at,
        total_budget_seconds=budget,
    )
    if not ok_state:
        logger.warning("[MAINT] cleanup aborted: state cleanup step failed or timed out")
        return False

    _set_cleanup_date_today_and_clear_pending()
    logger.info("[MAINT] done cleanup in %.1fs", time.time() - cleanup_started_at)
    return True


def cleanup_now() -> bool:
    return run_cleanup_with_time_limit(max_seconds=MAINTENANCE_MAX_RUNTIME_SECONDS, safe_mode=False)


def maintenance_daemon() -> None:
    logger.info("[MAINT] maintenance daemon started")
    next_pending_check_ts = 0.0

    try:
        maintenance_state = _get_maintenance_state()
        persisted_requested = bool(maintenance_state.get("cleanup_requested") or maintenance_state.get("pending_cleanup"))
        persisted_requested_at = maintenance_state.get("cleanup_requested_at")
        if persisted_requested:
            if persisted_requested_at is None:
                persisted_requested_at = time.time()
            _set_cleanup_runtime_flags(requested=True, in_progress=False, requested_at_ts=float(persisted_requested_at))
            _persist_cleanup_runtime_flags()
    except Exception:
        logger.exception("[MAINT] failed to restore cleanup runtime flags")

    while True:
        try:
            now_ts = time.time()
            now_dt = now_msk()
            today_msk = now_dt.strftime("%Y-%m-%d")
            target_reached = (now_dt.hour, now_dt.minute) >= (MAINTENANCE_TARGET_HOUR, MAINTENANCE_TARGET_MINUTE)

            maintenance_state = _get_maintenance_state()
            last_cleanup_date = maintenance_state.get("last_cleanup_date_msk")

            if target_reached and last_cleanup_date != today_msk:
                _request_cleanup("scheduled cleanup window reached", now_ts)

            if maintenance_state.get("pending_cleanup", False):
                with _maintenance_runtime_lock:
                    requested_now = bool(cleanup_requested)
                if not requested_now:
                    _request_cleanup("pending cleanup restored from persistent state", now_ts)

            with _maintenance_runtime_lock:
                req = bool(cleanup_requested)
                in_prog = bool(cleanup_in_progress)
                req_at = cleanup_requested_at

            if req and (not in_prog) and req_at and (now_ts - float(req_at)) > MAINTENANCE_WATCHDOG_SECONDS:
                wait_sec = int(now_ts - float(req_at))
                logger.warning(
                    "[MAINT] cleanup watchdog triggered: requested_for=%ss (> %ss)",
                    wait_sec,
                    MAINTENANCE_WATCHDOG_SECONDS,
                )
                _set_cleanup_runtime_flags(in_progress=True)
                _persist_cleanup_runtime_flags()
                try:
                    ok = run_cleanup_with_time_limit(max_seconds=MAINTENANCE_MAX_RUNTIME_SECONDS, safe_mode=True)
                finally:
                    _set_cleanup_runtime_flags(in_progress=False)
                    _persist_cleanup_runtime_flags()

                if ok:
                    _clear_cleanup_request("watchdog safe cleanup completed")
                else:
                    logger.warning("[MAINT] watchdog safe cleanup failed, forcing cleanup request reset")
                    _clear_cleanup_request("watchdog forced reset")

            with _maintenance_runtime_lock:
                req = bool(cleanup_requested)
                in_prog = bool(cleanup_in_progress)

            if req and (not in_prog):
                if now_ts >= next_pending_check_ts:
                    next_pending_check_ts = now_ts + MAINTENANCE_PENDING_CHECK_SECONDS

                    active_count = _count_active_matches()
                    logger.info("[MAINT] cleanup check: active_matches=%s", active_count)
                    if active_count > 0:
                        logger.info("[MAINT] cleanup deferred: active_live_matches=%s", active_count)
                    else:
                        _set_cleanup_runtime_flags(in_progress=True)
                        _persist_cleanup_runtime_flags()
                        try:
                            ok = run_cleanup_with_time_limit(max_seconds=MAINTENANCE_MAX_RUNTIME_SECONDS, safe_mode=False)
                        finally:
                            _set_cleanup_runtime_flags(in_progress=False)
                            _persist_cleanup_runtime_flags()

                        if ok:
                            _clear_cleanup_request("cleanup completed")
                        else:
                            logger.warning("[MAINT] cleanup not completed, will retry later")

            time.sleep(MAINTENANCE_LOOP_SECONDS)
        except Exception:
            logger.exception("[MAINT] maintenance daemon error")
            time.sleep(MAINTENANCE_LOOP_SECONDS)


_daily_cleanup_thread: Optional[threading.Thread] = None


def start_daily_cleanup_daemon(client: APISportsMetricsClient):
    """Start maintenance daemon (daily smart cleanup)."""
    global _daily_cleanup_thread

    if _daily_cleanup_thread and _daily_cleanup_thread.is_alive():
        logger.info("[MAINT] daemon already running")
        return

    t = threading.Thread(target=maintenance_daemon, daemon=True)
    _daily_cleanup_thread = t
    t.start()
    logger.info("[MAINT] maintenance daemon thread started")

# -------------------------
# Match metrics calculation (PressureIndex & Momentum)
# -------------------------
def calculate_pressure_index(fixture: Dict[str, Any]) -> float:
    """
    Calculate PressureIndex based on match statistics.
    
    Formula:
    PressureIndex = (xG_home + xG_away) * 2
                  + (shots_on_target_home + shots_on_target_away) * 1.5
                  + (corners_home + corners_away) * 0.5
                  + ((possession_home + possession_away) / 10)
    
    Args:
        fixture: Match fixture data with statistics
        
    Returns:
        Rounded PressureIndex value (2 decimal places)
    """
    try:
        # Extract xG values
        xg_home = get_any_metric(fixture, ["expected_goals"], "home") or 0.0
        xg_away = get_any_metric(fixture, ["expected_goals"], "away") or 0.0
        
        # If xG is 0, estimate it
        if xg_home == 0:
            xg_home = estimate_xg_from_metrics_combined(fixture, "home")
        if xg_away == 0:
            xg_away = estimate_xg_from_metrics_combined(fixture, "away")
        
        # Extract shots on target
        shots_on_h = int(get_any_metric(fixture, ["shots_on_target"], "home") or 0)
        shots_on_a = int(get_any_metric(fixture, ["shots_on_target"], "away") or 0)
        
        # Extract corners
        corners_h = int(get_any_metric(fixture, ["corner_kicks", "corners"], "home") or 0)
        corners_a = int(get_any_metric(fixture, ["corner_kicks", "corners"], "away") or 0)
        
        # Extract possession
        possession_h = float(get_any_metric(fixture, ["ball_possession", "passes_%"], "home") or 0)
        possession_a = float(get_any_metric(fixture, ["ball_possession", "passes_%"], "away") or 0)
        
        # Calculate PressureIndex
        pressure_index = (
            (xg_home + xg_away) * 2.0 +
            (shots_on_h + shots_on_a) * 1.5 +
            (corners_h + corners_a) * 0.5 +
            ((possession_h + possession_a) / 10.0)
        )
        
        return round(pressure_index, 2)
        
    except Exception as e:
        logger.exception(f"[METRICS] Failed to calculate PressureIndex: {e}")
        return 0.0

def calculate_momentum(previous_pressure: float, current_pressure: float) -> float:
    """
    Calculate Momentum as the change in PressureIndex.
    
    Formula:
    Momentum = current_pressure - previous_pressure
    
    Args:
        previous_pressure: Previous PressureIndex value
        current_pressure: Current PressureIndex value
        
    Returns:
        Rounded Momentum value (2 decimal places)
    """
    try:
        momentum = current_pressure - previous_pressure
        return round(momentum, 2)
    except Exception as e:
        logger.exception(f"[METRICS] Failed to calculate Momentum: {e}")
        return 0.0

# -------------------------
# Simple TTL cache (in-memory)
# -------------------------
_cache: Dict[str, Tuple[float, Any]] = {}
_cache_lock = threading.RLock()

def cache_get(key: str, ttl: int) -> Optional[Any]:
    with _cache_lock:
        ent = _cache.get(key)
        if not ent:
            return None
        t, val = ent
        if time.time() - t > ttl:
            _cache.pop(key, None)
            return None
        return val

def cache_set(key: str, val: Any) -> None:
    with _cache_lock:
        _cache[key] = (time.time(), val)

# -------------------------
# HTTP helpers with retry/backoff
# -------------------------
def _http_get(url: str, headers: Dict[str,str], params: Optional[Dict] = None,
              timeout: int = 10, retries: int = 3, backoff: float = 1.0) -> Optional[requests.Response]:
    attempt = 0
    while attempt < retries:
        attempt += 1
        try:
            r = requests.get(url, headers=headers, params=params, timeout=timeout)
            if r.status_code == 200:
                return r
            if r.status_code in (429, 500, 502, 503, 504):
                wait = backoff * (2 ** (attempt - 1))
                logger.warning(f"[HTTP] transient {r.status_code} for {url} params={params}, attempt={attempt}, sleeping {wait}s")
                time.sleep(wait)
                continue
            logger.warning(f"[HTTP] request failed {r.status_code} for {url}: {r.text[:200]}")
            return r
        except requests.RequestException as ex:
            wait = backoff * (2 ** (attempt - 1))
            logger.warning(f"[HTTP] exception {ex} for {url}, sleeping {wait}s")
            time.sleep(wait)
            continue
    logger.error(f"[HTTP] failed after {retries} attempts: {url}")
    return None

# -------------------------
# APISportsMetricsClient with robust normalization
# (unchanged code)...
# Due to message length the rest of the APISportsMetricsClient is unchanged and identical
# to the previous version. For brevity here we include it inline exactly as before.
# -------------------------
class APISportsMetricsClient:
    def __init__(self, api_key: str, host: str = "v3.football.api-sports.io", cache_ttl: int = 300):
        if not api_key:
            logger.warning("API_FOOTBALL_KEY not provided; API calls will fail")
        self.api_key = api_key
        self.host = host
        self.base = f"https://{self.host}"
        self.headers = {"x-apisports-key": self.api_key, "Accept": "application/json"} if api_key else {}
        self.cache_ttl = cache_ttl

    def _get(self, path: str, params: Optional[Dict]=None,
             cache: bool=False, cache_key: Optional[str]=None, ttl: Optional[int]=None) -> Dict[str,Any]:
        key = cache_key or f"{path}:{json.dumps(params or {}, sort_keys=True)}"
        if cache:
            val = cache_get(key, ttl or self.cache_ttl)
            if val is not None:
                return val
        url = f"{self.base}/{path}"
        resp = _http_get(url, headers=self.headers, params=params, retries=API_RETRY_COUNT, backoff=API_BACKOFF_FACTOR)
        if resp is None:
            return {}
        try:
            data = resp.json()
        except Exception:
            logger.debug(f"[API] Non-JSON response from {url}")
            data = {}
        if cache:
            cache_set(key, data)
        return data

    def fetch_fixture(self, fixture_id: int) -> Dict[str,Any]:
        data = self._get("fixtures", params={"id": fixture_id}, cache=False)
        return (data.get("response") or [{}])[0] if data.get("response") else {}

    def fetch_fixture_statistics(self, fixture_id: int) -> List[Dict[str,Any]]:
        data = self._get("fixtures/statistics", params={"fixture": fixture_id}, cache=False)
        return data.get("response") or []

    def fetch_fixture_events(self, fixture_id: int) -> List[Dict[str,Any]]:
        data = self._get("fixtures/events", params={"fixture": fixture_id}, cache=False)
        if data.get("response"):
            return data.get("response")
        f = self.fetch_fixture(fixture_id)
        events = f.get("events") or f.get("fixture",{}).get("events") or []
        return events

    def fetch_players(self, fixture_id: int) -> List[Dict[str,Any]]:
        data = self._get("players", params={"fixture": fixture_id}, cache=False)
        return data.get("response") or []

    def fetch_shotmap(self, fixture_id: int) -> List[Dict[str,Any]]:
        data = self._get("fixtures/shots", params={"fixture": fixture_id}, cache=False)
        return data.get("response") or []

    def fetch_odds(self, fixture_id: int) -> List[Dict[str,Any]]:
        data = self._get("odds", params={"fixture": fixture_id}, cache=True, cache_key=f"odds:{fixture_id}", ttl=self.cache_ttl)
        return data.get("response") or []

    def fetch_lineups(self, fixture_id: int) -> List[Dict[str,Any]]:
        data = self._get("fixtures/lineups", params={"fixture": fixture_id}, cache=True, cache_key=f"lineups:{fixture_id}", ttl=self.cache_ttl)
        return data.get("response") or []

    def fetch_finished_league_fixtures(self, league_id: int, last: int = LEAGUE_REFRESH_SAMPLE_TARGET) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {
            "league": int(league_id),
            "status": "FT",
            "last": int(max(1, min(500, last or LEAGUE_REFRESH_SAMPLE_TARGET))),
        }
        data = self._get("fixtures", params=params, cache=False)
        return data.get("response") or []

    def fetch_cup_league_last_fixtures(self, league_id: int, last: int = 50) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {
            "league": int(league_id),
            "last": int(max(1, min(500, last or 50))),
        }
        data = self._get("fixtures", params=params, cache=False)
        return data.get("response") or []

    def fetch_league_fixtures_by_season(self, league_id: int, season: int, last: int = 200) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {
            "league": int(league_id),
            "season": int(season),
            "last": int(max(1, min(500, last or 200))),
        }
        data = self._get("fixtures", params=params, cache=False)
        return data.get("response") or []

    def fetch_league_standings(self, league_id: int, season: int) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {
            "league": int(league_id),
            "season": int(season),
        }
        data = self._get("standings", params=params, cache=False)
        return data.get("response") or []

    def fetch_team_last_fixtures(self, team_id: int, last: int = TEAM_STATS_SAMPLE_SIZE) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {
            "team": int(team_id),
            "last": int(max(1, min(100, last or TEAM_STATS_SAMPLE_SIZE))),
        }
        data = self._get("fixtures", params=params, cache=False)
        return data.get("response") or []

    def fetch_cup_last30_finished_fixtures(
        self,
        league_id: int,
        start_season: Optional[int] = None,
        target_matches: int = LEAGUE_CUP_SAMPLE_TARGET,
        max_seasons_back: int = LEAGUE_CUP_MAX_SEASONS_BACK,
    ) -> Tuple[List[Dict[str, Any]], List[int]]:
        return fetch_cup_last30_finished_fixtures(
            client=self,
            league_id=league_id,
            start_season=start_season,
            target_matches=target_matches,
            max_seasons_back=max_seasons_back,
        )

    def _to_num(self, v: Any) -> Any:
        if v is None:
            return 0
        try:
            if isinstance(v, (int,float)):
                return v
            s = str(v).strip()
            if s.endswith("%"):
                return float(s.replace("%"," ").strip())
            if "," in s and "." not in s:
                s = s.replace(",", ".")
            if "." in s:
                return float(s)
            return int(s)
        except Exception:
            try:
                return float(v)
            except Exception:
                return v

    def _wrap(self, v: Any, src: str="api_sports") -> Dict[str,Any]:
        return {"value": self._to_num(v), "source": src}

    def _map_stat_name(self, key: str) -> str:
        k = (key or "").lower()
        if "shots on target" in k or "shots_on_target" in k or "shots_on_goal" in k:
            return "shots_on_target"
        if "shots" in k and "total" in k:
            return "total_shots"
        if "inside" in k and ("box" in k or "penalty" in k or "pen area" in k or "penalty area" in k or "inside the box" in k):
            return "shots_inside_box"
        if "shots inside" in k or "insidebox" in k or "inside_box" in k:
            return "shots_inside_box"
        if "off target" in k or "shots_off_target" in k:
            return "shots_off_target"
        if "big chances" in k or "big_chances" in k:
            return "big_chances"
        if "final third" in k:
            return "final_third_entries"
        if "possession" in k or "passes_%" in k:
            return "ball_possession"
        if "corner" in k or "corners" in k:
            return "corner_kicks"
        if "expected" in k and "goal" in k:
            return "expected_goals"
        if "save" in k or "saves" in k or "saves_total" in k:
            return "saves"
        if "attack" in k or k == "attacks":
            return "attacks"
        if "dangerous" in k:
            return "dangerous_attacks"
        if "ppda" in k:
            return "ppda"
        return key.replace(" ", "_").replace(".", "").replace("/", "_")

    def _normalize_statistics(self, raw_stats: List[Dict[str,Any]], raw_fixture: Dict[str,Any]) -> Dict[str,Any]:
        out={}
        try:
            home_id = raw_fixture.get("teams",{}).get("home",{}).get("id")
            away_id = raw_fixture.get("teams",{}).get("away",{}).get("id")
            for entry in raw_stats:
                team = entry.get("team") or {}
                t_id = team.get("id")
                side = "home" if t_id == home_id else "away"
                stats_block = entry.get("statistics") or entry.get("statistics", []) or []
                for s in stats_block or []:
                    key_raw = (s.get("type") or s.get("name") or s.get("label") or "").strip()
                    if not key_raw:
                        continue
                    canon = self._map_stat_name(key_raw)
                    val = s.get("value")
                    if isinstance(val, dict):
                        if "total" in val:
                            vnum = val.get("total")
                        elif "value" in val:
                            vnum = val.get("value")
                        else:
                            vnum = val.get(side) if side in val else next(iter(val.values()), None)
                    else:
                        vnum = val
                    out_key = f"{canon}_{side}"
                    out[out_key] = self._wrap(vnum, "api_sports")
                    if canon == "expected_goals":
                        out[f"expected_goals_{side}"] = self._wrap(vnum, "api_sports")
            try:
                ff_stats = raw_fixture.get("statistics") or []
                if isinstance(ff_stats, list):
                    for entry in ff_stats:
                        team = entry.get("team") or {}
                        t_id = team.get("id")
                        side = "home" if t_id == home_id else "away"
                        for s in entry.get("statistics", []) or []:
                            key_raw = (s.get("type") or s.get("name") or "").strip()
                            if not key_raw:
                                continue
                            canon = self._map_stat_name(key_raw)
                            val = s.get("value")
                            out_key = f"{canon}_{side}"
                            if out_key not in out:
                                out[out_key] = self._wrap(val, "api_sports")
                teams_block = raw_fixture.get("teams") or {}
                for side_name in ("home","away"):
                    tblock = teams_block.get(side_name) or {}
                    stats = tblock.get("statistics") or tblock.get("stats") or []
                    for s in stats or []:
                        key_raw = (s.get("type") or s.get("name") or "").strip()
                        if not key_raw:
                            continue
                        canon = self._map_stat_name(key_raw)
                        val = s.get("value")
                        out_key = f"{canon}_{side_name}"
                        if out_key not in out:
                            out[out_key] = self._wrap(val, "api_sports")
            except Exception:
                logger.debug("secondary statistics scan failed")
        except Exception:
            logger.exception("_normalize_statistics")
        return out

    def _normalize_fixture_basic(self, raw: Dict[str,Any]) -> Dict[str,Any]:
        f={}
        try:
            fixture = raw.get("fixture") or raw
            fid = fixture.get("id") or raw.get("fixture",{}).get("id") or raw.get("id")
            f["fixture_id"] = self._wrap(fid)
            f["timestamp"] = self._wrap(fixture.get("timestamp") or raw.get("fixture",{}).get("timestamp"))
            status = fixture.get("status") or {}
            f["status"] = {"value": status, "source":"api_sports"}
            minute = status.get("elapsed") if isinstance(status, dict) else raw.get("elapsed") or 0
            f["elapsed"] = self._wrap(minute)
            f["elapsed_label"] = {"value": status.get("short") or status.get("long") or "", "source":"api_sports"}
            # Extract extra/added time if available
            extra_time = status.get("extra") if isinstance(status, dict) else 0
            f["extra_time"] = self._wrap(extra_time or 0)
            venue = fixture.get("venue") or raw.get("venue") or {}
            venue_value = {"name": venue.get("name"), "city": venue.get("city")}
            if venue.get("country"):
                venue_value["country"] = venue.get("country")
            else:
                league_block = raw.get("league") or fixture.get("league") or {}
                if league_block and league_block.get("country"):
                    venue_value["country"] = league_block.get("country")
            f["venue"] = {"value": venue_value, "source":"api_sports"}
            league = raw.get("league") or fixture.get("league") or {}
            f["league_id"] = self._wrap(league.get("id"))
            f["league_name"] = self._wrap(league.get("name"))
            if league.get("type"):
                f["league_type"] = self._wrap(league.get("type"))
            if league.get("season") is not None:
                f["league_season"] = self._wrap(league.get("season"))
            if league.get("country"):
                f["league_country"] = self._wrap(league.get("country"))
            goals = raw.get("goals") or fixture.get("goals") or {}
            f["score_home"] = self._wrap(goals.get("home"))
            f["score_away"] = self._wrap(goals.get("away"))
            teams_block = raw.get("teams") or fixture.get("teams") or {}
            t_home = teams_block.get("home", {}).get("name") or raw.get("teams", {}).get("home", {}).get("name") or fixture.get("teams", {}).get("home", {}).get("name")
            t_away = teams_block.get("away", {}).get("name") or raw.get("teams", {}).get("away", {}).get("name") or fixture.get("teams", {}).get("away", {}).get("name")
            t_home_id = teams_block.get("home", {}).get("id")
            t_away_id = teams_block.get("away", {}).get("id")
            if t_home: f["team_home_name"] = self._wrap(t_home)
            if t_away: f["team_away_name"] = self._wrap(t_away)
            if t_home_id is not None:
                f["team_home_id"] = self._wrap(t_home_id)
            if t_away_id is not None:
                f["team_away_id"] = self._wrap(t_away_id)
        except Exception:
            logger.exception("normalize_fixture_basic error")
        return f

    def _normalize_events(self, raw_events: List[Dict[str,Any]]) -> List[Dict[str,Any]]:
        out=[]
        try:
            if not raw_events:
                return []
            for ev in raw_events:
                e={}
                e["event_id"] = ev.get("id") or ev.get("event_id")
                time_obj = ev.get("time") or {}
                if isinstance(time_obj, dict):
                    e["minute"] = self._to_num(time_obj.get("elapsed"))
                    e["second"] = self._to_num(time_obj.get("extra"))
                else:
                    e["minute"] = self._to_num(ev.get("minute") or ev.get("time"))
                    e["second"] = None
                typ = (ev.get("type") or ev.get("detail") or ev.get("event") or "").lower()
                e["type"] = self._map_event_type(typ, ev)
                team = ev.get("team") or {}
                e["team_name"] = team.get("name") if isinstance(team, dict) else team
                player = ev.get("player") or {}
                e["player_id"] = player.get("id") if isinstance(player, dict) else None
                e["player_name"] = player.get("name") if isinstance(player, dict) else player
                assist = ev.get("assist") or {}
                e["assist_player_id"] = assist.get("id") if isinstance(assist, dict) else None
                e["assist_player_name"] = assist.get("name") if isinstance(assist, dict) else assist
                loc = ev.get("location") or {}
                e["x"] = self._to_num(loc.get("x") or ev.get("x"))
                e["y"] = self._to_num(loc.get("y") or ev.get("y"))
                shot = ev.get("shot") or {}
                e["shot_xg"] = self._to_num(shot.get("xG") or shot.get("xg") or shot.get("expected_goals"))
                e["note"] = ev.get("detail") or ev.get("comments") or ev.get("note") or ev.get("description") or ""
                e["raw"] = ev
                e["source"] = "api_sports"
                out.append(e)
        except Exception:
            logger.exception("_normalize_events")
        return out

    def _map_event_type(self, typ: str, ev: Dict[str,Any]) -> str:
        s = (typ or "").lower()
        if "own" in s and "goal" in s: return "own_goal"
        if "goal" in s and ("cancel" not in s and "disallow" not in s and "disallowed" not in s): return "goal"
        if "cancel" in s or "disallow" in s or "disallowed" in s:
            return "goal"
        if "pen" in s: return "penalty"
        if "yellow" in s: return "yellow_card"
        if "red" in s: return "red_card"
        if "sub" in s: return "substitution"
        if "offside" in s: return "offside"
        if "foul" in s: return "foul"
        if "shot" in s or "on target" in s:
            res = (ev.get("shot") or {}).get("result") if ev.get("shot") else None
            if res and "on" in str(res).lower(): return "shot_on_target"
            if res and "off" in str(res).lower(): return "shot_off_target"
            return "shot"
        return s or "unknown"

    def _normalize_shotmap(self, raw_shots: List[Dict[str,Any]]) -> List[Dict[str,Any]]:
        out=[]
        try:
            shots = raw_shots or []
            for s in shots:
                sh={}
                sh["x"] = self._to_num(s.get("x") or s.get("coordinateX") or (s.get("location") or {}).get("x"))
                sh["y"] = self._to_num(s.get("y") or s.get("coordinateY") or (s.get("location") or {}).get("y"))
                sh["player_id"] = (s.get("player") or {}).get("id") if isinstance(s.get("player"), dict) else s.get("player_id")
                sh["player_name"] = (s.get("player") or {}).get("name") if isinstance(s.get("player"), dict) else s.get("player")
                sh["distance"] = self._to_num(s.get("distance") or s.get("shot_distance"))
                sh["angle"] = self._to_num(s.get("angle"))
                sh["body_part"] = s.get("body_part") or s.get("bodyPart")
                sh["is_big_chance"] = bool(s.get("is_big_chance") or s.get("big_chance") or False)
                sh["is_blocked"] = bool(s.get("is_blocked") or False)
                sh["result"] = s.get("result") or s.get("outcome")
                sh["xg"] = self._to_num(s.get("xG") or s.get("xg") or s.get("expected_goals"))
                sh["source"] = "api_sports"
                out.append(sh)
        except Exception:
            logger.exception("_normalize_shotmap")
        return out

    def _normalize_players(self, raw_players: List[Dict[str,Any]]) -> Dict[str,Any]:
        out={}
        try:
            for team_entry in raw_players:
                team = team_entry.get("team") or {}
                t_id = team.get("id")
                team_key = f"team_{t_id}" if t_id else "unknown_team"
                out.setdefault(team_key, [])
                players_list = team_entry.get("players") or team_entry.get("statistics") or []
                if isinstance(players_list, list) and players_list:
                    for p in players_list:
                        p_norm={"source":"api_sports"}
                        player_info = p.get("player") if isinstance(p.get("player"), dict) else (p.get("player") or p)
                        if isinstance(player_info, dict):
                            p_norm["player_id"] = player_info.get("id")
                            p_norm["player_name"] = player_info.get("name")
                            p_norm["position"] = player_info.get("position")
                        else:
                            p_norm["player_name"] = player_info
                        stats_block = None
                        if isinstance(p.get("statistics"), list) and p.get("statistics"):
                            stats_block = p.get("statistics")[0]
                        elif isinstance(p, dict):
                            stats_block = p
                        if stats_block:
                            p_norm["minutes_played"] = self._to_num((stats_block.get("games") or {}).get("minutes") or stats_block.get("minutes"))
                            p_norm["shots"] = self._to_num((stats_block.get("shots") or {}).get("total") or stats_block.get("shots"))
                            p_norm["shots_on_target"] = self._to_num((stats_block.get("shots") or {}).get("on") or (stats_block.get("shots") or {}).get("on_target"))
                            p_norm["xg"] = self._to_num((stats_block.get("shots") or {}).get("xG") or stats_block.get("xg") or stats_block.get("shots_xg"))
                            p_norm["assists"] = self._to_num((stats_block.get("goals") or {}).get("assists"))
                            p_norm["key_passes"] = self._to_num((stats_block.get("passes") or {}).get("key"))
                            p_norm["passes"] = self._to_num((stats_block.get("passes") or {}).get("total") or stats_block.get("passes"))
                            p_norm["progressive_passes"] = self._to_num((stats_block.get("passes") or {}).get("progressive") or 0)
                            p_norm["tackles"] = self._to_num((stats_block.get("tackles") or {}).get("total"))
                            p_norm["interceptions"] = self._to_num((stats_block.get("tackles") or {}).get("interceptions"))
                            p_norm["saves"] = self._to_num((stats_block.get("goals") or {}).get("saves") or (stats_block.get("shots") or {}).get("saves") or (stats_block.get("goalkeeper") or {}).get("saves"))
                            p_norm["yellow_cards"] = self._to_num(((stats_block.get("cards") or {}).get("yellow")))
                            p_norm["red_cards"] = self._to_num(((stats_block.get("cards") or {}).get("red")))
                            subs = stats_block.get("substitutes") or {}
                            p_norm["sub_in"] = self._to_num(subs.get("in"))
                            p_norm["sub_out"] = self._to_num(subs.get("out"))
                        out[team_key].append(p_norm)
        except Exception:
            logger.exception("_normalize_players")
        return out

    def _normalize_lineups(self, raw_lineups: List[Dict[str,Any]]) -> Dict[str,Any]:
        out={}
        try:
            for item in raw_lineups or []:
                team = item.get("team") or {}
                t_id = team.get("id")
                subs = item.get("substitutes") or []
                count = len(subs) if isinstance(subs, list) else 0
                last_min = None
                for s in subs:
                    m = (s.get("time") or {}).get("elapsed") if isinstance(s.get("time"), dict) else s.get("minute")
                    if m:
                        try:
                            mn = int(m)
                        except Exception:
                            try:
                                mn = int(float(str(m)))
                            except Exception:
                                mn = None
                        if mn is not None:
                            if last_min is None or mn > last_min:
                                last_min = mn
                if t_id:
                    out[f"substitutions_count_team_{t_id}"] = self._wrap(count)
                    out[f"substitutions_last_min_team_{t_id}"] = self._wrap(last_min)
        except Exception:
            logger.exception("_normalize_lineups")
        return out

    def _normalize_prematch(self, raw_odds: List[Dict[str,Any]], raw_fixture: Dict[str,Any]) -> Dict[str,Any]:
        out={}
        try:
            if not raw_odds:
                return out
            home_odds=[]; draw_odds=[]; away_odds=[]
            for entry in raw_odds:
                for bm in (entry.get("bookmakers") or entry.get("bookmaker") or []):
                    for bet in bm.get("bets") or []:
                        name = (bet.get("name") or "").lower()
                        if "match winner" in name or bet.get("id") in (1,):
                            for val in bet.get("values") or []:
                                label = (val.get("value") or "").lower()
                                odd = val.get("odd") or val.get("price") or val.get("odds")
                                if not odd:
                                    continue
                                o = self._to_num(odd)
                                if "home" in label:
                                    home_odds.append(o)
                                elif "draw" in label or "x" in label:
                                    draw_odds.append(o)
                                else:
                                    away_odds.append(o)
            def avg(l): return round(sum(l)/len(l),3) if l else None
            if home_odds: out["odds_home"] = self._wrap(avg(home_odds))
            if draw_odds: out["odds_draw"] = self._wrap(avg(draw_odds))
            if away_odds: out["odds_away"] = self._wrap(avg(away_odds))
        except Exception:
            logger.exception("_normalize_prematch")
        return out

    def _postprocess_fixture(self, f: Dict[str,Any]) -> Dict[str,Any]:
        try:
            for k in ("elapsed","score_home","score_away"):
                if k in f:
                    v = f[k]
                    if isinstance(v, dict) and "value" in v:
                        f[k]["value"] = self._to_num(v["value"])
                    else:
                        f[k] = self._wrap(v)
            try:
                minute = int((f.get("elapsed") or {}).get("value") or 0)
            except Exception:
                minute = 0
            for side in ("home","away"):
                eg_key = f"expected_goals_{side}"
                if eg_key in f and f[eg_key] and isinstance(f[eg_key], dict):
                    curr = self._to_num(f[eg_key].get("value"))
                    if curr is not None:
                        remain = max(0, 75 - minute)
                        lam = curr / max(1.0, minute) if minute>0 else curr / 45.0
                        pred = curr + lam * remain
                        f[f"predicted_xg_{side}_to_75"] = self._wrap(round(pred,3), "derived_api_sports")
        except Exception:
            logger.exception("_postprocess_fixture")
        return f

    def collect_match_all(self, fixture_id: int) -> Dict[str,Any]:
        out = {"fixture":{}, "events":[], "shotmap":[], "players":{}, "prematch":{}}
        raw_fixture = self.fetch_fixture(fixture_id) or {}
        raw_stats = self.fetch_fixture_statistics(fixture_id) or []
        raw_events = self.fetch_fixture_events(fixture_id) or []
        raw_players = self.fetch_players(fixture_id) or []
        raw_shots = self.fetch_shotmap(fixture_id) or []
        raw_odds = self.fetch_odds(fixture_id) or []
        raw_lineups = self.fetch_lineups(fixture_id) or []

        if raw_stats:
            out["statistics_raw"] = raw_stats

        out["fixture"].update(self._normalize_fixture_basic(raw_fixture))
        out["fixture"].update(self._normalize_statistics(raw_stats, raw_fixture))
        out["prematch"].update(self._normalize_prematch(raw_odds, raw_fixture))
        out["events"] = self._normalize_events(raw_events)
        out["shotmap"] = self._normalize_shotmap(raw_shots)
        out["players"] = self._normalize_players(raw_players)
        out["fixture"].update(self._normalize_lineups(raw_lineups))
        out["fixture"] = self._postprocess_fixture(out["fixture"])
        try:
            update_team_stats_for_fixture(self, out.get("fixture", {}))
        except Exception:
            logger.exception("[TEAM] update failed during collect_match_all fixture_id=%s", fixture_id)
        return out

# -------------------------
# State persistence & saver daemon
# -------------------------
def load_json(path: str, default: Any) -> Any:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except json.JSONDecodeError:
        ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        broken_path = f"{path}.broken.{ts}.json"
        try:
            os.replace(path, broken_path)
            logger.warning(f"[PERSIST] Broken JSON moved: {path} -> {broken_path}")
        except Exception:
            logger.exception(f"[PERSIST] Failed to move broken JSON: {path}")
        return default
    except Exception:
        logger.exception(f"[PERSIST] Failed to load JSON: {path}")
        return default


def save_json(path: str, data: Any) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def save_json_atomic(path: str, data: Any) -> None:
    save_json(path, data)


_league_file_lock = threading.RLock()
leagues_state_lock = threading.RLock()
leagues_state: Dict[str, Dict[str, Any]] = {}
teams_state_lock = threading.RLock()
teams_state: Dict[str, Dict[str, Any]] = {}
league_update_queue: deque[int] = deque()
league_update_set: Set[int] = set()
league_queue_lock = threading.RLock()
cup_league_queue: deque[int] = deque()
cup_league_set: Set[int] = set()
cup_league_queue_lock = threading.RLock()
_cup_batch_initialized = False
_cup_batch_stats: Dict[str, int] = {"processed": 0, "success": 0, "failed": 0, "skipped_cooldown": 0}
_league_stale_scan_date_msk: Optional[str] = None
_league_last_queue_process_ts: float = 0.0


@contextmanager
def _locked_json_file(path: str):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    lock_path = f"{path}.lock"
    with _league_file_lock:
        with open(lock_path, "a+b") as lock_fh:
            locked = False
            try:
                if os.name == "nt":
                    import msvcrt
                    lock_fh.seek(0)
                    lock_fh.write(b"0")
                    lock_fh.flush()
                    lock_fh.seek(0)
                    msvcrt.locking(lock_fh.fileno(), msvcrt.LK_LOCK, 1)
                    locked = True
                yield
            finally:
                if locked:
                    try:
                        import msvcrt
                        lock_fh.seek(0)
                        msvcrt.locking(lock_fh.fileno(), msvcrt.LK_UNLCK, 1)
                    except Exception:
                        pass


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_iso_utc(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        s = str(value).strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _is_within_ttl_iso(last_updated_utc: Any, ttl_days: int = LEAGUE_FACTOR_TTL_DAYS) -> bool:
    dt = _parse_iso_utc(last_updated_utc)
    if dt is None:
        return False
    return (datetime.now(timezone.utc) - dt) < timedelta(days=max(1, int(ttl_days or LEAGUE_FACTOR_TTL_DAYS)))


def _normalize_league_type(value: Any, fallback_name: Optional[str] = None) -> Optional[str]:
    """
    Normalize league type. If value is explicitly 'League' or 'Cup', return it.
    Otherwise if value is None/empty, try to determine type from fallback_name pattern.
    """
    if isinstance(value, dict):
        value = value.get("value") or value.get("name")
    raw = str(value or "").strip()
    lowered = raw.lower()
    
    # FIRST: Check for forced Cup overrides (these take precedence)
    if fallback_name and str(fallback_name).strip() in CUP_LEAGUE_EXCEPTIONS:
        return "Cup"
    
    # SECOND: Explicit type from API
    if lowered == "league":
        return "League"
    if lowered == "cup":
        return "Cup"
    
    # THIRD: If no explicit type, try fallback detection by name
    if not raw and fallback_name:
        lname = str(fallback_name or "").lower()
        # Check if any cup pattern matches
        for pattern in CUP_NAME_PATTERNS:
            if pattern in lname:
                return "Cup"
        # Default to League if name matches no cup patterns
        return "League"
    
    return None


def _league_default_record(
    league_id: int,
    name: str = "",
    country: Optional[str] = None,
    season: Optional[int] = None,
    league_type: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "league_id": int(league_id),
        "name": str(name or ""),
        "country": str(country) if country else None,
        "type": _normalize_league_type(league_type, fallback_name=name or ""),
        "season": int(season) if season is not None else None,
        "last_updated_utc": None,
        "sample_size": 0,
        "total_goals": 0,
        "avg_goals": 0.0,
        "factor": 1.0,
        "last_attempt_utc": None,
        "last_error": None,
        "cooldown_until_utc": None,
    }


def _normalize_league_record(league_id: int, rec: Any) -> Dict[str, Any]:
    base = _league_default_record(league_id)
    if not isinstance(rec, dict):
        return base

    league_name = str(rec.get("name") or "")
    base["name"] = league_name
    base["country"] = str(rec.get("country")) if rec.get("country") else None
    # Use league name as fallback for type detection
    base["type"] = _normalize_league_type(rec.get("type"), fallback_name=league_name)
    season_raw = rec.get("season")
    try:
        base["season"] = int(season_raw) if season_raw is not None else None
    except Exception:
        base["season"] = None

    base["last_updated_utc"] = rec.get("last_updated_utc") or None
    base["last_attempt_utc"] = rec.get("last_attempt_utc") or None
    base["last_error"] = str(rec.get("last_error")) if rec.get("last_error") else None
    base["cooldown_until_utc"] = rec.get("cooldown_until_utc") or None

    try:
        base["sample_size"] = max(0, int(rec.get("sample_size") or 0))
    except Exception:
        base["sample_size"] = 0
    try:
        base["total_goals"] = max(0, int(rec.get("total_goals") or 0))
    except Exception:
        base["total_goals"] = 0
    try:
        base["avg_goals"] = float(rec.get("avg_goals") or 0.0)
    except Exception:
        base["avg_goals"] = 0.0
    try:
        factor_raw = float(rec.get("factor") or 1.0)
    except Exception:
        factor_raw = 1.0
    base["factor"] = float(max(LEAGUE_FACTOR_CLAMP_MIN, min(LEAGUE_FACTOR_CLAMP_MAX, factor_raw)))

    return base


def load_persist_leagues() -> Dict[str, Dict[str, Any]]:
    os.makedirs(PERSIST_DIR, exist_ok=True)
    loaded_raw: Any = {}

    with _locked_json_file(PERSIST_LEAGUES_PATH):
        if os.path.exists(PERSIST_LEAGUES_PATH):
            try:
                with open(PERSIST_LEAGUES_PATH, "r", encoding="utf-8") as fh:
                    loaded_raw = json.load(fh)
            except json.JSONDecodeError:
                ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
                broken_path = f"{PERSIST_LEAGUES_PATH}.broken.{ts}.json"
                try:
                    os.replace(PERSIST_LEAGUES_PATH, broken_path)
                    logger.warning("[LEAGUE_PERSIST] Broken JSON moved: %s -> %s", PERSIST_LEAGUES_PATH, broken_path)
                except Exception:
                    logger.exception("[LEAGUE_PERSIST] Failed to move broken JSON")

                bak_path = f"{PERSIST_LEAGUES_PATH}.bak"
                if os.path.exists(bak_path):
                    try:
                        with open(bak_path, "r", encoding="utf-8") as fh:
                            loaded_raw = json.load(fh)
                        logger.warning("[LEAGUE_PERSIST] Restored from backup: %s", bak_path)
                    except Exception:
                        logger.exception("[LEAGUE_PERSIST] Failed to read backup JSON")
                        loaded_raw = {}
                else:
                    loaded_raw = {}
            except Exception:
                logger.exception("[LEAGUE_PERSIST] Failed to load %s", PERSIST_LEAGUES_PATH)
                loaded_raw = {}

    normalized: Dict[str, Dict[str, Any]] = {}
    if isinstance(loaded_raw, dict):
        for key, rec in loaded_raw.items():
            try:
                league_id = int(key)
            except Exception:
                if isinstance(rec, dict):
                    try:
                        league_id = int(rec.get("league_id"))
                    except Exception:
                        continue
                else:
                    continue
            normalized[str(league_id)] = _normalize_league_record(league_id, rec)
    return normalized


def save_persist_leagues(data: Dict[str, Dict[str, Any]]) -> None:
    payload: Dict[str, Dict[str, Any]] = {}
    for key, rec in (data or {}).items():
        try:
            lid = int(key)
        except Exception:
            continue
        payload[str(lid)] = _normalize_league_record(lid, rec)

    with _locked_json_file(PERSIST_LEAGUES_PATH):
        os.makedirs(PERSIST_DIR, exist_ok=True)
        tmp_path = f"{PERSIST_LEAGUES_PATH}.tmp"
        bak_path = f"{PERSIST_LEAGUES_PATH}.bak"
        try:
            with open(tmp_path, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, ensure_ascii=False, indent=2)
            if os.path.exists(PERSIST_LEAGUES_PATH):
                try:
                    shutil.copy2(PERSIST_LEAGUES_PATH, bak_path)
                except Exception:
                    logger.exception("[LEAGUE_PERSIST] Failed to create backup: %s", bak_path)
            os.replace(tmp_path, PERSIST_LEAGUES_PATH)
        finally:
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except Exception:
                pass


def load_leagues_state() -> None:
    global leagues_state
    normalized = load_persist_leagues()
    with leagues_state_lock:
        leagues_state = normalized
    save_leagues_state()


def save_leagues_state() -> None:
    with leagues_state_lock:
        payload = dict(leagues_state)
    save_persist_leagues(payload)


def _team_default_record(
    team_id: int,
    name: str = "",
    league_id: Optional[int] = None,
    season: Optional[int] = None,
) -> Dict[str, Any]:
    return {
        "team_id": int(team_id),
        "name": str(name or ""),
        "league_id": int(league_id) if league_id is not None else None,
        "season": int(season) if season is not None else None,
        "matches_used": 0,
        "avg_scored": 0.0,
        "avg_conceded": 0.0,
        "attack_factor": 1.0,
        "defense_factor": 1.0,
        "updated_at_utc": None,
        "cooldown_until_utc": None,
        "last_error": None,
    }


def _normalize_team_record(team_id: int, rec: Any) -> Dict[str, Any]:
    base = _team_default_record(team_id)
    if not isinstance(rec, dict):
        return base

    base["name"] = str(rec.get("name") or "")
    try:
        base["league_id"] = int(rec.get("league_id")) if rec.get("league_id") is not None else None
    except Exception:
        base["league_id"] = None
    try:
        base["season"] = int(rec.get("season")) if rec.get("season") is not None else None
    except Exception:
        base["season"] = None

    try:
        base["matches_used"] = max(0, int(rec.get("matches_used") or 0))
    except Exception:
        base["matches_used"] = 0
    try:
        base["avg_scored"] = float(rec.get("avg_scored") or 0.0)
    except Exception:
        base["avg_scored"] = 0.0
    try:
        base["avg_conceded"] = float(rec.get("avg_conceded") or 0.0)
    except Exception:
        base["avg_conceded"] = 0.0

    try:
        attack = float(rec.get("attack_factor") or 1.0)
    except Exception:
        attack = 1.0
    try:
        defense = float(rec.get("defense_factor") or 1.0)
    except Exception:
        defense = 1.0
    base["attack_factor"] = float(clamp(attack, LEAGUE_FACTOR_CLAMP_MIN, LEAGUE_FACTOR_CLAMP_MAX))
    base["defense_factor"] = float(clamp(defense, LEAGUE_FACTOR_CLAMP_MIN, LEAGUE_FACTOR_CLAMP_MAX))

    base["updated_at_utc"] = rec.get("updated_at_utc") or None
    base["cooldown_until_utc"] = rec.get("cooldown_until_utc") or None
    base["last_error"] = str(rec.get("last_error")) if rec.get("last_error") else None
    return base


def load_persist_teams() -> Dict[str, Dict[str, Any]]:
    os.makedirs(PERSIST_DIR, exist_ok=True)
    loaded_raw: Any = {}

    with _locked_json_file(PERSIST_TEAMS_PATH):
        if os.path.exists(PERSIST_TEAMS_PATH):
            try:
                with open(PERSIST_TEAMS_PATH, "r", encoding="utf-8") as fh:
                    loaded_raw = json.load(fh)
            except json.JSONDecodeError:
                ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
                broken_path = f"{PERSIST_TEAMS_PATH}.broken.{ts}.json"
                try:
                    os.replace(PERSIST_TEAMS_PATH, broken_path)
                    logger.warning("[TEAM] Broken JSON moved: %s -> %s", PERSIST_TEAMS_PATH, broken_path)
                except Exception:
                    logger.exception("[TEAM] Failed to move broken JSON")
                loaded_raw = {}
            except Exception:
                logger.exception("[TEAM] Failed to load %s", PERSIST_TEAMS_PATH)
                loaded_raw = {}

    normalized: Dict[str, Dict[str, Any]] = {}
    if isinstance(loaded_raw, dict):
        for key, rec in loaded_raw.items():
            try:
                team_id = int(key)
            except Exception:
                if isinstance(rec, dict):
                    try:
                        team_id = int(rec.get("team_id"))
                    except Exception:
                        continue
                else:
                    continue
            normalized[str(team_id)] = _normalize_team_record(team_id, rec)
    return normalized


def save_persist_teams(data: Dict[str, Dict[str, Any]]) -> None:
    payload: Dict[str, Dict[str, Any]] = {}
    for key, rec in (data or {}).items():
        try:
            tid = int(key)
        except Exception:
            continue
        payload[str(tid)] = _normalize_team_record(tid, rec)

    with _locked_json_file(PERSIST_TEAMS_PATH):
        os.makedirs(PERSIST_DIR, exist_ok=True)
        tmp_path = f"{PERSIST_TEAMS_PATH}.tmp"
        try:
            with open(tmp_path, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, ensure_ascii=False, indent=2)
            os.replace(tmp_path, PERSIST_TEAMS_PATH)
        finally:
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except Exception:
                pass


def load_teams_state() -> None:
    global teams_state
    normalized = load_persist_teams()
    with teams_state_lock:
        teams_state = normalized


def save_teams_state() -> None:
    with teams_state_lock:
        payload = dict(teams_state)
    save_persist_teams(payload)


def get_persisted_team_record(team_id: Optional[int]) -> Optional[Dict[str, Any]]:
    if team_id is None:
        return None
    try:
        team_key = str(int(team_id))
    except Exception:
        return None
    with teams_state_lock:
        rec = teams_state.get(team_key)
        if not isinstance(rec, dict):
            return None
        return dict(rec)


def _get_team_goal_totals_for_fixture(raw_fixture: Dict[str, Any], team_id: int) -> Optional[Tuple[int, int]]:
    try:
        teams_block = raw_fixture.get("teams") or {}
        home_id = (teams_block.get("home") or {}).get("id")
        away_id = (teams_block.get("away") or {}).get("id")
        goals_block = raw_fixture.get("goals") or (raw_fixture.get("fixture") or {}).get("goals") or {}
        gh = goals_block.get("home")
        ga = goals_block.get("away")
        if gh is None or ga is None:
            return None
        goals_home = int(gh)
        goals_away = int(ga)

        if home_id is not None and int(home_id) == int(team_id):
            return goals_home, goals_away
        if away_id is not None and int(away_id) == int(team_id):
            return goals_away, goals_home
    except Exception:
        return None
    return None


def _update_single_team_stats(
    client: APISportsMetricsClient,
    team_id: int,
    team_name: str,
    league_id: Optional[int],
    season: Optional[int],
) -> None:
    if team_id is None:
        return

    team_id_int = int(team_id)
    team_key = str(team_id_int)
    rec_existing = get_persisted_team_record(team_id_int)
    rec_before = rec_existing if isinstance(rec_existing, dict) else None

    if rec_before and _is_cooldown_active(rec_before.get("cooldown_until_utc")):
        logger.info("[TEAM] skip (fresh) team_id=%s", team_id_int)
        return

    should_update = True
    if rec_before and _is_within_ttl_iso(rec_before.get("updated_at_utc"), TEAM_STATS_TTL_DAYS):
        should_update = False

    if not should_update:
        logger.info("[TEAM] skip (fresh) team_id=%s", team_id_int)
        return

    if rec_before is None:
        logger.info("[TEAM] added team_id=%s", team_id_int)
    logger.info("[TEAM] updating team_id=%s", team_id_int)

    try:
        fixtures = client.fetch_team_last_fixtures(team_id_int, last=TEAM_STATS_SAMPLE_SIZE)
        totals_for = 0
        totals_against = 0
        used = 0
        for raw_fixture in fixtures:
            status_val = _extract_status_short_from_raw_fixture(raw_fixture)
            if status_val not in FINISHED_STATUSES:
                continue
            pair = _get_team_goal_totals_for_fixture(raw_fixture, team_id_int)
            if pair is None:
                continue
            gf, ga = pair
            totals_for += int(gf)
            totals_against += int(ga)
            used += 1
            if used >= TEAM_STATS_SAMPLE_SIZE:
                break

        if used <= 0:
            raise RuntimeError("no finished fixtures")

        avg_scored = float(totals_for / used)
        avg_conceded = float(totals_against / used)

        league_avg_goals = None
        rec_league = get_persisted_league_record(league_id)
        if rec_league and rec_league.get("avg_goals") is not None:
            try:
                league_avg_goals = float(rec_league.get("avg_goals"))
            except Exception:
                league_avg_goals = None
        if league_avg_goals is None or league_avg_goals <= 0:
            league_avg_goals = float(DEFAULT_LEAGUE_AVG_GOALS)

        league_team_avg = max(0.001, float(league_avg_goals) / 2.0)
        attack_factor = clamp(float(avg_scored) / league_team_avg, LEAGUE_FACTOR_CLAMP_MIN, LEAGUE_FACTOR_CLAMP_MAX)
        defense_factor = clamp(float(avg_conceded) / league_team_avg, LEAGUE_FACTOR_CLAMP_MIN, LEAGUE_FACTOR_CLAMP_MAX)

        with teams_state_lock:
            rec = teams_state.get(team_key)
            if not isinstance(rec, dict):
                rec = _team_default_record(team_id_int, name=team_name, league_id=league_id, season=season)
            if team_name:
                rec["name"] = str(team_name)
            if league_id is not None:
                rec["league_id"] = int(league_id)
            if season is not None:
                rec["season"] = int(season)
            rec["matches_used"] = int(used)
            rec["avg_scored"] = float(round(avg_scored, 6))
            rec["avg_conceded"] = float(round(avg_conceded, 6))
            rec["attack_factor"] = float(round(attack_factor, 6))
            rec["defense_factor"] = float(round(defense_factor, 6))
            rec["updated_at_utc"] = _utc_now_iso()
            rec["cooldown_until_utc"] = None
            rec["last_error"] = None
            teams_state[team_key] = rec
        save_teams_state()
        logger.info(
            "[TEAM] updated team_id=%s avg_scored=%.3f avg_conceded=%.3f",
            team_id_int,
            avg_scored,
            avg_conceded,
        )
    except Exception as e:
        cooldown_dt = datetime.now(timezone.utc) + timedelta(hours=TEAM_STATS_COOLDOWN_HOURS)
        with teams_state_lock:
            rec = teams_state.get(team_key)
            if not isinstance(rec, dict):
                rec = _team_default_record(team_id_int, name=team_name, league_id=league_id, season=season)
            if team_name and not rec.get("name"):
                rec["name"] = str(team_name)
            if league_id is not None and rec.get("league_id") is None:
                rec["league_id"] = int(league_id)
            if season is not None and rec.get("season") is None:
                rec["season"] = int(season)
            rec["last_error"] = str(e)[:500]
            rec["cooldown_until_utc"] = cooldown_dt.isoformat()
            teams_state[team_key] = rec
        save_teams_state()
        logger.warning("[TEAM] error team_id=%s err=\"%s\"", team_id_int, str(e))


def update_team_stats_for_fixture(client: APISportsMetricsClient, fixture_metrics: Dict[str, Any]) -> None:
    if not isinstance(fixture_metrics, dict):
        return

    league_id = _unwrap_metric_int(fixture_metrics.get("league_id"))
    season = _unwrap_metric_int(fixture_metrics.get("league_season"))
    if season is None:
        season = _unwrap_metric_int(fixture_metrics.get("season"))

    home_id = _unwrap_metric_int(fixture_metrics.get("team_home_id"))
    away_id = _unwrap_metric_int(fixture_metrics.get("team_away_id"))
    home_name = normalize_team_name(fixture_metrics.get("team_home_name"))
    away_name = normalize_team_name(fixture_metrics.get("team_away_name"))

    if home_id is not None:
        _update_single_team_stats(client, home_id, home_name, league_id, season)
    if away_id is not None:
        _update_single_team_stats(client, away_id, away_name, league_id, season)


def _unwrap_metric_int(value: Any) -> Optional[int]:
    if isinstance(value, dict):
        value = value.get("value")
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        try:
            return int(float(value))
        except Exception:
            return None


def _extract_league_context(fixture_metrics: Dict[str, Any]) -> Dict[str, Any]:
    league_id = _unwrap_metric_int(fixture_metrics.get("league_id"))

    name_raw = fixture_metrics.get("league_name")
    if isinstance(name_raw, dict):
        league_name = str(name_raw.get("value") or name_raw.get("name") or "").strip()
    else:
        league_name = str(name_raw or "").strip()

    country_raw = fixture_metrics.get("league_country")
    if isinstance(country_raw, dict):
        league_country = str(country_raw.get("value") or country_raw.get("name") or "").strip()
    else:
        league_country = str(country_raw or "").strip()
    if not league_country:
        league_country = None

    season = _unwrap_metric_int(fixture_metrics.get("league_season"))
    if season is None:
        season = _unwrap_metric_int(fixture_metrics.get("season"))

    # Use league name as fallback for type detection
    league_type = _normalize_league_type(fixture_metrics.get("league_type"), fallback_name=league_name)

    return {
        "league_id": league_id,
        "name": league_name,
        "country": league_country,
        "type": league_type,
        "season": season,
    }


def _is_cooldown_active(cooldown_until_utc: Any) -> bool:
    dt = _parse_iso_utc(cooldown_until_utc)
    if dt is None:
        return False
    return dt > datetime.now(timezone.utc)


def enqueue_league_update(
    league_id: Optional[int],
    name: str = "",
    country: Optional[str] = None,
    season: Optional[int] = None,
    league_type: Optional[str] = None,
    reason: str = "",
) -> None:
    if league_id is None:
        return

    league_id_int = int(league_id)
    league_key = str(league_id_int)
    is_first_seen = False
    type_filled = False
    
    # Normalize type, using league name as fallback
    raw_api_type = league_type
    normalized_type = _normalize_league_type(league_type, fallback_name=name)

    with leagues_state_lock:
        rec = leagues_state.get(league_key)
        if not isinstance(rec, dict):
            rec = _league_default_record(league_id_int, name=name, country=country, season=season, league_type=normalized_type)
            leagues_state[league_key] = rec
            is_first_seen = True
            final_type = rec.get("type")
            # Log type determination
            if raw_api_type:
                logger.info('[LEAGUE_TYPE] league_id=%s name="%s" api_type="%s" final_type=%s source=api', 
                           league_id_int, str(name or ""), str(raw_api_type), final_type)
            else:
                logger.info('[LEAGUE_TYPE] league_id=%s name="%s" api_type=null final_type=%s source=fallback_name', 
                           league_id_int, str(name or ""), final_type)
        else:
            if name and not rec.get("name"):
                rec["name"] = str(name)
            if country and not rec.get("country"):
                rec["country"] = str(country)
            
            # If record has no type, try to fill it
            if not _normalize_league_type(rec.get("type")):
                old_type = rec.get("type")
                # Apply fallback if we have name or normalized_type from API
                attempt_type = normalized_type if normalized_type else _normalize_league_type(None, fallback_name=rec.get("name") or name)
                if attempt_type:
                    rec["type"] = attempt_type
                    type_filled = True
                    source = "api" if raw_api_type else "fallback_name"
                    logger.info('[LEAGUE_TYPE] league_id=%s name="%s" api_type=%s old_type=%s new_type=%s source=%s',
                               league_id_int,
                               str(rec.get("name") or name or ""),
                               str(raw_api_type) if raw_api_type else "null",
                               str(old_type) if old_type else "null",
                               attempt_type,
                               source)
            elif raw_api_type and normalized_type and _normalize_league_type(rec.get("type")) != normalized_type:
                # Type mismatch - log it
                logger.info('[LEAGUE_TYPE_MISMATCH] league_id=%s name="%s" api_type="%s" persisted_type=%s',
                           league_id_int,
                           str(rec.get("name") or name or ""),
                           str(raw_api_type),
                           _normalize_league_type(rec.get("type")))
            
            if season is not None:
                try:
                    rec["season"] = int(season)
                except Exception:
                    pass

    if is_first_seen or type_filled:
        if is_first_seen:
            logger.info(
                "[LEAGUE_QUEUE] first seen league_id=%s name='%s' country='%s' type=%s season=%s",
                league_id_int,
                name or "",
                country or "",
                normalized_type or "UNKNOWN",
                season,
            )
        save_leagues_state()

    with league_queue_lock:
        if league_id_int not in league_update_set:
            league_update_set.add(league_id_int)
            league_update_queue.append(league_id_int)
            logger.info("[LEAGUE_QUEUE] queued league_id=%s reason=%s", league_id_int, reason or "unspecified")


def get_persisted_league_record(league_id: Optional[int]) -> Optional[Dict[str, Any]]:
    if league_id is None:
        return None
    try:
        league_key = str(int(league_id))
    except Exception:
        return None
    with leagues_state_lock:
        rec = leagues_state.get(league_key)
        if not isinstance(rec, dict):
            return None
        return dict(rec)


def get_league_factor(
    league_id: Optional[int],
    league_name: str,
    league_country: Optional[str],
    league_type: Optional[str] = None,
    season: Optional[int] = None,
) -> Tuple[float, str, Optional[float]]:
    """
    Get league factor from persist_leagues.json only.

    Rules:
    - Missing league in persist -> return 1.0 and enqueue refresh
    - Stale league (>7 days) -> return current persisted factor and enqueue refresh
    - Fresh league -> return persisted factor
    - If league type is unknown, try to determine it from name
    """
    if league_id is None:
        logger.info('[LEAGUE] league_id=None name="%s" country="%s" avg_goals=None factor=1.00 source=default', str(league_name or ""), str(league_country or ""))
        return 1.0, "default", None

    league_id_int = int(league_id)
    # Use fallback for type detection
    normalized_type = _normalize_league_type(league_type, fallback_name=league_name)
    rec = get_persisted_league_record(league_id_int)

    if not rec:
        enqueue_league_update(
            league_id=league_id_int,
            name=str(league_name or ""),
            country=league_country,
            league_type=normalized_type,
            season=season,
            reason="missing",
        )
        logger.info('[LEAGUE] league_id=%s name="%s" country="%s" avg_goals=None factor=1.00 source=default', league_id_int, str(league_name or ""), str(league_country or ""))
        return 1.0, "default", None

    # Try to fill in missing type from API or fallback
    if not _normalize_league_type(rec.get("type")):
        attempt_type = normalized_type if normalized_type else _normalize_league_type(None, fallback_name=rec.get("name") or league_name)
        if attempt_type:
            old_type = rec.get("type")
            with leagues_state_lock:
                rec_live = leagues_state.get(str(league_id_int))
                if isinstance(rec_live, dict) and not _normalize_league_type(rec_live.get("type")):
                    rec_live["type"] = attempt_type
                    rec = dict(rec_live)
                else:
                    rec = dict(rec or {})
            logger.info(
                '[LEAGUE_TYPE] league_id=%s name="%s" old_type=%s new_type=%s source=%s',
                league_id_int,
                str(rec.get("name") or league_name or ""),
                str(old_type) if old_type else "null",
                attempt_type,
                "api" if normalized_type else "fallback_name",
            )
            save_leagues_state()

    try:
        factor = float(rec.get("factor") or 1.0)
    except Exception:
        factor = 1.0
    factor = float(max(LEAGUE_FACTOR_CLAMP_MIN, min(LEAGUE_FACTOR_CLAMP_MAX, factor)))

    avg_goals: Optional[float] = None
    try:
        if rec.get("avg_goals") is not None:
            avg_goals = float(rec.get("avg_goals"))
    except Exception:
        avg_goals = None

    if not _is_within_ttl_iso(rec.get("last_updated_utc"), LEAGUE_FACTOR_TTL_DAYS):
        enqueue_league_update(
            league_id=league_id_int,
            name=str(league_name or rec.get("name") or ""),
            country=league_country if league_country is not None else rec.get("country"),
            league_type=_normalize_league_type(rec.get("type")) or normalized_type,
            season=season if season is not None else rec.get("season"),
            reason="expired",
        )
    name_out = str(league_name or rec.get("name") or "")
    country_out = str(league_country or rec.get("country") or "")
    avg_out = f"{avg_goals:.2f}" if avg_goals is not None else "None"
    logger.info('[LEAGUE] league_id=%s name="%s" country="%s" avg_goals=%s factor=%.2f source=persist', league_id_int, name_out, country_out, avg_out, factor)
    return factor, "persist", avg_goals


def _get_persisted_league_factor(fixture_metrics: Dict[str, Any]) -> Tuple[float, Optional[float], str]:
    info = _extract_league_context(fixture_metrics)
    league_id = info.get("league_id")
    factor, source, avg_goals = get_league_factor(
        league_id=league_id,
        league_name=str(info.get("name") or ""),
        league_country=info.get("country"),
        league_type=info.get("type"),
        season=info.get("season"),
    )
    return factor, avg_goals, source


def _extract_goals_from_raw_fixture(raw_fixture: Dict[str, Any]) -> Optional[int]:
    try:
        goals = raw_fixture.get("goals") or (raw_fixture.get("fixture") or {}).get("goals") or {}
        home = goals.get("home")
        away = goals.get("away")
        if home is None or away is None:
            return None
        return int(home) + int(away)
    except Exception:
        return None


def _extract_standings_totals(standings_response: List[Dict[str, Any]]) -> Tuple[int, int]:
    total_goals_for = 0
    total_played = 0

    for item in standings_response or []:
        league_obj = item.get("league") if isinstance(item, dict) else None
        standings_groups = league_obj.get("standings") if isinstance(league_obj, dict) else None
        if not isinstance(standings_groups, list):
            continue
        for group in standings_groups:
            if not isinstance(group, list):
                continue
            for team_row in group:
                if not isinstance(team_row, dict):
                    continue
                all_stats = team_row.get("all") or {}
                goals_stats = all_stats.get("goals") or {}
                try:
                    played = int(all_stats.get("played") or 0)
                except Exception:
                    played = 0
                try:
                    goals_for = int(goals_stats.get("for") or 0)
                except Exception:
                    goals_for = 0
                total_played += max(0, played)
                total_goals_for += max(0, goals_for)

    return int(total_goals_for), int(total_played)


def _is_cup_flow_league_record(rec: Dict[str, Any]) -> bool:
    return _normalize_league_mode(rec)[0] == "cup_last30"


def _normalize_league_mode(rec: Dict[str, Any]) -> Tuple[str, str]:
    league_type = _normalize_league_type((rec or {}).get("type"))
    if league_type == "Cup":
        return "cup_last30", "Cup"
    if league_type == "League":
        return "league_standard", "League"
    return "league_standard_default", "UNKNOWN"


def _extract_status_short_from_raw_fixture(raw_fixture: Dict[str, Any]) -> str:
    fixture_obj = raw_fixture.get("fixture") if isinstance(raw_fixture, dict) else {}
    if not isinstance(fixture_obj, dict):
        fixture_obj = {}

    status_obj = fixture_obj.get("status") or raw_fixture.get("status") or {}
    if isinstance(status_obj, dict):
        return str(status_obj.get("short") or status_obj.get("value") or "").upper()
    return str(status_obj or "").upper()


def _extract_fixture_id_from_raw_fixture(raw_fixture: Dict[str, Any]) -> Optional[int]:
    try:
        fixture_obj = raw_fixture.get("fixture") if isinstance(raw_fixture, dict) else {}
        if not isinstance(fixture_obj, dict):
            fixture_obj = {}
        fixture_id_raw = fixture_obj.get("id") or raw_fixture.get("fixture_id") or raw_fixture.get("id")
        if fixture_id_raw is None:
            return None
        return int(fixture_id_raw)
    except Exception:
        return None


def fetch_cup_last30_finished_fixtures(
    client: APISportsMetricsClient,
    league_id: int,
    target_matches: int = LEAGUE_CUP_SAMPLE_TARGET,
    fetch_last: int = 50,
) -> List[Dict[str, Any]]:
    """
    Collect finished cup fixtures using fixtures?league={id}&last=50.
    Keeps only FT/AET/PEN and returns up to target_matches rows.
    """
    target = max(1, int(target_matches or LEAGUE_CUP_SAMPLE_TARGET))
    fetched = client.fetch_cup_league_last_fixtures(int(league_id), last=fetch_last)

    collected: List[Dict[str, Any]] = []
    seen_fixture_ids: Set[int] = set()
    for raw_fixture in fetched:
        status_val = _extract_status_short_from_raw_fixture(raw_fixture)
        if status_val not in FINISHED_STATUSES:
            continue

        fixture_id = _extract_fixture_id_from_raw_fixture(raw_fixture)
        if fixture_id is not None and fixture_id in seen_fixture_ids:
            continue

        goals_total = _extract_goals_from_raw_fixture(raw_fixture)
        if goals_total is None:
            continue

        if fixture_id is not None:
            seen_fixture_ids.add(fixture_id)
        collected.append(raw_fixture)
        if len(collected) >= target:
            break

    return collected


def update_cup_league_stats(client: APISportsMetricsClient, league_id: int, league_meta: Optional[Dict[str, Any]] = None) -> bool:
    league_id_int = int(league_id)
    league_key = str(league_id_int)
    now_iso = _utc_now_iso()

    with leagues_state_lock:
        rec = leagues_state.get(league_key)
        if not isinstance(rec, dict):
            rec = _league_default_record(league_id_int)
            leagues_state[league_key] = rec

        if isinstance(league_meta, dict):
            league_name_meta = str(league_meta.get("name") or "")
            if league_name_meta and not rec.get("name"):
                rec["name"] = league_name_meta
            if league_meta.get("country") and not rec.get("country"):
                rec["country"] = str(league_meta.get("country") or "")
            
            # Apply fallback type detection using name
            league_type_meta = _normalize_league_type(league_meta.get("type"), fallback_name=league_name_meta)
            if league_type_meta and not _normalize_league_type(rec.get("type")):
                rec["type"] = league_type_meta
            
            if rec.get("season") is None and league_meta.get("season") is not None:
                try:
                    rec["season"] = int(league_meta.get("season"))
                except Exception:
                    pass

        if _is_cooldown_active(rec.get("cooldown_until_utc")):
            return False

        rec["last_attempt_utc"] = now_iso

    save_leagues_state()

    try:
        league_name = str(rec.get("name") or "")
        league_type = str(rec.get("type") or "Cup")
        max_matches = LEAGUE_CUP_SAMPLE_TARGET  # 30 for cups
        
        logger.info(
            "[LEAGUE_CUP_START] league_id=%s name='%s' type=%s max_window=%s",
            league_id_int,
            league_name,
            league_type,
            max_matches,
        )
        
        fixtures = fetch_cup_last30_finished_fixtures(
            client=client,
            league_id=league_id_int,
            target_matches=max_matches,
            fetch_last=50,
        )

        totals: List[int] = []
        for raw_fixture in fixtures:
            goals_total = _extract_goals_from_raw_fixture(raw_fixture)
            if goals_total is None:
                continue
            totals.append(int(goals_total))
            if len(totals) >= max_matches:
                break

        sample_size = int(min(max_matches, len(totals)))
        if sample_size <= 0:
            raise RuntimeError("cup_last30 insufficient finished fixtures")

        total_goals = int(sum(totals[:sample_size]))
        avg_goals = float(total_goals / sample_size)
        factor_raw = avg_goals / float(LEAGUE_FACTOR_BASELINE_GOALS)
        factor = float(max(LEAGUE_FACTOR_CLAMP_MIN, min(LEAGUE_FACTOR_CLAMP_MAX, factor_raw)))
        
        logger.info(
            "[LEAGUE_WINDOW] league_id=%s season=%s type=%s completed_found=%s max_matches=%s used_count=%s",
            league_id_int,
            rec.get("season"),
            league_type,
            len(fixtures),
            max_matches,
            sample_size,
        )

        with leagues_state_lock:
            rec_ok = leagues_state.setdefault(league_key, _league_default_record(league_id_int))
            rec_ok["league_id"] = league_id_int
            rec_ok["sample_size"] = int(sample_size)
            rec_ok["total_goals"] = int(total_goals)
            rec_ok["avg_goals"] = float(round(avg_goals, 6))
            rec_ok["factor"] = float(round(factor, 6))
            rec_ok["last_updated_utc"] = _utc_now_iso()
            rec_ok["last_error"] = None
            rec_ok["cooldown_until_utc"] = None

        save_leagues_state()
        logger.info(
            "[LEAGUE_STATS] league_id=%s type=%s sample_size=%s total_goals=%s avg_goals=%.3f baseline=%.2f factor=%.3f",
            league_id_int,
            league_type,
            sample_size,
            total_goals,
            avg_goals,
            LEAGUE_FACTOR_BASELINE_GOALS,
            factor,
        )
        return True
    except Exception as e:
        cooldown_dt = datetime.now(timezone.utc) + timedelta(hours=LEAGUE_REFRESH_COOLDOWN_HOURS)
        err_msg = str(e)
        if "cup_last30 insufficient finished fixtures" in err_msg:
            err_msg = "cup_last30 insufficient finished fixtures"
        
        with leagues_state_lock:
            rec_err = leagues_state.setdefault(league_key, _league_default_record(league_id_int))
            rec_err["sample_size"] = 0
            rec_err["total_goals"] = 0
            rec_err["avg_goals"] = 0.0
            rec_err["factor"] = 1.0
            rec_err["last_error"] = err_msg[:500]
            rec_err["cooldown_until_utc"] = cooldown_dt.isoformat()
            league_name_err = str(rec_err.get("name") or "")
            league_type_err = str(rec_err.get("type") or "Cup")
        
        save_leagues_state()
        logger.warning(
            '[LEAGUE_CUP_ERR] league_id=%s name="%s" type=%s error="%s" cooldown_hours=%s',
            league_id_int,
            league_name_err,
            league_type_err,
            err_msg,
            LEAGUE_REFRESH_COOLDOWN_HOURS,
        )
        return False


def update_league_goals_cup(client: APISportsMetricsClient, league_id: int, league_meta: Optional[Dict[str, Any]] = None) -> bool:
    return update_cup_league_stats(client=client, league_id=league_id, league_meta=league_meta)


def try_league_stats_from_fixtures(client: APISportsMetricsClient, league_id: int, season: Optional[int] = None, league_type: str = "League") -> Tuple[Optional[float], Optional[int], Optional[int]]:
    """
    Try to calculate league stats from recent fixtures when standings are unavailable.
    Returns: (avg_goals, sample_size, total_goals) or (None, None, None) on failure.
    
    Uses different window sizes based on league type:
    - League: up to 200 matches
    - Cup: up to 30 matches
    """
    try:
        league_id_int = int(league_id)
        
        # Determine max matches based on league type
        max_matches = LEAGUE_CUP_SAMPLE_TARGET if league_type == "Cup" else LEAGUE_REFRESH_SAMPLE_TARGET
        
        # Fetch recent finished fixtures for this league
        # Fetch more than needed to account for incomplete/cancelled matches
        fetch_limit = max(100, max_matches * 2)
        fixtures = client.fetch_cup_league_last_fixtures(league_id_int, last=fetch_limit)
        
        totals: List[int] = []
        for raw_fixture in fixtures:
            status_val = _extract_status_short_from_raw_fixture(raw_fixture)
            if status_val not in FINISHED_STATUSES:
                continue
            
            goals_total = _extract_goals_from_raw_fixture(raw_fixture)
            if goals_total is None:
                continue
            
            totals.append(int(goals_total))
            # Use up to max_matches recent matches
            if len(totals) >= max_matches:
                break
        
        if len(totals) <= 0:
            return None, None, None
        
        # Apply window limit (only use max_matches recent matches)
        used_totals = totals[:max_matches]
        sample_size = len(used_totals)
        total_goals = int(sum(used_totals))
        avg_goals = float(total_goals / sample_size)
        
        logger.info(
            "[LEAGUE_WINDOW] league_id=%s type=%s completed_found=%s max_matches=%s used_count=%s",
            league_id_int,
            league_type,
            len(totals),
            max_matches,
            sample_size,
        )
        logger.info(
            "[LEAGUE_STATS] league_id=%s type=%s sample_size=%s total_goals=%s avg_goals=%.3f baseline=%.2f",
            league_id_int,
            league_type,
            sample_size,
            total_goals,
            avg_goals,
            LEAGUE_FACTOR_BASELINE_GOALS,
        )
        
        return avg_goals, sample_size, total_goals
    except Exception as e:
        logger.debug("[LEAGUE_FALLBACK_FIXTURES_ERR] league_id=%s error=%s", league_id, str(e))
        return None, None, None


def update_league_from_standings(client: APISportsMetricsClient, league_id: int, season: Optional[int] = None) -> bool:
    league_id_int = int(league_id)
    league_key = str(league_id_int)
    now_iso = _utc_now_iso()

    # Get league type and determine max_matches limit
    with leagues_state_lock:
        rec = leagues_state.get(league_key)
        if not isinstance(rec, dict):
            rec = _league_default_record(league_id_int)
            leagues_state[league_key] = rec

        # Get league name for type determination
        league_name = str(rec.get("name") or "")
        
        # Determine final league type with forced overrides
        raw_type = str(rec.get("type") or "")
        final_type = _normalize_league_type(raw_type, fallback_name=league_name) or "League"
        
        # Check if forced cup override was applied
        forced_cup = league_name in CUP_LEAGUE_EXCEPTIONS
        
        logger.info(
            "[LEAGUE_TYPE] league_id=%s name='%s' raw_type='%s' final_type=%s forced_cup=%s",
            league_id_int,
            league_name,
            raw_type,
            final_type,
            forced_cup,
        )
        
        # Apply match window limit based on final league type
        max_matches = LEAGUE_CUP_SAMPLE_TARGET if final_type == "Cup" else LEAGUE_REFRESH_SAMPLE_TARGET

        if season is None:
            try:
                season = int(rec.get("season")) if rec.get("season") is not None else None
            except Exception:
                season = None

        if _is_cooldown_active(rec.get("cooldown_until_utc")):
            return False

        rec["last_attempt_utc"] = now_iso

    save_leagues_state()

    # Primary attempt: use standings
    try:
        season_candidates: List[int] = []
        if season is not None:
            season_candidates.append(int(season))
        current_year = now_msk().year
        for s in (current_year, current_year - 1):
            if s not in season_candidates:
                season_candidates.append(s)

        standings_data: List[Dict[str, Any]] = []
        used_season: Optional[int] = None
        for season_try in season_candidates:
            rows = client.fetch_league_standings(league_id=league_id_int, season=season_try)
            gf_total, played_total = _extract_standings_totals(rows)
            if gf_total > 0 and played_total > 0:
                standings_data = rows
                used_season = season_try
                break

        if not standings_data:
            raise RuntimeError("standings unavailable")

        gf_total, played_total = _extract_standings_totals(standings_data)
        if played_total <= 0 or gf_total <= 0:
            raise RuntimeError("standings totals unavailable")

        matches_est = played_total / 2.0
        if matches_est <= 0:
            raise RuntimeError("invalid matches_est")

        # APPLY MATCH WINDOW LIMIT
        # Determine how many matches to use (capped at max_matches)
        completed_found = int(matches_est)
        used_count = int(min(matches_est, max_matches))
        
        # Proportionally estimate goals for the used window
        if matches_est > max_matches:
            # If more matches than limit, scale goals proportionally
            goals_for_window = gf_total * (max_matches / matches_est)
        else:
            # If within limit, use all goals
            goals_for_window = gf_total
        
        avg_goals = float(goals_for_window / used_count)
        sample_size = int(used_count)
        factor = float(max(LEAGUE_FACTOR_CLAMP_MIN, min(LEAGUE_FACTOR_CLAMP_MAX, avg_goals / LEAGUE_FACTOR_BASELINE_GOALS)))
        
        logger.info(
            "[LEAGUE_WINDOW] league_id=%s type=%s completed_found=%s max_matches=%s used_count=%s",
            league_id_int,
            final_type,
            completed_found,
            max_matches,
            used_count,
        )

        with leagues_state_lock:
            rec2 = leagues_state.setdefault(league_key, _league_default_record(league_id_int))
            rec2["league_id"] = league_id_int
            if used_season is not None:
                rec2["season"] = int(used_season)
            rec2["last_updated_utc"] = _utc_now_iso()
            rec2["sample_size"] = int(sample_size)
            rec2["total_goals"] = int(goals_for_window)
            rec2["avg_goals"] = float(round(avg_goals, 6))
            rec2["factor"] = float(round(factor, 6))
            rec2["last_error"] = None
            rec2["cooldown_until_utc"] = None

        save_leagues_state()
        with leagues_state_lock:
            rec_meta = leagues_state.get(league_key) or {}
            name_meta = str(rec_meta.get("name") or "")
            country_meta = str(rec_meta.get("country") or "")
            type_meta = str(rec_meta.get("type") or "League")
        
        logger.info(
            '[LEAGUE] league_id=%s name="%s" country="%s" type=%s avg_goals=%.2f factor=%.2f source=standings',
            league_id_int,
            name_meta,
            country_meta,
            type_meta,
            avg_goals,
            factor,
        )
        logger.info(
            "[LEAGUE_STATS] league_id=%s type=%s sample_size=%s total_goals=%s avg_goals=%.3f baseline=%.2f factor=%.3f",
            league_id_int,
            type_meta,
            sample_size,
            int(goals_for_window),
            avg_goals,
            LEAGUE_FACTOR_BASELINE_GOALS,
            factor,
        )
        return True
    except Exception as standings_error:
        # Fallback: try to use recent fixtures
        logger.info(
            "[LEAGUE_STANDNGS_FAILED] league_id=%s error=%s, attempting fixtures fallback",
            league_id_int,
            str(standings_error)[:100],
        )
        
        # Try fallback with fixture-based stats using detected league_type
        avg_goals_fb, sample_size_fb, total_goals_fb = try_league_stats_from_fixtures(client, league_id_int, season, league_type=final_type)
        
        if avg_goals_fb is not None and sample_size_fb is not None and sample_size_fb > 0:
            factor_fb = float(max(LEAGUE_FACTOR_CLAMP_MIN, min(LEAGUE_FACTOR_CLAMP_MAX, avg_goals_fb / LEAGUE_FACTOR_BASELINE_GOALS)))
            
            with leagues_state_lock:
                rec_fb = leagues_state.setdefault(league_key, _league_default_record(league_id_int))
                rec_fb["league_id"] = league_id_int
                rec_fb["last_updated_utc"] = _utc_now_iso()
                rec_fb["sample_size"] = int(sample_size_fb)
                rec_fb["total_goals"] = int(total_goals_fb)
                rec_fb["avg_goals"] = float(round(avg_goals_fb, 6))
                rec_fb["factor"] = float(round(factor_fb, 6))
                rec_fb["last_error"] = None
                rec_fb["cooldown_until_utc"] = None
                league_name_fb = str(rec_fb.get("name") or "")
                type_fb = str(rec_fb.get("type") or "League")
            
            save_leagues_state()
            logger.info(
                '[LEAGUE_FALLBACK_OK] league_id=%s name="%s" type=%s avg_goals=%.2f factor=%.2f source=fixtures sample_size=%s',
                league_id_int,
                league_name_fb,
                type_fb,
                avg_goals_fb,
                factor_fb,
                sample_size_fb,
            )
            logger.info(
                "[LEAGUE_STATS] league_id=%s type=%s sample_size=%s total_goals=%s avg_goals=%.3f baseline=%.2f factor=%.3f",
                league_id_int,
                type_fb,
                sample_size_fb,
                total_goals_fb,
                avg_goals_fb,
                LEAGUE_FACTOR_BASELINE_GOALS,
                factor_fb,
            )
            return True
        
        # If fallback also failed
        cooldown_dt = datetime.now(timezone.utc) + timedelta(hours=LEAGUE_REFRESH_COOLDOWN_HOURS)
        cooldown_iso = cooldown_dt.isoformat()
        with leagues_state_lock:
            rec_err = leagues_state.setdefault(league_key, _league_default_record(league_id_int))
            rec_err["sample_size"] = 0
            rec_err["total_goals"] = 0
            rec_err["avg_goals"] = 0.0
            rec_err["factor"] = 1.0
            rec_err["last_error"] = str(standings_error)[:500]
            rec_err["cooldown_until_utc"] = cooldown_iso
        save_leagues_state()
        logger.warning(
            "[LEAGUE_UPDATE_ERR] league_id=%s err=\"%s\" cooldown_hours=%s",
            league_id_int,
            str(standings_error),
            LEAGUE_REFRESH_COOLDOWN_HOURS,
        )
        return False


def update_league_stats(client: APISportsMetricsClient, league_id: int) -> bool:
    rec = get_persisted_league_record(league_id) or {}
    mode, _ = _normalize_league_mode(rec)
    if mode == "cup_last30":
        return update_league_goals_cup(client=client, league_id=league_id, league_meta=rec)
    return update_league_from_standings(client=client, league_id=league_id, season=None)


def refresh_league_factor(client: APISportsMetricsClient, league_id: int) -> bool:
    rec = get_persisted_league_record(league_id) or {}
    mode, _ = _normalize_league_mode(rec)
    if mode == "cup_last30":
        return update_league_goals_cup(client=client, league_id=league_id, league_meta=rec)
    return update_league_from_standings(client=client, league_id=league_id, season=None)


def refresh_all_cup_leagues_now() -> None:
    """
    Queue all cup-flow leagues for non-blocking sequential updates.
    Intended to be called once at startup.
    """
    global _cup_batch_initialized, _cup_batch_stats
    if _cup_batch_initialized:
        return

    queued = 0
    skipped_cooldown = 0
    with leagues_state_lock:
        league_items = [(k, dict(v)) for k, v in leagues_state.items() if isinstance(v, dict)]

    for league_key, rec in league_items:
        try:
            league_id = int(league_key)
        except Exception:
            continue

        if not _is_cup_flow_league_record(rec):
            continue

        if _is_cooldown_active(rec.get("cooldown_until_utc")):
            skipped_cooldown += 1
            continue

        with cup_league_queue_lock:
            if league_id not in cup_league_set:
                cup_league_set.add(league_id)
                cup_league_queue.append(league_id)
                queued += 1

    _cup_batch_stats = {
        "processed": 0,
        "success": 0,
        "failed": 0,
        "skipped_cooldown": skipped_cooldown,
    }
    _cup_batch_initialized = True
    logger.info(
        "[LEAGUE_CUP_BATCH] processed=%s success=%s failed=%s skipped_cooldown=%s",
        0,
        0,
        0,
        skipped_cooldown,
    )
    if queued > 0:
        logger.info("[LEAGUE_CUP_BATCH] queued cup leagues=%s", queued)


def process_cup_league_queue(client: APISportsMetricsClient, limit: int = 1) -> int:
    if not client:
        return 0

    processed = 0
    max_jobs = max(1, int(limit or 1))

    while processed < max_jobs:
        with cup_league_queue_lock:
            if not cup_league_queue:
                break
            league_id = int(cup_league_queue.popleft())
            cup_league_set.discard(league_id)

        with leagues_state_lock:
            rec = dict(leagues_state.get(str(league_id)) or {})
            if _is_cooldown_active(rec.get("cooldown_until_utc")):
                _cup_batch_stats["skipped_cooldown"] = _cup_batch_stats.get("skipped_cooldown", 0) + 1
                continue

        ok = False
        try:
            ok = bool(update_league_goals_cup(client, league_id, league_meta=rec))
        except Exception:
            logger.exception("[LEAGUE_CUP_ERR] unexpected refresh exception for league_id=%s", league_id)

        _cup_batch_stats["processed"] = _cup_batch_stats.get("processed", 0) + 1
        if ok:
            _cup_batch_stats["success"] = _cup_batch_stats.get("success", 0) + 1
        else:
            _cup_batch_stats["failed"] = _cup_batch_stats.get("failed", 0) + 1

        processed += 1

    with cup_league_queue_lock:
        is_done = len(cup_league_queue) == 0

    if processed > 0 and is_done:
        logger.info(
            "[LEAGUE_CUP_BATCH] processed=%s success=%s failed=%s skipped_cooldown=%s",
            _cup_batch_stats.get("processed", 0),
            _cup_batch_stats.get("success", 0),
            _cup_batch_stats.get("failed", 0),
            _cup_batch_stats.get("skipped_cooldown", 0),
        )

    return processed


def enqueue_stale_leagues_for_weekly_refresh(limit: int = 100) -> int:
    queued = 0
    with leagues_state_lock:
        league_ids = [int(k) for k in leagues_state.keys() if str(k).isdigit()]

    for league_id in league_ids:
        with leagues_state_lock:
            rec = leagues_state.get(str(league_id)) or {}
            is_stale = not _is_within_ttl_iso(rec.get("last_updated_utc"), LEAGUE_FACTOR_TTL_DAYS)
            cooldown_active = _is_cooldown_active(rec.get("cooldown_until_utc"))
            if not is_stale or cooldown_active:
                continue
            name = str(rec.get("name") or "")
            country = rec.get("country")
            league_type = rec.get("type")

        enqueue_league_update(
            league_id=league_id,
            name=name,
            country=country,
            league_type=league_type,
            reason="weekly_stale",
        )
        queued += 1
        if queued >= max(1, int(limit or 1)):
            break

    return queued


def process_league_update_queue(client: APISportsMetricsClient, limit: int = LEAGUE_REFRESH_MAX_PER_CYCLE) -> int:
    if not client:
        return 0

    processed = 0
    max_jobs = max(1, int(limit or LEAGUE_REFRESH_MAX_PER_CYCLE))

    while processed < max_jobs:
        with league_queue_lock:
            if not league_update_queue:
                break
            league_id = int(league_update_queue.popleft())
            league_update_set.discard(league_id)

        with leagues_state_lock:
            rec = leagues_state.get(str(league_id)) or {}
            if _is_cooldown_active(rec.get("cooldown_until_utc")):
                continue
            season = rec.get("season")
            league_name = str(rec.get("name") or "")

        mode, type_out = _normalize_league_mode(rec)

        logger.info('[LEAGUE_MODE] league_id=%s name="%s" type=%s mode=%s', league_id, league_name, type_out, mode)

        try:
            if mode == "cup_last30":
                update_league_goals_cup(client, league_id, league_meta=dict(rec))
            else:
                update_league_from_standings(client, league_id, season=season)
        except Exception:
            logger.exception("[LEAGUE_QUEUE] unexpected refresh exception for league_id=%s", league_id)
        processed += 1

    return processed


def weekly_refresh_tick(client: APISportsMetricsClient, stale_scan_limit: int = 200, process_limit: int = LEAGUE_REFRESH_MAX_PER_CYCLE) -> int:
    global _league_stale_scan_date_msk, _league_last_queue_process_ts

    today_msk = get_msk_date()
    if _league_stale_scan_date_msk != today_msk:
        queued_stale = enqueue_stale_leagues_for_weekly_refresh(limit=stale_scan_limit)
        _league_stale_scan_date_msk = today_msk
        if queued_stale > 0:
            logger.info("[LEAGUE_WEEKLY] queued stale leagues=%s date=%s", queued_stale, today_msk)

    now_ts = time.time()
    if (now_ts - _league_last_queue_process_ts) < LEAGUE_QUEUE_PROCESS_INTERVAL_SECONDS:
        return 0

    refreshed_count = process_league_update_queue(client, limit=process_limit)
    _league_last_queue_process_ts = now_ts
    return refreshed_count


persistent_state_lock = threading.RLock()
persistent_state: Dict[str, Any] = {}
matches_state_lock = threading.RLock()
matches_state: Dict[str, Any] = {}


def _default_persistent_state() -> Dict[str, Any]:
    return {
        "admin_user_id": ADMIN_USER_ID,
        "started_users": {},
        "pinned_daily_msg_id": None,
        "pinned_daily_stats_message_id": None,
        "instruction_message_id": None,
        "daily_report_last_sent_date": None,
        "last_daily_stats_sent_date_msk": None,
        "stats": {
            "last_daily_stats_date_msk": None
        },
        "maintenance": {
            "last_cleanup_date_msk": None,
            "pending_cleanup": False,
            "cleanup_requested": False,
            "cleanup_requested_at": None,
            "cleanup_in_progress": False,
        }
    }


def _is_match_tracking_payload(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    match_fields = {
        "last_home_score",
        "last_away_score",
        "last_status",
        "last_minute_seen",
        "last_sent_text_hash",
        "is_finished",
    }
    return any(field in value for field in match_fields)


def cleanup_matches(matches: Dict[str, Any], now_ts: float) -> Dict[str, Any]:
    kept: Dict[str, Any] = {}
    removed = 0
    now_val = float(now_ts)

    for fixture_key, rec in (matches or {}).items():
        if not isinstance(rec, dict):
            removed += 1
            continue

        is_finished = bool(rec.get("is_finished", False))
        last_updated_ts_raw = rec.get("last_updated_ts")

        if not is_finished:
            kept[str(fixture_key)] = rec
            continue

        try:
            last_updated_ts = float(last_updated_ts_raw)
        except Exception:
            removed += 1
            continue

        if (now_val - last_updated_ts) > 172800:
            removed += 1
            continue

        kept[str(fixture_key)] = rec

    logger.info(f"[GC] persist_matches cleanup: removed={removed} kept={len(kept)}")
    return kept


def cleanup_persist_matches() -> None:
    """
    Remove finished matches from persist_matches.json when they are older than 48 hours.
    Keeps file structure intact and logs cleanup result.
    """
    global matches_state
    now_ts = time.time()
    with matches_state_lock:
        loaded = load_json(MATCHES_PATH, {})
        if not isinstance(loaded, dict):
            loaded = {}
        cleaned = cleanup_matches(loaded, now_ts)
        matches_state = cleaned
        save_json(MATCHES_PATH, matches_state)


def load_matches_state() -> None:
    global matches_state
    with matches_state_lock:
        loaded = load_json(MATCHES_PATH, {})
        if not isinstance(loaded, dict):
            loaded = {}
        matches_state = cleanup_matches(loaded, time.time())
        save_json(MATCHES_PATH, matches_state)


def save_matches_state() -> None:
    with matches_state_lock:
        try:
            save_json(MATCHES_PATH, matches_state)
        except Exception:
            logger.exception("[PERSIST] Failed to save matches state")


def migrate_matches_from_core() -> None:
    global matches_state
    changed_core = False
    changed_matches = False

    with persistent_state_lock, matches_state_lock:
        if not isinstance(persistent_state, dict):
            return
        if not isinstance(matches_state, dict):
            matches_state = {}

        legacy_tracking = persistent_state.get("fixture_tracking")
        if isinstance(legacy_tracking, dict):
            for fixture_key, rec in legacy_tracking.items():
                if not _is_match_tracking_payload(rec):
                    continue
                fixture_str = str(fixture_key)
                merged = dict(matches_state.get(fixture_str, {})) if isinstance(matches_state.get(fixture_str), dict) else {}
                merged.update(rec)
                merged["last_updated_ts"] = float(merged.get("last_updated_ts") or time.time())
                matches_state[fixture_str] = merged
                changed_matches = True
            persistent_state.pop("fixture_tracking", None)
            changed_core = True

        for key in list(persistent_state.keys()):
            if not (isinstance(key, str) and key.isdigit()):
                continue
            value = persistent_state.get(key)
            if not _is_match_tracking_payload(value):
                continue
            merged = dict(matches_state.get(key, {})) if isinstance(matches_state.get(key), dict) else {}
            merged.update(value)
            merged["last_updated_ts"] = float(merged.get("last_updated_ts") or time.time())
            matches_state[key] = merged
            persistent_state.pop(key, None)
            changed_core = True
            changed_matches = True

        if changed_matches:
            matches_state = cleanup_matches(matches_state, time.time())

    if changed_matches:
        save_matches_state()
    if changed_core:
        save_persistent_state()


def load_persistent_state() -> None:
    global persistent_state
    with persistent_state_lock:
        try:
            file_exists = os.path.exists(CORE_PATH)
            loaded = load_json(CORE_PATH, _default_persistent_state())
            if not isinstance(loaded, dict):
                loaded = _default_persistent_state()
            for k, v in _default_persistent_state().items():
                loaded.setdefault(k, v)

            if loaded.get("admin_user_id") is None and ADMIN_USER_ID is not None:
                loaded["admin_user_id"] = ADMIN_USER_ID

            pinned_legacy = loaded.get("pinned_daily_stats_message_id")
            pinned_new = loaded.get("pinned_daily_msg_id")
            if pinned_new is None and pinned_legacy is not None:
                loaded["pinned_daily_msg_id"] = pinned_legacy
            if pinned_legacy is None and pinned_new is not None:
                loaded["pinned_daily_stats_message_id"] = pinned_new

            daily_legacy = loaded.get("last_daily_stats_sent_date_msk")
            daily_new = loaded.get("daily_report_last_sent_date")
            if daily_new is None and daily_legacy is not None:
                loaded["daily_report_last_sent_date"] = daily_legacy
            if daily_legacy is None and daily_new is not None:
                loaded["last_daily_stats_sent_date_msk"] = daily_new

            started_users = loaded.get("started_users")
            if not isinstance(started_users, dict):
                started_users = {}
                loaded["started_users"] = started_users
            for user_key, user_rec in list(started_users.items()):
                if isinstance(user_rec, dict):
                    ts = user_rec.get("first_seen") or user_rec.get("ts") or datetime.now(timezone.utc).isoformat()
                    user_rec.setdefault("first_seen", ts)
                    user_rec.setdefault("last_seen", ts)
                    user_rec.setdefault("ts", user_rec.get("first_seen"))
                else:
                    now_iso = datetime.now(timezone.utc).isoformat()
                    started_users[str(user_key)] = {"first_seen": now_iso, "last_seen": now_iso, "ts": now_iso}

            persistent_state = loaded
            if not file_exists:
                save_persistent_state()
        except Exception:
            logger.exception("[PERSIST] Failed to load persistent state")
            persistent_state = _default_persistent_state()


def save_persistent_state() -> None:
    with persistent_state_lock:
        try:
            save_json(CORE_PATH, persistent_state)
        except Exception:
            logger.exception("[PERSIST] Save failed")


def _migrate_persistent_state_from_state() -> None:
    changed = False
    with persistent_state_lock:
        if not isinstance(persistent_state.get("stats"), dict):
            persistent_state["stats"] = {"last_daily_stats_date_msk": None}
            changed = True
        else:
            persistent_state["stats"].setdefault("last_daily_stats_date_msk", None)

        if not isinstance(persistent_state.get("maintenance"), dict):
            persistent_state["maintenance"] = {
                "last_cleanup_date_msk": None,
                "pending_cleanup": False,
                "cleanup_requested": False,
                "cleanup_requested_at": None,
                "cleanup_in_progress": False,
            }
            changed = True
        else:
            persistent_state["maintenance"].setdefault("last_cleanup_date_msk", None)
            persistent_state["maintenance"].setdefault("pending_cleanup", False)
            persistent_state["maintenance"].setdefault("cleanup_requested", False)
            persistent_state["maintenance"].setdefault("cleanup_requested_at", None)
            persistent_state["maintenance"].setdefault("cleanup_in_progress", False)

        if persistent_state.get("pinned_daily_stats_message_id") is None:
            pinned_id = state.get("pinned_stats_message_id") or STATS_MESSAGE_ID
            if pinned_id:
                persistent_state["pinned_daily_stats_message_id"] = int(pinned_id)
                persistent_state["pinned_daily_msg_id"] = int(pinned_id)
                changed = True

        if persistent_state.get("instruction_message_id") is None:
            instruction_id = state.get("instruction_message_id")
            if instruction_id:
                persistent_state["instruction_message_id"] = int(instruction_id)
                changed = True

        if not persistent_state.get("started_users"):
            legacy_started = state.get("user_started", {})
            if isinstance(legacy_started, dict) and legacy_started:
                now_iso = datetime.now(timezone.utc).isoformat()
                for user_id in legacy_started.keys():
                    persistent_state["started_users"][str(user_id)] = {
                        "first_seen": now_iso,
                        "last_seen": now_iso,
                        "ts": now_iso,
                    }
                changed = True

        if persistent_state.get("last_daily_stats_sent_date_msk") is None:
            sent_dates = state.get("daily_stats_sent", {})
            if isinstance(sent_dates, dict) and sent_dates:
                try:
                    last_date = max([d for d, v in sent_dates.items() if v])
                except Exception:
                    last_date = None
                if last_date:
                    persistent_state["last_daily_stats_sent_date_msk"] = last_date
                    persistent_state["daily_report_last_sent_date"] = last_date
                    changed = True

        if persistent_state.get("stats", {}).get("last_daily_stats_date_msk") is None:
            sent_dates = state.get("daily_stats_sent", {})
            if isinstance(sent_dates, dict) and sent_dates:
                try:
                    last_stat_date = max([d for d, v in sent_dates.items() if v])
                except Exception:
                    last_stat_date = None
                if last_stat_date:
                    persistent_state["stats"]["last_daily_stats_date_msk"] = last_stat_date
                    changed = True

    if changed:
        save_persistent_state()


def _text_hash(text: str) -> str:
    return hashlib.sha256((text or "").encode("utf-8")).hexdigest()


def _normalize_status_short(fixture: Dict[str, Any]) -> str:
    status = fixture.get("status")
    if isinstance(status, dict):
        return str(status.get("short") or status.get("value") or "").upper()
    return str(status or "").upper()


def _extract_fixture_tracking_snapshot(fixture: Dict[str, Any]) -> Dict[str, Any]:
    score_home = int((fixture.get("score_home") or {}).get("value") or 0)
    score_away = int((fixture.get("score_away") or {}).get("value") or 0)
    minute_seen = int((fixture.get("elapsed") or {}).get("value") or 0)
    status_short = _normalize_status_short(fixture)
    return {
        "last_home_score": score_home,
        "last_away_score": score_away,
        "last_score_home": score_home,
        "last_score_away": score_away,
        "last_status": status_short,
        "last_minute_seen": minute_seen,
    }


def get_persistent_fixture_tracking(fixture_id: int) -> Optional[Dict[str, Any]]:
    fixture_key = str(int(fixture_id))
    with matches_state_lock:
        raw = matches_state.get(fixture_key)
        if not isinstance(raw, dict):
            return None
        raw["last_updated_ts"] = float(time.time())
        out = dict(raw)
    save_matches_state()
    return out


def update_persistent_fixture_tracking(fixture_id: int, fixture: Dict[str, Any], sent_text: Optional[str] = None, force_finished: bool = False) -> None:
    snapshot = _extract_fixture_tracking_snapshot(fixture)
    fixture_key = str(int(fixture_id))
    with matches_state_lock:
        rec = matches_state.setdefault(fixture_key, {})
        if not isinstance(rec, dict):
            rec = {}
            matches_state[fixture_key] = rec

        rec.update(snapshot)
        if sent_text is not None:
            rec["last_sent_text_hash"] = _text_hash(sent_text)
        if force_finished or snapshot.get("last_status") in ("FT", "AET", "PEN"):
            rec["last_status"] = "FT"
            rec["is_finished"] = True
        rec["last_updated_ts"] = float(time.time())

        cleaned = cleanup_matches(matches_state, time.time())
        matches_state.clear()
        matches_state.update(cleaned)

    save_matches_state()


def update_persistent_fixture_text_hash(fixture_id: int, text: str) -> None:
    fixture_key = str(int(fixture_id))
    with matches_state_lock:
        rec = matches_state.setdefault(fixture_key, {})
        if not isinstance(rec, dict):
            rec = {}
            matches_state[fixture_key] = rec
        rec["last_sent_text_hash"] = _text_hash(text)
        rec["last_updated_ts"] = float(time.time())
    save_matches_state()


def prune_finished_matches_from_tracking() -> None:
    finished_ids: Set[int] = set()
    with matches_state_lock:
        if isinstance(matches_state, dict):
            for fixture_id_str, rec in matches_state.items():
                if not isinstance(rec, dict):
                    continue
                status = str(rec.get("last_status") or "").upper()
                if bool(rec.get("is_finished")) or status in ("FT", "AET", "PEN"):
                    try:
                        finished_ids.add(int(fixture_id_str))
                    except Exception:
                        continue

    if not finished_ids:
        return

    with state_lock:
        monitored_before = list(state.get("monitored_matches", []))
        filtered = []
        for match_id in monitored_before:
            try:
                if int(match_id) in finished_ids:
                    continue
            except Exception:
                pass
            filtered.append(match_id)

        if len(filtered) != len(monitored_before):
            state["monitored_matches"] = filtered
            excluded = state.setdefault("excluded_matches", [])
            for match_id in finished_ids:
                if match_id not in excluded:
                    excluded.append(match_id)
            mark_state_dirty()

    for match_id in finished_ids:
        ACTIVE_FIXTURES.discard(int(match_id))


state_lock = threading.RLock()
state: Dict[str, Any] = {
    "sent": {},                     # fixture_id -> legacy tracking payload for footer logic
    "sent_matches": {},
    "last_message_texts": {},
    "tracked_matches": {},            # fixture_id -> tracking metadata for guaranteed updates
    "active_fixtures": [],            # fixture_ids that must be updated by monitor loop
    "signal_header_texts": {},         # match_id -> immutable snapshot header text
    "signal_snapshot_meta": {},        # match_id -> {signal_score_home, signal_score_away, signal_minute, message_id, chat_id, prob_to75, prob_to90}
    "monitored_matches": [],
    "match_initial_score": {},     # score_at_signal (top-line, immutable)
    "match_initial_minute": {},    # minute at signal time (for event filtering)
    "match_prob_status": {},
    "match_goal_status": {},            # per-match dict with processed events and flags
    "match_processed_event_ids": {},    # processed event keys per match
    "match_sent_at": {},                # epoch timestamp when signal was sent
    "excluded_matches": [],
    "ignored_matches": [],              # STRICT: matches to never query again (per user request)
    "daily_stats": {},                  # daily statistics by date (YYYY-MM-DD)
    "match_goals_after_signal": {},     # list of goal minutes after signal per match
    "labeled_matches": [],              # matches that have been labeled in Google Sheets
    "gsheets_rows": {},                 # NEW: match_id -> {"row": sheet_row_number, "signal_minute": minute}
    "user_rate_limit": {},              # Rate limiting: user_id -> {"ts": timestamp}
    "no_stats_fixtures": {},            # Blocked fixtures without live stats: fixture_id -> {"ts": unix_time, "reason": str, "expires_ts": unix_time}
    "no_stats_blocked": {},             # Long block for fixtures without stats: key -> {"ts": unix_time, "reason": str, "expires_ts": unix_time}
    "approved_matches_auto": [],        # Auto-approved review matches (duplicate protection)
    "admin_auto_posted": {},            # fixture_id -> bool, dedupe guard for auto "Сигнал от админа"
    "review_queue": {},                 # fixture_id -> {"review_sent": bool, "review_decision": str, "review_ts": float, "data": {...}}
    "review_tracking": {},              # fixture_id -> {"chat_id": int, "message_id": int, "signal_minute": int, "score_home_at_signal": int, "score_away_at_signal": int, "base_text": str, "created_ts": float, "done": bool}
    "admin_reviews": {}                 # fixture_id -> {"message_id": int, "chat_id": int, "created_ts": float, "last_edit_ts": float, "signal_minute": int, "score_home_at_signal": int, "score_away_at_signal": int, "sent_payload": dict, "finished": bool}
}

def load_state():
    global state
    with state_lock:
        try:
            if os.path.exists(STATE_FILE):
                with open(STATE_FILE, "r", encoding="utf-8") as fh:
                    st = json.load(fh)
                    if isinstance(st, dict):
                        for k, v in st.items():
                            state[k] = v
                        state.setdefault("ignored_matches", state.get("ignored_matches", []))
                        state.setdefault("daily_stats", state.get("daily_stats", {}))
                        state.setdefault("no_stats_blocked", state.get("no_stats_blocked", {}))
                        state.setdefault("approved_matches_auto", state.get("approved_matches_auto", []))
                        state.setdefault("admin_auto_posted", state.get("admin_auto_posted", {}))
                        state.setdefault("admin_reviews", state.get("admin_reviews", {}))
                        state.setdefault("sent", state.get("sent", {}))
                        state.setdefault("tracked_matches", state.get("tracked_matches", {}))
                        state.setdefault("active_fixtures", state.get("active_fixtures", []))
                        state.setdefault("signal_header_texts", state.get("signal_header_texts", {}))
                        state.setdefault("signal_snapshot_meta", state.get("signal_snapshot_meta", {}))
                        logger.info(f"[STATE] Loaded state from {STATE_FILE}")
        except Exception:
            logger.exception("Failed to load state")


def add_tracked_match(
    fixture_id: int,
    message_id: int,
    chat_id: int,
    score_home: int,
    score_away: int,
    status: str = "LIVE",
    sent_at_ts: Optional[float] = None,
    signal_minute: Optional[int] = None,
) -> None:
    now_ts = float(sent_at_ts if sent_at_ts is not None else time.time())
    fixture_key = str(int(fixture_id))
    with state_lock:
        tracked = state.setdefault("tracked_matches", {})
        if not isinstance(tracked, dict):
            tracked = {}
            state["tracked_matches"] = tracked

        existing = tracked.get(fixture_key)
        if not isinstance(existing, dict):
            existing = {}

        rec = {
            "chat_id": int(chat_id),
            "message_id": int(message_id),
            "sent_at_ts": now_ts,
            "signal_ts": now_ts,
            "score_at_signal": {
                "home": int(score_home),
                "away": int(score_away),
            },
            "signal_score": {
                "home": int(score_home),
                "away": int(score_away),
            },
            "signal_minute": int(signal_minute or existing.get("signal_minute", 0) or 0),
            "last_edit_ts": float(existing.get("last_edit_ts", 0.0) or 0.0),
            "last_known_score": {
                "home": int(existing.get("last_known_score", {}).get("home", score_home)),
                "away": int(existing.get("last_known_score", {}).get("away", score_away)),
            },
            "flash_goal_until_ts": float(existing.get("flash_goal_until_ts", 0.0) or 0.0),
            "last_known_scored_goals": list(existing.get("last_known_scored_goals", [])) if isinstance(existing.get("last_known_scored_goals"), list) else [],
            "finished": bool(existing.get("finished", False)),
            "status": str(status or "LIVE").upper(),
        }

        if isinstance(existing.get("score_at_signal"), dict):
            rec["score_at_signal"] = {
                "home": int(existing["score_at_signal"].get("home", score_home)),
                "away": int(existing["score_at_signal"].get("away", score_away)),
            }

        tracked[fixture_key] = rec

        sent_map = state.setdefault("sent", {})
        sent_map[fixture_key] = {
            "chat_id": int(chat_id),
            "message_id": int(message_id),
            "sent_at_ts": now_ts,
            "signal_ts": now_ts,
            "signal_score": {"home": int(score_home), "away": int(score_away)},
            "signal_minute": int(rec.get("signal_minute", 0) or 0),
            "last_edit_ts": float(rec.get("last_edit_ts", 0.0) or 0.0),
            "last_known_score": {
                "home": int(rec.get("last_known_score", {}).get("home", score_home)),
                "away": int(rec.get("last_known_score", {}).get("away", score_away)),
            },
            "flash_goal_until_ts": float(rec.get("flash_goal_until_ts", 0.0) or 0.0),
            "last_known_scored_goals": list(rec.get("last_known_scored_goals", [])),
            "finished": bool(rec.get("finished", False)),
            "status": str(rec.get("status") or "LIVE"),
        }

        active = state.setdefault("active_fixtures", [])
        if fixture_id not in active:
            active.append(int(fixture_id))

        monitored = state.setdefault("monitored_matches", [])
        if fixture_id not in monitored:
            monitored.append(int(fixture_id))

        mark_state_dirty()

    logger.info("[TRACK] added fixture=%s msg_id=%s", int(fixture_id), int(message_id))


def mark_tracked_match_update(fixture_id: int, score_home: int, score_away: int, edited: bool = False, status: str = "LIVE") -> None:
    fixture_key = str(int(fixture_id))
    with state_lock:
        tracked = state.setdefault("tracked_matches", {})
        if not isinstance(tracked, dict):
            tracked = {}
            state["tracked_matches"] = tracked

        rec = tracked.get(fixture_key)
        if not isinstance(rec, dict):
            return

        rec["last_known_score"] = {
            "home": int(score_home),
            "away": int(score_away),
        }
        rec["status"] = str(status or rec.get("status") or "LIVE").upper()
        rec["finished"] = bool(rec.get("status") in ("FT", "AET", "PEN", "CANC", "ABD", "AWD", "WO"))
        if edited:
            rec["last_edit_ts"] = time.time()

        tracked[fixture_key] = rec

        sent_map = state.setdefault("sent", {})
        sent_rec = sent_map.get(fixture_key)
        if not isinstance(sent_rec, dict):
            sent_rec = {
                "chat_id": int(rec.get("chat_id", TELEGRAM_CHAT_ID)),
                "message_id": int(rec.get("message_id", state.get("sent_matches", {}).get(fixture_key, 0)) or 0),
                "sent_at_ts": float(rec.get("sent_at_ts", time.time()) or time.time()),
                "signal_ts": float(rec.get("signal_ts", rec.get("sent_at_ts", time.time())) or time.time()),
                "signal_score": dict(rec.get("signal_score", rec.get("score_at_signal", {"home": int(score_home), "away": int(score_away)}))),
                "signal_minute": int(rec.get("signal_minute", 0) or 0),
            }
        sent_rec["last_known_score"] = {"home": int(score_home), "away": int(score_away)}
        sent_rec["last_edit_ts"] = float(rec.get("last_edit_ts", 0.0) or 0.0)
        sent_rec["flash_goal_until_ts"] = float(rec.get("flash_goal_until_ts", 0.0) or 0.0)
        sent_rec["last_known_scored_goals"] = list(rec.get("last_known_scored_goals", []))
        sent_rec["finished"] = bool(rec.get("finished", False))
        sent_rec["status"] = str(rec.get("status") or "LIVE")
        sent_map[fixture_key] = sent_rec
        mark_state_dirty()


def mark_tracked_match_finished(fixture_id: int, score_home: int, score_away: int) -> None:
    fixture_key = str(int(fixture_id))
    with state_lock:
        tracked = state.setdefault("tracked_matches", {})
        rec = tracked.get(fixture_key) if isinstance(tracked, dict) else None
        if not isinstance(rec, dict):
            rec = {
                "chat_id": int(TELEGRAM_CHAT_ID),
                "message_id": int(state.get("sent_matches", {}).get(fixture_key) or 0),
                "sent_at_ts": time.time(),
                "score_at_signal": {"home": int(score_home), "away": int(score_away)},
                "last_edit_ts": 0.0,
                "last_known_score": {"home": int(score_home), "away": int(score_away)},
                "status": "FT",
            }

        rec["last_known_score"] = {"home": int(score_home), "away": int(score_away)}
        rec["status"] = "FT"
        rec["finished"] = True
        rec["flash_goal_until_ts"] = 0.0
        rec["last_edit_ts"] = time.time()

        if isinstance(tracked, dict):
            tracked[fixture_key] = rec

        active = state.setdefault("active_fixtures", [])
        state["active_fixtures"] = [x for x in active if int(x) != int(fixture_id)]
        monitored = state.setdefault("monitored_matches", [])
        state["monitored_matches"] = [x for x in monitored if int(x) != int(fixture_id)]

        sent_map = state.setdefault("sent", {})
        sent_rec = sent_map.get(fixture_key)
        if not isinstance(sent_rec, dict):
            sent_rec = {}
        sent_rec["last_known_score"] = {"home": int(score_home), "away": int(score_away)}
        sent_rec["status"] = "FT"
        sent_rec["finished"] = True
        sent_rec["flash_goal_until_ts"] = 0.0
        sent_rec["last_known_scored_goals"] = list(rec.get("last_known_scored_goals", []))
        sent_map[fixture_key] = sent_rec
        mark_state_dirty()


def get_tracked_update_candidates() -> List[int]:
    with state_lock:
        tracked = state.get("tracked_matches", {})
        active = state.get("active_fixtures", [])

        candidates: Set[int] = set()
        for item in active if isinstance(active, list) else []:
            try:
                candidates.add(int(item))
            except Exception:
                continue

        if isinstance(tracked, dict):
            for fixture_id_str, rec in tracked.items():
                if not isinstance(rec, dict):
                    continue
                status = str(rec.get("status") or "LIVE").upper()
                if status in ("FT", "AET", "PEN", "CANC", "ABD", "AWD", "WO"):
                    continue
                try:
                    candidates.add(int(fixture_id_str))
                except Exception:
                    continue

    out = [mid for mid in sorted(candidates) if not is_ignored_match(mid)]
    return out


def bootstrap_tracking_from_state() -> None:
    with state_lock:
        sent_matches = state.get("sent_matches", {})
        if not isinstance(sent_matches, dict):
            return

    for fixture_id_str, message_id in sent_matches.items():
        try:
            fixture_id = int(fixture_id_str)
            msg_id = int(message_id)
        except Exception:
            continue

        with state_lock:
            tracked = state.setdefault("tracked_matches", {})
            if isinstance(tracked.get(str(fixture_id)), dict):
                continue
            init_score = state.get("match_initial_score", {}).get(str(fixture_id), (0, 0))
            signal_info = state.get("match_signal_info", {}).get(str(fixture_id), {})
            is_finished = bool((signal_info or {}).get("is_finished", False))
            status = "FT" if is_finished else "LIVE"
            try:
                home_sc = int(init_score[0])
                away_sc = int(init_score[1])
            except Exception:
                home_sc = 0
                away_sc = 0

        add_tracked_match(
            fixture_id=fixture_id,
            message_id=msg_id,
            chat_id=int(TELEGRAM_CHAT_ID),
            score_home=home_sc,
            score_away=away_sc,
            status=status,
            sent_at_ts=time.time(),
            signal_minute=int(state.get("match_initial_minute", {}).get(str(fixture_id), 0) or 0),
        )

def save_state_to_disk():
    with state_lock:
        try:
            tmp = STATE_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(state, fh, ensure_ascii=False, indent=2)
            os.replace(tmp, STATE_FILE)
            logger.info("[STATE] Saved")
        except Exception:
            logger.exception("[STATE] Save failed")

_state_dirty = False
_state_dirty_lock = threading.RLock()
_state_saver_thread: Optional[threading.Thread] = None
_state_saver_stop = threading.Event()

def mark_state_dirty():
    global _state_dirty
    with _state_dirty_lock:
        _state_dirty = True

def state_saver_daemon(interval: int = SAVE_INTERVAL):
    global _state_dirty
    logger.info(f"[STATE] saver daemon started, interval={interval}s")
    while not _state_saver_stop.wait(interval):
        with _state_dirty_lock:
            dirty = _state_dirty
            if dirty:
                try:
                    save_state_to_disk()
                finally:
                    _state_dirty = False
    logger.info("[STATE] saver daemon stopped")

def start_state_saver_daemon():
    global _state_saver_thread
    if _state_saver_thread and _state_saver_thread.is_alive():
        return
    _state_saver_stop.clear()
    t = threading.Thread(target=state_saver_daemon, args=(SAVE_INTERVAL,), daemon=True)
    _state_saver_thread = t
    t.start()

os.makedirs(PERSIST_DIR, exist_ok=True)
load_persistent_state()
load_matches_state()
load_leagues_state()
load_teams_state()
migrate_matches_from_core()
start_state_saver_daemon()
load_state()
bootstrap_tracking_from_state()
_migrate_persistent_state_from_state()
prune_finished_matches_from_tracking()

# -------------------------
# Helper utilities for ignored matches logic (STRICT RULES)
# -------------------------
def is_ignored_match(match_id: int) -> bool:
    with state_lock:
        return int(match_id) in [int(x) for x in state.get("ignored_matches", [])]

def add_ignored_match(match_id: int, reason: str = "") -> None:
    with state_lock:
        ims = state.setdefault("ignored_matches", [])
        if match_id not in ims:
            ims.append(match_id)
            mark_state_dirty()
            logger.info(f"[IGNORED] match_id={match_id} added to ignored_matches reason='{reason}'")


def is_auto_approved_match(match_id: int) -> bool:
    with state_lock:
        approved = state.get("approved_matches_auto", [])
        try:
            mid = int(match_id)
        except Exception:
            return False
        return mid in [int(x) for x in approved]


def add_auto_approved_match(match_id: int) -> None:
    with state_lock:
        approved = state.setdefault("approved_matches_auto", [])
        try:
            mid = int(match_id)
        except Exception:
            return
        existing = [int(x) for x in approved]
        if mid not in existing:
            approved.append(mid)
            mark_state_dirty()


def save_signal_snapshot_state(
    match_id: int,
    header_text: str,
    signal_score_home: int,
    signal_score_away: int,
    signal_minute: int,
    message_id: Optional[int],
    chat_id: Optional[int],
    prob_to75: Optional[float] = None,
    prob_to90: Optional[float] = None,
) -> None:
    with state_lock:
        state.setdefault("signal_header_texts", {})[str(match_id)] = str(header_text or "")
        state.setdefault("signal_snapshot_meta", {})[str(match_id)] = {
            "signal_score_home": int(signal_score_home or 0),
            "signal_score_away": int(signal_score_away or 0),
            "signal_minute": int(signal_minute or 0),
            "message_id": int(message_id) if message_id else None,
            "chat_id": int(chat_id) if chat_id else None,
            "prob_to75": float(prob_to75) if prob_to75 is not None else None,
            "prob_to90": float(prob_to90) if prob_to90 is not None else None,
        }
        mark_state_dirty()


def get_signal_snapshot_state(match_id: int) -> Tuple[Optional[str], Dict[str, Any]]:
    with state_lock:
        header = state.get("signal_header_texts", {}).get(str(match_id))
        meta = state.get("signal_snapshot_meta", {}).get(str(match_id), {})
    if not isinstance(meta, dict):
        meta = {}
    return (str(header) if header else None), dict(meta)

def cleanup_expired_no_stats_fixtures() -> None:
    """
    Очистить записи no_stats_fixtures с истекшим expires_ts.
    Вызывается периодически из main_loop для освобождения памяти.
    """
    if not ENABLE_CLEANUP:
        logger.info("[CLEANUP] disabled (temporary)")
        return
    now = time.time()
    with state_lock:
        no_stats = state.setdefault("no_stats_fixtures", {})
        expired_ids = [fid for fid, info in no_stats.items() if info.get("expires_ts", 0) < now]
        
        if expired_ids:
            for fid in expired_ids:
                del no_stats[fid]
            mark_state_dirty()
            logger.debug(f"[NO_STATS] Cleaned up {len(expired_ids)} expired fixture(s)")

def is_no_stats_fixture(fixture_id: int) -> bool:
    """
    Проверить, заблокирован ли fixture из-за отсутствия статистики.
    Возвращает True если fixture в no_stats и не истек expires_ts.
    """
    now = time.time()
    with state_lock:
        no_stats = state.get("no_stats_fixtures", {})
        info = no_stats.get(str(fixture_id))
        
        if not info:
            return False
        
        expires_ts = info.get("expires_ts", 0)
        if expires_ts < now:
            if not ENABLE_CLEANUP:
                logger.info("[CLEANUP] disabled (temporary)")
                return False
            # Expired - удалить и вернуть False
            del no_stats[str(fixture_id)]
            mark_state_dirty()
            return False
        
        return True

def block_no_stats_fixture(fixture_id: int, minute: int, coverage: float) -> None:
    """
    Заблокировать fixture из-за отсутствия live-статистики.
    Добавить в no_stats_fixtures с expires_ts = now + 6 часов.
    """
    now = time.time()
    expires_ts = now + 6 * 60 * 60  # 6 часов достаточно для завершения матча
    
    with state_lock:
        no_stats = state.setdefault("no_stats_fixtures", {})
        no_stats[str(fixture_id)] = {
            "ts": now,
            "reason": f"coverage={coverage:.2f} at minute={minute}",
            "expires_ts": expires_ts
        }
        mark_state_dirty()
        
        # Лог только при первой блокировке
        expires_str = datetime.fromtimestamp(expires_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        logger.info(f"[NO_STATS] fixture_id={fixture_id} minute={minute} coverage={coverage:.2f} -> blocked until {expires_str}")


def cleanup_expired_no_stats_blocked() -> None:
    """Cleanup long no-stats blocks with expired timestamps."""
    if not ENABLE_CLEANUP:
        logger.info("[CLEANUP] disabled (temporary)")
        return
    now = time.time()
    with state_lock:
        blocked = state.setdefault("no_stats_blocked", {})
        expired = [k for k, info in blocked.items() if info.get("expires_ts", 0) < now]
        if expired:
            for k in expired:
                del blocked[k]
            mark_state_dirty()
            logger.debug(f"[NO_STATS_BLOCK] Cleaned up {len(expired)} expired block(s)")


def is_no_stats_blocked(block_key: str) -> bool:
    """Check if a fixture key is long-blocked due to missing stats."""
    now = time.time()
    with state_lock:
        blocked = state.get("no_stats_blocked", {})
        info = blocked.get(str(block_key))
        if not info:
            return False
        expires_ts = info.get("expires_ts", 0)
        if expires_ts < now:
            if not ENABLE_CLEANUP:
                logger.info("[CLEANUP] disabled (temporary)")
                return False
            del blocked[str(block_key)]
            mark_state_dirty()
            return False
        return True


def block_no_stats_match_long(block_key: str, reason: str, ttl_hours: int = 6) -> None:
    """Long block for fixtures without stats to avoid log spam."""
    now = time.time()
    expires_ts = now + ttl_hours * 60 * 60
    with state_lock:
        blocked = state.setdefault("no_stats_blocked", {})
        blocked[str(block_key)] = {
            "ts": now,
            "reason": reason,
            "expires_ts": expires_ts
        }
        mark_state_dirty()
    expires_str = datetime.fromtimestamp(expires_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    logger.info(f"[NO_STATS_BLOCK] key={block_key} reason={reason} -> blocked until {expires_str}")

# -------------------------
# Rate limiting helpers
# -------------------------
def can_user_request(user_id: int, action: str, now_ts: float, window: int = 60) -> tuple:
    """
    Check if user can make a request for specific action within rate limit window.
    Separate limit for each action (weekly, monthly, instruction, start).
    
    Args:
        user_id: Telegram user ID
        action: Action name ("weekly", "monthly", "instruction", "start")
        now_ts: Current timestamp (typically time.time())
        window: Rate limit window in seconds (default 60)
        
    Returns:
        Tuple (ok, wait_sec) where:
        - ok: True if user can request, False if rate limited
        - wait_sec: Seconds to wait if rate limited, 0 if ok
    """
    with state_lock:
        user_rate_limit = state.setdefault("user_rate_limit", {})
        user_key = str(user_id)
        
        if user_key in user_rate_limit:
            action_data = user_rate_limit[user_key].get(action)
            if action_data:
                last_ts = action_data.get("ts", 0)
                time_since_last = now_ts - last_ts
                
                if time_since_last < window:
                    wait_sec = int(window - time_since_last) + 1
                    return False, wait_sec
        
        return True, 0

def mark_user_request(user_id: int, action: str, now_ts: float) -> None:
    """
    Mark that user has made a request for specific action at the given timestamp.
    Maintains per-action timestamps independently.
    
    Args:
        user_id: Telegram user ID
        action: Action name ("weekly", "monthly", "instruction", "start")
        now_ts: Current timestamp (typically time.time())
    """
    with state_lock:
        user_rate_limit = state.setdefault("user_rate_limit", {})
        user_key = str(user_id)
        
        if user_key not in user_rate_limit:
            user_rate_limit[user_key] = {}
        
        user_rate_limit[user_key][action] = {"ts": now_ts}
        mark_state_dirty()

def _fixture_minute_from_raw(raw: Dict[str,Any]) -> int:
    try:
        r = raw.get("fixture") or raw
        status = r.get("status") or {}
        minute = status.get("elapsed") if isinstance(status, dict) else r.get("elapsed")
        if minute is None:
            return 0
        try:
            return int(minute)
        except Exception:
            try:
                return int(float(minute))
            except Exception:
                return 0
    except Exception:
        return 0

def _has_minimal_key_metrics_for_persistence(fixture_metrics: Dict[str,Any]) -> bool:
    if not fixture_metrics:
        return False
    required = ["total_shots", "shots_on_target", "expected_goals", "corner_kicks", "ball_possession"]
    for side in ("home","away"):
        for k in required:
            v = get_any_metric(fixture_metrics, [k], side)
            try:
                if v is not None and float(v) > 0:
                    return True
            except Exception:
                continue
    return False

# -------------------------
# Telegram helpers
# -------------------------
def _tg_get(method: str, params: Optional[Dict]=None):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/{method}"
    try:
        r = requests.get(url, params=params, timeout=8)
        try:
            return r.status_code, r.json()
        except Exception:
            return r.status_code, {"raw": r.text}
    except Exception:
        return None, None


class TgEditQueue:
    def __init__(self, global_limit_per_minute: int = 30, per_message_seconds: int = 60):
        self.global_limit_per_minute = max(1, int(global_limit_per_minute or 30))
        self.per_message_seconds = max(1, int(per_message_seconds or 60))
        self._lock = threading.RLock()
        self._global_edit_ts: deque[float] = deque()
        self.last_edit_ts: Dict[Tuple[int, int], float] = {}
        self.last_sent_hash: Dict[Tuple[int, int], str] = {}
        self.chat_block_until: Dict[int, float] = {}
        self._last_429_log_at: Dict[int, float] = {}

    @staticmethod
    def _parse_body(response: requests.Response) -> Dict[str, Any]:
        try:
            return response.json()
        except Exception:
            return {"raw": response.text}

    @staticmethod
    def _extract_retry_after(body: Any) -> Optional[int]:
        if not isinstance(body, dict):
            return None
        if int(body.get("error_code") or 0) != 429:
            return None
        params = body.get("parameters") or {}
        retry_after = params.get("retry_after")
        if retry_after is None:
            return None
        try:
            return int(retry_after)
        except Exception:
            return None

    def _wait_global_slot(self) -> None:
        while True:
            with self._lock:
                now_ts = time.time()
                while self._global_edit_ts and (now_ts - self._global_edit_ts[0]) >= 60:
                    self._global_edit_ts.popleft()

                if len(self._global_edit_ts) < self.global_limit_per_minute:
                    self._global_edit_ts.append(now_ts)
                    return

                wait_seconds = 60 - (now_ts - self._global_edit_ts[0])

            time.sleep(max(0.05, wait_seconds))

    def edit_message_text(
        self,
        chat_id: int,
        message_id: int,
        text: str,
        parse_mode: Optional[str] = None,
        reply_markup: Optional[Dict[str, Any]] = None,
        log_prefix: str = "[TG] edit",
        allow_plain_fallback: bool = False,
    ) -> Tuple[bool, str, int]:
        if not chat_id or not message_id:
            return False, "invalid_target", 0

        key = (int(chat_id), int(message_id))
        text_hash = _text_hash(text)
        now_ts = time.time()

        with self._lock:
            blocked_until = float(self.chat_block_until.get(int(chat_id), 0.0) or 0.0)
            if blocked_until > now_ts:
                return False, "chat_frozen", max(1, int(blocked_until - now_ts))

            prev_hash = self.last_sent_hash.get(key)
            if prev_hash == text_hash:
                return False, "unchanged", 0

            last_ts = float(self.last_edit_ts.get(key, 0.0) or 0.0)
            elapsed = now_ts - last_ts
            if elapsed < self.per_message_seconds:
                return False, "per_message_rate", int(self.per_message_seconds - elapsed)

        self._wait_global_slot()

        payload: Dict[str, Any] = {
            "chat_id": int(chat_id),
            "message_id": int(message_id),
            "text": text,
        }
        if parse_mode:
            payload["parse_mode"] = parse_mode
        if reply_markup is not None:
            payload["reply_markup"] = json.dumps(reply_markup)

        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/editMessageText"

        try:
            r = requests.post(url, data=payload, timeout=10)
            body = self._parse_body(r)

            if r.ok and body.get("ok"):
                with self._lock:
                    now_ok = time.time()
                    self.last_edit_ts[key] = now_ok
                    self.last_sent_hash[key] = text_hash
                    self.chat_block_until.pop(int(chat_id), None)
                return True, "ok", 0

            desc = str(body.get("description", "")) if isinstance(body, dict) else str(body)
            desc_l = desc.lower()

            if "message is not modified" in desc_l:
                with self._lock:
                    self.last_sent_hash[key] = text_hash
                return False, "not_modified", 0

            if "message to edit not found" in desc_l or "message not found" in desc_l:
                return False, "not_found", 0

            retry_after = self._extract_retry_after(body)
            if retry_after is not None:
                block_until = time.time() + max(1, int(retry_after))
                with self._lock:
                    self.chat_block_until[int(chat_id)] = block_until
                    last_log = float(self._last_429_log_at.get(int(chat_id), 0.0) or 0.0)
                    if (time.time() - last_log) > 1.0:
                        logger.warning(
                            "%s 429 Too Many Requests: chat_id=%s retry_after=%ss (chat frozen)",
                            log_prefix,
                            chat_id,
                            retry_after,
                        )
                        self._last_429_log_at[int(chat_id)] = time.time()
                return False, "rate_limit", max(1, int(retry_after))

            if allow_plain_fallback and ("can't parse entities" in desc_l or "entity" in desc_l):
                payload_plain = {
                    "chat_id": int(chat_id),
                    "message_id": int(message_id),
                    "text": text,
                }
                r2 = requests.post(url, data=payload_plain, timeout=10)
                body2 = self._parse_body(r2)
                if r2.ok and body2.get("ok"):
                    with self._lock:
                        now_ok = time.time()
                        self.last_edit_ts[key] = now_ok
                        self.last_sent_hash[key] = text_hash
                        self.chat_block_until.pop(int(chat_id), None)
                    return True, "ok", 0

            return False, "error", 0
        except Exception:
            logger.exception("%s exception for chat_id=%s message_id=%s", log_prefix, chat_id, message_id)
            return False, "error", 0

    def edit_message_caption(
        self,
        chat_id: int,
        message_id: int,
        caption: str,
        parse_mode: Optional[str] = None,
        reply_markup: Optional[Dict[str, Any]] = None,
        log_prefix: str = "[TG] edit_caption",
        allow_plain_fallback: bool = False,
    ) -> Tuple[bool, str, int]:
        if not chat_id or not message_id:
            return False, "invalid_target", 0

        key = (int(chat_id), int(message_id))
        caption_hash = _text_hash(caption)
        now_ts = time.time()

        with self._lock:
            blocked_until = float(self.chat_block_until.get(int(chat_id), 0.0) or 0.0)
            if blocked_until > now_ts:
                return False, "chat_frozen", max(1, int(blocked_until - now_ts))

            prev_hash = self.last_sent_hash.get(key)
            if prev_hash == caption_hash:
                return False, "unchanged", 0

            last_ts = float(self.last_edit_ts.get(key, 0.0) or 0.0)
            elapsed = now_ts - last_ts
            if elapsed < self.per_message_seconds:
                return False, "per_message_rate", int(self.per_message_seconds - elapsed)

        self._wait_global_slot()

        payload: Dict[str, Any] = {
            "chat_id": int(chat_id),
            "message_id": int(message_id),
            "caption": caption,
        }
        if parse_mode:
            payload["parse_mode"] = parse_mode
        if reply_markup is not None:
            payload["reply_markup"] = json.dumps(reply_markup)

        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/editMessageCaption"

        try:
            r = requests.post(url, data=payload, timeout=10)
            body = self._parse_body(r)

            if r.ok and body.get("ok"):
                with self._lock:
                    now_ok = time.time()
                    self.last_edit_ts[key] = now_ok
                    self.last_sent_hash[key] = caption_hash
                    self.chat_block_until.pop(int(chat_id), None)
                return True, "ok", 0

            desc = str(body.get("description", "")) if isinstance(body, dict) else str(body)
            desc_l = desc.lower()

            if "message is not modified" in desc_l:
                with self._lock:
                    self.last_sent_hash[key] = caption_hash
                return False, "not_modified", 0

            if "message to edit not found" in desc_l or "message not found" in desc_l:
                return False, "not_found", 0

            retry_after = self._extract_retry_after(body)
            if retry_after is not None:
                block_until = time.time() + max(1, int(retry_after))
                with self._lock:
                    self.chat_block_until[int(chat_id)] = block_until
                    last_log = float(self._last_429_log_at.get(int(chat_id), 0.0) or 0.0)
                    if (time.time() - last_log) > 1.0:
                        logger.warning(
                            "%s 429 Too Many Requests: chat_id=%s retry_after=%ss (chat frozen)",
                            log_prefix,
                            chat_id,
                            retry_after,
                        )
                        self._last_429_log_at[int(chat_id)] = time.time()
                return False, "rate_limit", max(1, int(retry_after))

            if allow_plain_fallback and ("can't parse entities" in desc_l or "entity" in desc_l):
                payload_plain = {
                    "chat_id": int(chat_id),
                    "message_id": int(message_id),
                    "caption": caption,
                }
                r2 = requests.post(url, data=payload_plain, timeout=10)
                body2 = self._parse_body(r2)
                if r2.ok and body2.get("ok"):
                    with self._lock:
                        now_ok = time.time()
                        self.last_edit_ts[key] = now_ok
                        self.last_sent_hash[key] = caption_hash
                        self.chat_block_until.pop(int(chat_id), None)
                    return True, "ok", 0

            return False, "error", 0
        except Exception:
            logger.exception("%s exception for chat_id=%s message_id=%s", log_prefix, chat_id, message_id)
            return False, "error", 0


tg_edit_queue = TgEditQueue(
    global_limit_per_minute=TG_EDIT_GLOBAL_LIMIT_PER_MIN,
    per_message_seconds=TG_EDIT_PER_MESSAGE_SECONDS,
)

def validate_telegram_target():
    """
    Validate Telegram configuration.
    Returns True if basic validation passes, False otherwise.
    IMPORTANT: Does NOT exit on failure - bot continues with warnings.
    """
    ok = True
    
    # Check bot token
    status, body = _tg_get("getMe")
    if status != 200 or not body or not body.get("ok"):
        logger.error("[TG] getMe failed. Check TELEGRAM_TOKEN.")
        ok = False
    else:
        bot_username = body.get("result", {}).get("username", "unknown")
        logger.info(f"[TG] Bot validated: @{bot_username}")
    
    # Check chat_id
    if not TELEGRAM_CHAT_ID:
        logger.error("[TG] TELEGRAM_CHAT_ID not set.")
        ok = False
    else:
        # Try getChat with numeric chat_id
        status, body = _tg_get("getChat", {"chat_id": TELEGRAM_CHAT_ID})
        if status != 200 or not body or not body.get("ok"):
            error_desc = body.get("description", "") if body else ""
            logger.warning(f"[TG] getChat failed for chat_id={TELEGRAM_CHAT_ID}: {status} {error_desc}")
            
            # Provide helpful hints based on error type
            if "chat not found" in error_desc.lower():
                logger.warning("[TG] HINT: Проверь, что бот добавлен администратором в канал/группу и имеет права на публикацию/редактирование")
            elif "forbidden" in error_desc.lower():
                logger.warning("[TG] HINT: Бот не имеет прав доступа к каналу. Добавь бота как администратора.")
            
            # Don't fail validation - continue with warning
            logger.warning("[TG] Chat validation failed, but bot will continue (messages may fail until fixed)")
        else:
            chat_title = body.get("result", {}).get("title", "unknown")
            chat_type = body.get("result", {}).get("type", "unknown")
            logger.info(f"[TG] Target chat validated: '{chat_title}' (type: {chat_type}, id: {TELEGRAM_CHAT_ID})")
    
    return ok

def send_to_telegram(
    text: str,
    match_id: Optional[int] = None,
    score_at_signal: Optional[Tuple[int, int]] = None,
    signal_minute: Optional[int] = None,
) -> Optional[int]:
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"}
    
    # Log details before sending
    logger.info(f"[TG] Sending message: chat_id={TELEGRAM_CHAT_ID}, match_id={match_id}, text_preview={text[:100]}...")
    
    try:
        r = requests.post(url, data=payload, timeout=10)
        try:
            body = r.json()
        except Exception:
            body = {"raw": r.text}
        if r.ok and body.get("ok"):
            mid = body.get("result", {}).get("message_id")
            if mid:
                try:
                    tg_edit_queue.last_sent_hash[(int(TELEGRAM_CHAT_ID), int(mid))] = _text_hash(text)
                except Exception:
                    pass
            if match_id is not None and mid:
                score_home = 0
                score_away = 0
                if isinstance(score_at_signal, (tuple, list)) and len(score_at_signal) >= 2:
                    try:
                        score_home = int(score_at_signal[0])
                        score_away = int(score_at_signal[1])
                    except Exception:
                        score_home, score_away = 0, 0
                else:
                    with state_lock:
                        init = state.get("match_initial_score", {}).get(str(match_id), (0, 0))
                    try:
                        score_home = int(init[0])
                        score_away = int(init[1])
                    except Exception:
                        score_home, score_away = 0, 0

                add_tracked_match(
                    fixture_id=int(match_id),
                    message_id=int(mid),
                    chat_id=int(TELEGRAM_CHAT_ID),
                    score_home=score_home,
                    score_away=score_away,
                    status="LIVE",
                    sent_at_ts=time.time(),
                    signal_minute=int(signal_minute or 0),
                )

                with state_lock:
                    state.setdefault("sent_matches", {})[str(match_id)] = int(mid)
                    state.setdefault("last_message_texts", {})[str(match_id)] = text
                    snapshot_meta = state.setdefault("signal_snapshot_meta", {}).get(str(match_id))
                    if isinstance(snapshot_meta, dict):
                        snapshot_meta["message_id"] = int(mid)
                        snapshot_meta["chat_id"] = int(TELEGRAM_CHAT_ID)
                    # Save signal timestamp (UTC timezone-aware) and date
                    # Date is determined ONLY by signal send time, regardless of match start time
                    signal_timestamp = datetime.now(timezone.utc).timestamp()
                    signal_date = get_msk_date()
                    msk_time = get_msk_datetime().strftime("%H:%M:%S")
                    signal_minute_value = int(signal_minute or 0)
                    if signal_minute_value > 0:
                        state.setdefault("match_initial_minute", {})[str(match_id)] = signal_minute_value
                    state.setdefault("match_signal_info", {})[str(match_id)] = {
                        "signal_timestamp": signal_timestamp,
                        "signal_date": signal_date,
                        "is_finished": False
                    }
                    logger.info(f"[STATS] Match {match_id}: signal sent at {msk_time} MSK, assigned to date {signal_date}")
                    if match_id not in state.setdefault("monitored_matches", []):
                        state["monitored_matches"].append(match_id)
                mark_state_dirty()
                update_persistent_fixture_text_hash(match_id, text)
            logger.info(f"[TG] Sent message id={mid} for match {match_id}")
            return int(mid) if mid else None
        else:
            logger.warning(f"[TG] send failed: {body}")
            return None
    except Exception:
        logger.exception("[TG] send exception")
        return None


def validate_match_context_before_send(data: Dict[str, Any]) -> bool:
    """Validate fixture context before sending any signal to Telegram."""
    if not isinstance(data, dict):
        logger.warning("[PRE_SEND_BLOCK] Invalid match context, skip signal send reason=no data")
        return False

    fixture = data.get("fixture") or {}
    home_name = normalize_team_name(fixture.get("team_home_name"))
    away_name = normalize_team_name(fixture.get("team_away_name"))
    league_name = str(_unwrap_value(fixture.get("league_name")) or "").strip()
    league_id = _unwrap_metric_int(fixture.get("league_id"))
    home_team_id = _unwrap_metric_int(fixture.get("team_home_id"))
    away_team_id = _unwrap_metric_int(fixture.get("team_away_id"))

    reason = None
    if not home_name or home_name == "Home":
        reason = "invalid home team"
    elif not away_name or away_name == "Away":
        reason = "invalid away team"
    elif not league_name:
        reason = "missing league name"
    elif league_id is None:
        reason = "missing league id"
    elif home_team_id is None:
        reason = "missing home team id"
    elif away_team_id is None:
        reason = "missing away team id"
    else:
        team_boosts = calc_match_team_boost_v2(fixture)
        if int(team_boosts.get("home_matches", 0)) == 0 and int(team_boosts.get("away_matches", 0)) == 0 and str(team_boosts.get("league_factor_source") or "default") == "default":
            reason = "fallback league/team context"

    if reason:
        logger.warning("[PRE_SEND_BLOCK] Invalid match context, skip signal send reason=%s", reason)
        return False

    return True


def edit_telegram_message_improved(text: str, match_id: int) -> bool:
    with state_lock:
        message_id = state.get("sent_matches", {}).get(str(match_id))
        prev = state.get("last_message_texts", {}).get(str(match_id))
    if not message_id:
        mid = send_to_telegram(text, match_id=match_id)
        return bool(mid)
    if prev == text:
        return False
    
    # Log details before editing
    logger.info(f"[TG] Editing message: chat_id={TELEGRAM_CHAT_ID}, message_id={message_id}, match_id={match_id}, text_preview={text[:100]}...")
    
    try:
        ok, status, retry_after = tg_edit_queue.edit_message_text(
            chat_id=int(TELEGRAM_CHAT_ID),
            message_id=int(message_id),
            text=text,
            parse_mode="Markdown",
            log_prefix="[TG] edit",
            allow_plain_fallback=True,
        )
        if ok:
            with state_lock:
                state.setdefault("last_message_texts", {})[str(match_id)] = text
            mark_state_dirty()
            update_persistent_fixture_text_hash(match_id, text)
            logger.info(f"[TG] Edited message for match {match_id}")
            return True

        if status in ("unchanged", "not_modified", "per_message_rate", "chat_frozen"):
            return False

        if status == "invalid_target":
            mid = send_to_telegram(text, match_id=match_id)
            return bool(mid)

        if status == "not_found":
            mid = send_to_telegram(text, match_id=match_id)
            return bool(mid)

        if status == "rate_limit":
            logger.info("[TG] edit deferred by rate limit for match=%s retry_after=%ss", match_id, retry_after)
            return False

        logger.warning("[TG] edit failed for match=%s status=%s", match_id, status)
        return False
    except Exception:
        logger.exception("[TG] edit exception")
        return False

# -------------------------
# REVIEW Mode (Manual Moderation for Borderline Signals)
# -------------------------
def normalize_league_name(x: Any) -> str:
    if isinstance(x, dict):
        return str(x.get("value") or x.get("name") or "").strip()
    return str(x).strip() if x is not None else ""


def normalize_team_name(x: Any) -> str:
    if isinstance(x, dict):
        return str(x.get("name") or x.get("value") or "").strip()
    return str(x).strip() if x is not None else ""


def apply_league_factor(base_prob: float, factor: float) -> float:
    """
    Apply league factor to probability using odds transformation.
    
    This ensures probability stays in valid range and doesn't break the scale.
    Formula:
        p = base_prob/100
        odds = p/(1-p)
        odds_adj = odds * factor
        p_adj = odds_adj/(1+odds_adj)
        final_prob = p_adj*100
    
    Args:
        base_prob: Base probability in percent (0-100)
        factor: League multiplier factor
    
    Returns:
        float: Adjusted probability in percent (0-100)
    """
    # Protection: handle edge cases
    if base_prob <= 0:
        return 0.0
    if base_prob >= 99.9:
        return 99.9
    
    # Convert percent to probability
    p = base_prob / 100.0
    
    # Convert to odds
    odds = p / (1.0 - p)
    
    # Apply factor
    odds_adj = odds * factor
    
    # Convert back to probability
    p_adj = odds_adj / (1.0 + odds_adj)
    
    # Convert to percent
    final_prob = p_adj * 100.0
    
    # Ensure valid range
    final_prob = max(0.0, min(99.9, final_prob))
    
    return final_prob


def _unwrap_value(v: Any) -> Any:
    if isinstance(v, dict) and "value" in v:
        return v.get("value")
    return v


def fmt_prob(x: Any) -> str:
    try:
        if x is None or x == "" or x == "N/A":
            return "N/A"
        s = f"{float(x):.1f}"
        return s.replace(".", ",")
    except Exception:
        return "N/A"


def fmt2(x: Any) -> str:
    try:
        if x is None or x == "" or x == "N/A":
            return "N/A"
        return f"{float(x):.2f}"
    except Exception:
        return "N/A"


def _apply_intensity_scaling(base_prob01: float, combined_m: float, cap: float = FINAL_PROB_CAP) -> float:
    """Apply v2 intensity scaling to a base probability in [0..1]."""
    p = clamp(float(base_prob01 or 0.0), 0.0, 1.0)
    m = max(0.0001, float(combined_m or 1.0))
    boosted = 1.0 - ((1.0 - p) ** m)
    return min(float(cap), clamp(boosted, 0.0, 1.0))


def _get_v2_league_context(fixture_metrics: Dict[str, Any]) -> Dict[str, Any]:
    league_id = _unwrap_metric_int(fixture_metrics.get("league_id"))
    league_rec = get_persisted_league_record(league_id) or {}

    league_avg_goals_raw = league_rec.get("avg_goals")
    try:
        league_avg_goals = float(league_avg_goals_raw) if league_avg_goals_raw not in (None, "", "N/A") else None
    except Exception:
        league_avg_goals = None

    has_persist_avg = bool(league_avg_goals is not None and league_avg_goals > 0)
    if not has_persist_avg:
        league_avg_goals = float(GLOBAL_GOALS_BASE)

    league_factor = 1.0
    if has_persist_avg:
        league_factor = clamp(float(league_avg_goals) / float(GLOBAL_GOALS_BASE), 0.85, 1.20)

    league_avg_team = max(0.001, float(league_avg_goals) / 2.0)

    return {
        "league_id": league_id,
        "league_avg_goals": float(league_avg_goals),
        "league_factor": float(league_factor),
        "league_avg_team": float(league_avg_team),
        "league_factor_source": "persist" if has_persist_avg else "default",
    }


def _get_v2_team_profile(team_id: Optional[int], league_avg_team: float) -> Dict[str, Any]:
    team_rec = get_persisted_team_record(team_id) if team_id is not None else None
    team_rec = team_rec if isinstance(team_rec, dict) else {}

    try:
        matches_count = int(team_rec.get("matches_used") or 0)
    except Exception:
        matches_count = 0

    avg_scored_raw = team_rec.get("avg_scored")
    avg_conceded_raw = team_rec.get("avg_conceded")

    try:
        avg_scored = float(avg_scored_raw) if avg_scored_raw not in (None, "", "N/A") else None
    except Exception:
        avg_scored = None
    try:
        avg_conceded = float(avg_conceded_raw) if avg_conceded_raw not in (None, "", "N/A") else None
    except Exception:
        avg_conceded = None

    if matches_count <= 0 or avg_scored is None or avg_conceded is None:
        gf_used = float(league_avg_team)
        ga_used = float(league_avg_team)
        attack_factor = 1.0
        defense_factor = 1.0
    else:
        goals_for_total = float(avg_scored) * float(matches_count)
        goals_against_total = float(avg_conceded) * float(matches_count)
        denom = float(SHRINK_K + matches_count)
        gf_used = (float(SHRINK_K) * float(league_avg_team) + goals_for_total) / denom
        ga_used = (float(SHRINK_K) * float(league_avg_team) + goals_against_total) / denom
        attack_factor = clamp(float(gf_used) / float(league_avg_team), 0.85, 1.20)
        defense_factor = clamp(float(ga_used) / float(league_avg_team), 0.85, 1.20)

    return {
        "matches_count": int(matches_count),
        "avg_scored_raw": avg_scored,
        "avg_conceded_raw": avg_conceded,
        "gf_used": float(gf_used),
        "ga_used": float(ga_used),
        "attack_factor": float(attack_factor),
        "defense_factor": float(defense_factor),
    }


def calc_match_team_boost_v2(fixture_metrics: Dict[str, Any]) -> Dict[str, Any]:
    """Compute v2 factors from persist leagues/teams only."""
    home_name = normalize_team_name(fixture_metrics.get("team_home_name"))
    away_name = normalize_team_name(fixture_metrics.get("team_away_name"))
    home_team_id = _unwrap_metric_int(fixture_metrics.get("team_home_id"))
    away_team_id = _unwrap_metric_int(fixture_metrics.get("team_away_id"))

    league_ctx = _get_v2_league_context(fixture_metrics)
    league_avg_team = float(league_ctx["league_avg_team"])

    home_profile = _get_v2_team_profile(home_team_id, league_avg_team)
    away_profile = _get_v2_team_profile(away_team_id, league_avg_team)

    team_mix_raw = (
        float(home_profile["attack_factor"]) * float(away_profile["defense_factor"]) +
        float(away_profile["attack_factor"]) * float(home_profile["defense_factor"])
    ) / 2.0
    team_mix_factor = clamp(float(team_mix_raw), 0.85, 1.20)

    combined_m = math.exp(
        float(WL) * math.log(max(1e-9, float(league_ctx["league_factor"]))) +
        float(WT) * math.log(max(1e-9, float(team_mix_factor)))
    )
    combined_m = clamp(float(combined_m), 0.85, 1.30)

    return {
        "home_team": home_name,
        "away_team": away_name,
        "league_id": league_ctx["league_id"],
        "league_avg_goals": float(league_ctx["league_avg_goals"]),
        "league_factor": float(league_ctx["league_factor"]),
        "league_avg_team": float(league_avg_team),
        "league_factor_source": league_ctx["league_factor_source"],
        "home_matches": int(home_profile["matches_count"]),
        "home_avg_scored_raw": home_profile["avg_scored_raw"],
        "home_avg_conceded_raw": home_profile["avg_conceded_raw"],
        "home_gf_used": float(home_profile["gf_used"]),
        "home_ga_used": float(home_profile["ga_used"]),
        "home_attack_factor": float(home_profile["attack_factor"]),
        "home_defense_factor": float(home_profile["defense_factor"]),
        "away_matches": int(away_profile["matches_count"]),
        "away_avg_scored_raw": away_profile["avg_scored_raw"],
        "away_avg_conceded_raw": away_profile["avg_conceded_raw"],
        "away_gf_used": float(away_profile["gf_used"]),
        "away_ga_used": float(away_profile["ga_used"]),
        "away_attack_factor": float(away_profile["attack_factor"]),
        "away_defense_factor": float(away_profile["defense_factor"]),
        "team_mix_factor": float(team_mix_factor),
        "combined_m": float(combined_m),
        # Legacy-compatible aliases
        "home_boost": float(home_profile["attack_factor"]),
        "away_boost": float(away_profile["attack_factor"]),
        "match_boost": float(combined_m),
    }

    print(f"[DEBUG_FACTORS] league_factor={league_ctx['league_factor']} team_mix_factor={team_mix_factor} combined_M={combined_m}")

    return {
        "home_team": home_name,
        "away_team": away_name,
        "league_id": league_ctx["league_id"],
        "league_avg_goals": float(league_ctx["league_avg_goals"]),
        "league_factor": float(league_ctx["league_factor"]),
        "league_avg_team": float(league_avg_team),
        "league_factor_source": league_ctx["league_factor_source"],
        "home_matches": int(home_profile["matches_count"]),
        "away_matches": int(away_profile["matches_count"]),
        "home_avg_scored": float(home_profile["avg_scored"]),
        "home_avg_conceded": float(home_profile["avg_conceded"]),
        "away_avg_scored": float(away_profile["avg_scored"]),
        "away_avg_conceded": float(away_profile["avg_conceded"]),
        "home_gf_used": float(home_profile["gf_used"]),
        "home_ga_used": float(home_profile["ga_used"]),
        "away_gf_used": float(away_profile["gf_used"]),
        "away_ga_used": float(away_profile["ga_used"]),
        "home_attack_factor": float(home_profile["attack_factor"]),
        "home_defense_factor": float(home_profile["defense_factor"]),
        "away_attack_factor": float(away_profile["attack_factor"]),
        "away_defense_factor": float(away_profile["defense_factor"]),
        "team_mix_factor": float(team_mix_factor),
        "combined_m": float(combined_m),
        # Legacy-compatible aliases
        "home_boost": float(home_profile["attack_factor"]),
        "away_boost": float(away_profile["attack_factor"]),
        "match_boost": float(combined_m),
    }


def fmt4(x: Any) -> str:
    try:
        if x is None or x == "" or x == "N/A":
            return "N/A"
        return str(int(float(x)))
    except Exception:
        return "N/A"


def get_color(metric_name: str, value: Any, minute: int) -> str:
    """
    Determine color indicator (emoji) for review metrics based on thresholds and match minute.
    
    Args:
        metric_name: Name of the metric (pressure_index, xg_delta, shots_ratio, save_stress, possession_pressure)
        value: Numeric value of the metric
        minute: Current match minute
        
    Returns:
        Color emoji: 🟢 (green), 🟡 (yellow), or 🔴 (red)
    """
    try:
        val = float(value) if value not in (None, "", "N/A") else 0.0
    except Exception:
        return "🔴"
    
    # Determine EARLY (15-30) or MID (31-45) phase
    is_early = 15 <= minute <= 30
    is_mid = 31 <= minute <= 45
    
    # PRESSURE_INDEX
    if metric_name == "pressure_index":
        if is_early:
            if val >= 18:
                return "🟢"
            elif val >= 14:
                return "🟡"
            else:
                return "🔴"
        elif is_mid:
            if val >= 20:
                return "🟢"
            elif val >= 16:
                return "🟡"
            else:
                return "🔴"
    
    # XG_DELTA (use absolute value)
    elif metric_name == "xg_delta":
        abs_val = abs(val)
        if is_early:
            if abs_val >= 0.60:
                return "🟢"
            elif abs_val >= 0.30:
                return "🟡"
            else:
                return "🔴"
        elif is_mid:
            if abs_val >= 0.50:
                return "🟢"
            elif abs_val >= 0.25:
                return "🟡"
            else:
                return "🔴"
    
    # SHOTS_RATIO
    elif metric_name == "shots_ratio":
        if val >= 0.75:
            return "🟢"
        elif val >= 0.40:
            return "🟡"
        else:
            return "🔴"
    
    # SAVE_STRESS
    elif metric_name == "save_stress":
        if val >= 0.60:
            return "🟢"
        elif val >= 0.30:
            return "🟡"
        else:
            return "🔴"
    
    # POSSESSION_PRESSURE
    elif metric_name == "possession_pressure":
        if is_early:
            if val >= 0.20:
                return "🟢"
            elif val >= 0.10:
                return "🟡"
            else:
                return "🔴"
        elif is_mid:
            if val >= 0.15:
                return "🟢"
            elif val >= 0.08:
                return "🟡"
            else:
                return "🔴"
    
    # Default to red if metric not recognized
    return "🔴"


def get_league_color(league_name: str, league_avg: Optional[float]) -> Tuple[str, str]:
    """
    Determine color indicator for league based on average goals.
    
    Args:
        league_name: Name of the league
        
    Returns:
        Tuple of (emoji, display_text) where emoji is 🟢 (green), 🟡 (yellow), or 🔴 (red)
    """
    avg = league_avg
    
    if avg is None:
        return "🟡", f"{league_name} (нет в базе)"
    
    if avg >= 3.0:
        return "🟢", league_name
    elif avg >= 2.5:
        return "🟡", league_name
    else:
        return "🔴", league_name


def _extract_minute_for_match_score(live_data: Dict[str, Any]) -> int:
    """Extract elapsed minute from live fixture safely."""
    try:
        elapsed = get_metric_from_fixture(live_data, "elapsed", None)
        if elapsed is None:
            elapsed = get_metric_from_fixture(live_data, "minute", 0)
        return max(0, int(float(elapsed or 0)))
    except Exception:
        return 0


def compute_match_score_v2(live_data: Dict[str, Any], league_factor: Any) -> Tuple[float, int, Dict[str, float]]:
    """
    Compute robust v2 match score.

    Returns:
        score01_final (0..1), match_score (0..12), metrics dict for logging.
    """
    fixture = live_data or {}

    fixture_id = get_fixture_id(fixture)
    minute = _extract_minute_for_match_score(fixture)

    xg_home = float(get_any_metric(fixture, ["expected_goals"], "home") or 0.0)
    xg_away = float(get_any_metric(fixture, ["expected_goals"], "away") or 0.0)
    if xg_home == 0.0:
        xg_home = float(estimate_xg_from_metrics_combined(fixture, "home") or 0.0)
    if xg_away == 0.0:
        xg_away = float(estimate_xg_from_metrics_combined(fixture, "away") or 0.0)

    shots_home = float(get_any_metric(fixture, ["total_shots"], "home") or 0.0)
    shots_away = float(get_any_metric(fixture, ["total_shots"], "away") or 0.0)
    shots_on_target_home = float(get_any_metric(fixture, ["shots_on_target"], "home") or 0.0)
    shots_on_target_away = float(get_any_metric(fixture, ["shots_on_target"], "away") or 0.0)
    shots_in_box_home = float(get_any_metric(fixture, ["shots_insidebox", "shots_inside_box", "inside_box"], "home") or 0.0)
    shots_in_box_away = float(get_any_metric(fixture, ["shots_insidebox", "shots_inside_box", "inside_box"], "away") or 0.0)
    corners_home = float(get_any_metric(fixture, ["corner_kicks", "corners"], "home") or 0.0)
    corners_away = float(get_any_metric(fixture, ["corner_kicks", "corners"], "away") or 0.0)
    saves_home = float(get_any_metric(fixture, ["saves", "goalkeeper_saves"], "home") or 0.0)
    saves_away = float(get_any_metric(fixture, ["saves", "goalkeeper_saves"], "away") or 0.0)
    current_home_score = float(get_metric_from_fixture(fixture, "score_home", 0) or 0.0)
    current_away_score = float(get_metric_from_fixture(fixture, "score_away", 0) or 0.0)
    possession_home = float(get_any_metric(fixture, ["ball_possession", "passes_%"], "home") or 0.0)
    possession_away = float(get_any_metric(fixture, ["ball_possession", "passes_%"], "away") or 0.0)

    # keep signature compatibility; v2.5 uses combined_M context instead of league_component
    _ = league_factor

    pressure_index = float(calculate_pressure_index(fixture) or 0.0)
    pressure_index_norm = clamp(pressure_index / 25.0, 0.0, 1.0)

    xg_total = float(max(0.0, xg_home + xg_away))
    xg_delta = float(xg_home - xg_away)
    xg_total_norm = clamp(xg_total / 1.6, 0.0, 1.0)
    xg_balance_norm = clamp(1.0 - min(abs(xg_delta) / 0.8, 1.0), 0.0, 1.0)
    xg_component = 0.7 * xg_total_norm + 0.3 * xg_balance_norm

    shots_ratio = (float(shots_on_target_home) + 1.0) / (float(shots_on_target_away) + 1.0)
    shots_ratio_norm = clamp(shots_ratio / 1.5, 0.0, 1.0)

    save_stress = calculate_save_stress(
        saves_home=saves_home,
        saves_away=saves_away,
        current_home_score=current_home_score,
        current_away_score=current_away_score,
        shots_on_target_home=shots_on_target_home,
        shots_on_target_away=shots_on_target_away
    )
    save_stress_norm = clamp(save_stress, 0.0, 1.0)

    possession_diff = abs(float(possession_home) - float(possession_away))
    if possession_diff > 1.0:
        possession_pressure = possession_diff / 100.0
    else:
        possession_pressure = possession_diff
    possession_pressure_norm = clamp(possession_pressure, 0.0, 1.0)

    total_shots = max(0.0, float(shots_home) + float(shots_away))
    total_sot = max(0.0, float(shots_on_target_home) + float(shots_on_target_away))
    volume_gate = min(1.0, total_shots / 8.0) * min(1.0, total_sot / 3.0)

    if 15 <= minute <= 25:
        live_score = (
            0.32 * pressure_index_norm +
            0.20 * xg_component +
            0.18 * shots_ratio_norm +
            0.12 * save_stress_norm +
            0.08 * possession_pressure_norm +
            0.10 * volume_gate
        )
    elif 26 <= minute <= 45:
        live_score = (
            0.28 * pressure_index_norm +
            0.26 * xg_component +
            0.18 * shots_ratio_norm +
            0.14 * save_stress_norm +
            0.08 * possession_pressure_norm +
            0.06 * volume_gate
        )
    else:
        live_score = 0.0

    live_score = clamp(live_score, 0.0, 1.0)

    boost_ctx = calc_match_team_boost_v2(fixture)
    combined_m = float(boost_ctx.get("combined_m", 1.0) or 1.0)

    context_gate = clamp((live_score - 0.35) / 0.25, 0.0, 1.0)
    effective_context = 1.0 + (combined_m - 1.0) * context_gate

    final_score = live_score * effective_context
    final_score = clamp(final_score, 0.0, 1.0)

    match_score = round(final_score * 12)
    match_score = int(clamp(match_score, 0, 12))

    metrics = {
        "fixture_id": float(fixture_id) if fixture_id is not None else -1.0,
        "minute": float(minute),
        "pressure_norm": float(pressure_index_norm),
        "xg_total": float(xg_total),
        "xg_total_norm": float(xg_total_norm),
        "xg_balance_norm": float(xg_balance_norm),
        "xg_component": float(xg_component),
        "shots_ratio_norm": float(shots_ratio_norm),
        "save_stress_norm": float(save_stress_norm),
        "possession_pressure_norm": float(possession_pressure_norm),
        "volume_gate": float(volume_gate),
        "live_score": float(live_score),
        "context_gate": float(context_gate),
        "combined_m": float(combined_m),
        "effective_context": float(effective_context),
        "score01_final": float(final_score),
        "match_score": float(match_score),
    }

    logger.info(
        f"[MATCH_SCORE_V25] "
        f"minute={minute} "
        f"pressure_norm={pressure_index_norm:.3f} "
        f"xg_total={xg_total:.3f} "
        f"xg_total_norm={xg_total_norm:.3f} "
        f"xg_balance_norm={xg_balance_norm:.3f} "
        f"xg_component={xg_component:.3f} "
        f"shots_ratio_norm={shots_ratio_norm:.3f} "
        f"save_stress_norm={save_stress_norm:.3f} "
        f"possession_pressure_norm={possession_pressure_norm:.3f} "
        f"volume_gate={volume_gate:.3f} "
        f"live_score={live_score:.3f} "
        f"context_gate={context_gate:.3f} "
        f"combined_M={combined_m:.3f} "
        f"effective_context={effective_context:.3f} "
        f"final_score={final_score:.3f} "
        f"match_score={match_score}"
    )

    return final_score, match_score, metrics


def calculate_signal_score(league_avg: Optional[float], pressure_index: float, xg_delta: float, shots_ratio: float, save_stress: float, possession_pressure: float) -> int:
    """
    Calculate total signal score based on multiple metrics.
    
    Args:
        league_avg: Average goals for league from persist (if available)
        pressure_index: Pressure index value
        xg_delta: Expected goals difference
        shots_ratio: Shots ratio value
        save_stress: Save stress value
        possession_pressure: Possession pressure value
        
    Returns:
        Total score (0-12)
    """
    score = 0
    
    try:
        pressure_index = float(pressure_index) if pressure_index not in (None, "", "N/A") else 0.0
    except Exception:
        pressure_index = 0.0
    
    try:
        xg_delta = float(xg_delta) if xg_delta not in (None, "", "N/A") else 0.0
    except Exception:
        xg_delta = 0.0
    
    try:
        shots_ratio = float(shots_ratio) if shots_ratio not in (None, "", "N/A") else 0.0
    except Exception:
        shots_ratio = 0.0
    
    try:
        save_stress = float(save_stress) if save_stress not in (None, "", "N/A") else 0.0
    except Exception:
        save_stress = 0.0
    
    try:
        possession_pressure = float(possession_pressure) if possession_pressure not in (None, "", "N/A") else 0.0
    except Exception:
        possession_pressure = 0.0
    
    # --- Лига ---
    avg = league_avg
    if avg is not None:
        if avg >= 3.0:
            score += 2
        elif avg >= 2.5:
            score += 1
    
    # --- Давление ---
    if pressure_index >= 20:
        score += 2
    elif pressure_index >= 15:
        score += 1
    
    # --- xg_delta ---
    if abs(xg_delta) >= 0.6:
        score += 2
    elif abs(xg_delta) >= 0.3:
        score += 1
    
    # --- shots_ratio ---
    if shots_ratio >= 0.5:
        score += 2
    elif shots_ratio >= 0.3:
        score += 1
    
    # --- save_stress ---
    if save_stress >= 0.7:
        score += 2
    elif save_stress >= 0.4:
        score += 1
    
    # --- possession_pressure ---
    if possession_pressure >= 0.15:
        score += 2
    elif possession_pressure >= 0.07:
        score += 1
    
    return score


def get_signal_rating(score: int) -> str:
    """
    Convert signal score to rating text.
    
    Args:
        score: Total score (0-12)
        
    Returns:
        Rating text with emoji
    """
    if score >= 10:
        return "СИЛЬНЫЙ"
    elif score >= 7:
        return "РАБОЧИЙ"
    elif score >= 4:
        return "СОМНИТЕЛЬНЫЙ"
    else:
        return "СЛАБЫЙ"


def calculate_signal_score_by_minute(
    minute: int,
    league_avg: Optional[float],
    pressure_index: float,
    xg_total: float,
    xg_delta: float,
    shots_ratio: float,
    save_stress: float,
    possession_pressure: float
) -> int:
    """
    Calculate signal score with minute-dependent thresholds.
    
    Uses different evaluation criteria based on match minute windows:
    - 25-30: Stricter thresholds
    - 31-35: Medium thresholds
    - 36-45: Stricter thresholds
    
    Args:
        minute: Current match minute
        league_avg: Average goals for league
        pressure_index: Pressure index value
        xg_total: Total expected goals (xg_home + xg_away)
        xg_delta: Absolute expected goals difference
        shots_ratio: Shots ratio value
        save_stress: Save stress value
        possession_pressure: Possession pressure value
        
    Returns:
        Total score (0-12)
    """
    score = 0
    
    # Type conversions
    try:
        pressure_index = float(pressure_index) if pressure_index not in (None, "", "N/A") else 0.0
    except Exception:
        pressure_index = 0.0
    
    try:
        xg_total = float(xg_total) if xg_total not in (None, "", "N/A") else 0.0
    except Exception:
        xg_total = 0.0
    
    try:
        xg_delta = float(xg_delta) if xg_delta not in (None, "", "N/A") else 0.0
    except Exception:
        xg_delta = 0.0
    
    try:
        shots_ratio = float(shots_ratio) if shots_ratio not in (None, "", "N/A") else 0.0
    except Exception:
        shots_ratio = 0.0
    
    try:
        save_stress = float(save_stress) if save_stress not in (None, "", "N/A") else 0.0
    except Exception:
        save_stress = 0.0
    
    try:
        possession_pressure = float(possession_pressure) if possession_pressure not in (None, "", "N/A") else 0.0
    except Exception:
        possession_pressure = 0.0
    
    # Determine thresholds based on minute window
    if 25 <= minute <= 30:
        # Stricter thresholds (25-30 min)
        pressure_threshold_high = 16.5
        pressure_threshold_low = 16.5
        xg_total_threshold_high = 1.0
        xg_total_threshold_low = 1.0
        xg_delta_threshold_high = 0.35
        xg_delta_threshold_low = 0.35
        save_stress_threshold_high = 0.40
        save_stress_threshold_low = 0.40
        possession_pressure_threshold_high = 0.10
        possession_pressure_threshold_low = 0.10
    elif 31 <= minute <= 35:
        # Medium thresholds (31-35 min)
        pressure_threshold_high = 18.0
        pressure_threshold_low = 18.0
        xg_total_threshold_high = 1.1
        xg_total_threshold_low = 1.1
        xg_delta_threshold_high = 0.30
        xg_delta_threshold_low = 0.30
        save_stress_threshold_high = 0.35
        save_stress_threshold_low = 0.35
        possession_pressure_threshold_high = 0.08
        possession_pressure_threshold_low = 0.08
    elif 36 <= minute <= 45:
        # Stricter thresholds (36-45 min)
        pressure_threshold_high = 19.5
        pressure_threshold_low = 19.5
        xg_total_threshold_high = 1.2
        xg_total_threshold_low = 1.2
        xg_delta_threshold_high = 0.30
        xg_delta_threshold_low = 0.30
        save_stress_threshold_high = 0.35
        save_stress_threshold_low = 0.35
        possession_pressure_threshold_high = 0.08
        possession_pressure_threshold_low = 0.08
    else:
        # Outside monitored window - use base thresholds
        pressure_threshold_high = 20.0
        pressure_threshold_low = 15.0
        xg_total_threshold_high = 1.0
        xg_total_threshold_low = 0.6
        xg_delta_threshold_high = 0.6
        xg_delta_threshold_low = 0.3
        save_stress_threshold_high = 0.7
        save_stress_threshold_low = 0.4
        possession_pressure_threshold_high = 0.15
        possession_pressure_threshold_low = 0.07
    
    # --- Лига ---
    avg = league_avg
    if avg is not None:
        if avg >= 3.0:
            score += 2
        elif avg >= 2.5:
            score += 1
    
    # --- Давление (Pressure) ---
    if pressure_index >= pressure_threshold_high:
        score += 2
    elif pressure_index >= pressure_threshold_low:
        score += 1
    
    # --- xg_total + xg_delta (compound) ---
    # Award points if either xg_total or xg_delta meets stricter threshold
    if xg_total >= xg_total_threshold_high or xg_delta >= xg_delta_threshold_high:
        score += 2
    elif xg_total >= xg_total_threshold_low or xg_delta >= xg_delta_threshold_low:
        score += 1
    
    # --- shots_ratio ---
    if shots_ratio >= 0.5:
        score += 2
    elif shots_ratio >= 0.3:
        score += 1
    
    # --- save_stress ---
    if save_stress >= save_stress_threshold_high:
        score += 2
    elif save_stress >= save_stress_threshold_low:
        score += 1
    
    # --- possession_pressure ---
    if possession_pressure >= possession_pressure_threshold_high:
        score += 2
    elif possession_pressure >= possession_pressure_threshold_low:
        score += 1
    
    return score


def format_match_minute(fixture_metrics: Dict[str, Any], minute: int) -> str:
    status_val = _unwrap_value(fixture_metrics.get("status"))
    status_short = ""
    if isinstance(status_val, dict):
        status_short = str(status_val.get("short") or "").upper()

    elapsed = get_metric_from_fixture(fixture_metrics, "elapsed", None)
    if elapsed is None:
        elapsed = minute or 0
    try:
        elapsed = int(float(elapsed))
    except Exception:
        elapsed = int(minute or 0)

    extra = get_metric_from_fixture(fixture_metrics, "extra_time", 0)
    try:
        extra = int(float(extra))
    except Exception:
        extra = 0

    if status_short == "HT":
        base_minute = 45
        return f"{base_minute}+{extra}" if extra > 0 else str(base_minute)

    if extra > 0:
        return f"{elapsed}+{extra}"

    return str(elapsed)


def build_review_message(fx: Dict[str, Any], stats: Dict[str, Any], computed: Dict[str, Any], probs: Dict[str, Any]) -> str:
    home_team = stats.get("home_team") or "Home"
    away_team = stats.get("away_team") or "Away"

    league_name = normalize_league_name(stats.get("league_name"))
    country_name = normalize_league_name(stats.get("league_country") or stats.get("country"))

    minute_display = stats.get("minute_display") or fmt_int(stats.get("minute"))
    minute = int(stats.get("minute", 0))

    league_avg = stats.get("league_avg_goals")
    league_color, _league_display = get_league_color(league_name, league_avg)

    home_score = stats.get("score_home", 0)
    away_score = stats.get("score_away", 0)
    signal_score = f"{home_score}-{away_score}"

    league_factor_for_score, _league_avg_unused, _league_src = _get_persisted_league_factor(fx)
    _score01_final, match_score, _score_metrics = compute_match_score_v2(fx, league_factor_for_score)
    match_status = get_signal_rating(match_score)

    team_boosts = calc_match_team_boost_v2(fx)
    league_avg_goals = float(team_boosts.get("league_avg_goals", GLOBAL_GOALS_BASE) or GLOBAL_GOALS_BASE)

    attack_factor_home = float(team_boosts.get("home_attack_factor", 1.0) or 1.0)
    defense_factor_home = float(team_boosts.get("home_defense_factor", 1.0) or 1.0)
    attack_factor_away = float(team_boosts.get("away_attack_factor", 1.0) or 1.0)
    defense_factor_away = float(team_boosts.get("away_defense_factor", 1.0) or 1.0)

    home_matches_count = int(team_boosts.get("home_matches", 0) or 0)
    away_matches_count = int(team_boosts.get("away_matches", 0) or 0)
    home_avg_scored_raw = team_boosts.get("home_avg_scored_raw")
    home_avg_conceded_raw = team_boosts.get("home_avg_conceded_raw")
    away_avg_scored_raw = team_boosts.get("away_avg_scored_raw")
    away_avg_conceded_raw = team_boosts.get("away_avg_conceded_raw")
    home_avg_scored = float(home_avg_scored_raw) if home_avg_scored_raw is not None else 0.0
    home_avg_conceded = float(home_avg_conceded_raw) if home_avg_conceded_raw is not None else 0.0
    away_avg_scored = float(away_avg_scored_raw) if away_avg_scored_raw is not None else 0.0
    away_avg_conceded = float(away_avg_conceded_raw) if away_avg_conceded_raw is not None else 0.0

    teams_boost = float(team_boosts.get("team_mix_factor", 1.0) or 1.0)
    league_factor = float(team_boosts.get("league_factor", 1.0) or 1.0)
    final_boost = float(team_boosts.get("combined_m", 1.0) or 1.0)

    def get_boost_emoji(value: float) -> str:
        if value >= 1.15:
            return "🟢"
        elif value >= 1.05:
            return "🟡"
        return "🔴"

    teams_boost_emoji = get_boost_emoji(teams_boost)
    league_boost_emoji = get_boost_emoji(float(league_factor))
    final_boost_emoji = get_boost_emoji(final_boost)

    context_block = f"""
📈 Контекст лиги и команд:

🏟️Лига: {league_avg_goals:.2f} гола/матч

⚽️ {home_team}
↳ Атака: {attack_factor_home:.2f}
↳ Оборона: {defense_factor_home:.2f}
↳ {home_matches_count} матчей: {home_avg_scored:.2f} заб / {home_avg_conceded:.2f} проп

⚽️ {away_team}
↳ Атака: {attack_factor_away:.2f}
↳ Оборона: {defense_factor_away:.2f}
↳ {away_matches_count} матчей: {away_avg_scored:.2f} заб / {away_avg_conceded:.2f} проп

{teams_boost_emoji} Буст команд: {teams_boost:.2f}
{league_boost_emoji} Буст лиги: {float(league_factor):.2f}
{final_boost_emoji} Итоговый буст матча: {final_boost:.2f}
""".strip()

    pressure_index = stats.get('pressure_index', 0)
    xg_delta = computed.get('xg_delta', 0)
    shots_ratio = computed.get('shots_ratio', 0)
    save_stress = computed.get('save_stress', 0)
    possession_pressure = computed.get('possession_pressure', 0)

    pressure_color = get_color('pressure_index', pressure_index, minute)
    xg_color = get_color('xg_delta', xg_delta, minute)
    shots_ratio_color = get_color('shots_ratio', shots_ratio, minute)
    save_stress_color = get_color('save_stress', save_stress, minute)
    possession_color = get_color('possession_pressure', possession_pressure, minute)

    admin_message = f"""
👁️ Босс, нужен совет

{home_team} — {away_team}

{league_name}, {country_name}

Счет: {signal_score} {minute} минута 

Вероятность гола до 75 минуты: {float(probs.get('prob_75') or 0.0):.1f}%
Вероятность гола до 90 минуты: {float(probs.get('prob_90') or 0.0):.1f}%

xG до {minute} минуты: {float(stats.get('xg_home') or 0.0):.2f} - {float(stats.get('xg_away') or 0.0):.2f}

Удары по воротам: {int(float(stats.get('shots_total_home') or 0.0))} - {int(float(stats.get('shots_total_away') or 0.0))}
Удары в створ: {int(float(stats.get('shots_on_target_home') or 0.0))} - {int(float(stats.get('shots_on_target_away') or 0.0))}
Сейвы: {int(float(stats.get('saves_home') or 0.0))} - {int(float(stats.get('saves_away') or 0.0))}
Удары из штрафной: {int(float(stats.get('shots_in_box_home') or 0.0))} - {int(float(stats.get('shots_in_box_away') or 0.0))}
Угловые: {int(float(stats.get('corners_home') or 0.0))} - {int(float(stats.get('corners_away') or 0.0))}
Владение: {int(float(stats.get('possession_home') or 0.0))}% - {int(float(stats.get('possession_away') or 0.0))}%

{context_block}

{league_color} Лига: {league_name}
{pressure_color} Индекс давления: {float(pressure_index or 0.0):.2f}
{xg_color} xg_delta: {float(xg_delta or 0.0):.2f}
{shots_ratio_color} shots_ratio: {float(shots_ratio or 0.0):.2f}
{save_stress_color} save_stress: {float(save_stress or 0.0):.2f}
{possession_color} possession_pressure: {float(possession_pressure or 0.0):.2f}

📊 Итоговая оценка: {match_status} ({int(match_score)}/12)
"""
    return admin_message.strip()


def _admin_review_extract_score(fixture_metrics: Dict[str, Any]) -> Tuple[int, int]:
    home_score = extract_value(fixture_metrics.get("score_home", 0), 0)
    away_score = extract_value(fixture_metrics.get("score_away", 0), 0)
    try:
        return int(home_score), int(away_score)
    except Exception:
        return 0, 0


def _admin_review_get_minute(fixture_metrics: Dict[str, Any], fallback_minute: int) -> int:
    elapsed = fixture_metrics.get("elapsed")
    if isinstance(elapsed, dict):
        elapsed = elapsed.get("value")
    if elapsed is None:
        elapsed = fixture_metrics.get("minute")
    try:
        return int(float(elapsed))
    except Exception:
        return int(fallback_minute or 0)


def _admin_review_is_finished(fixture_metrics: Dict[str, Any]) -> bool:
    status_val = fixture_metrics.get("status")
    if isinstance(status_val, dict):
        status_short = str(status_val.get("short") or "").upper()
        status_value = str(status_val.get("value") or "").strip().lower()
    else:
        status_short = str(status_val or "").upper()
        status_value = str(status_val or "").strip().lower()

    elapsed_label = (fixture_metrics.get("elapsed_label") or {}).get("value") or ""
    elapsed_label = str(elapsed_label).strip().lower()

    if status_short in ("FT", "AET", "PEN"):
        return True
    if status_value in ("finished", "match finished"):
        return True
    if elapsed_label in ("ft", "finished", "match finished"):
        return True
    return False


def build_admin_review_text(data: Dict[str, Any], minute: int, fixture_id: int, include_monitoring: bool, final_line: Optional[str] = None) -> str:
    base_text = build_review_card(data, 0.0, minute, fixture_id)
    if final_line:
        return base_text + "\n\n" + final_line
    if include_monitoring:
        return base_text + "\n\n⏳ Мониторинг до финального свистка..."
    return base_text


def build_review_keyboard(fixture_id: int) -> Dict[str, Any]:
    """
    Build inline keyboard for review messages.
    
    Args:
        fixture_id: Match fixture ID for callback data
        
    Returns:
        Keyboard dictionary with inline buttons
    """
    return {
        "inline_keyboard": [
            [
                {"text": "✅ ОТПРАВИТЬ В КАНАЛ", "callback_data": f"review_send:{fixture_id}"}
            ],
            [
                {"text": "❌ ИГНОРИРОВАТЬ", "callback_data": f"review_skip:{fixture_id}"}
            ]
        ]
    }


def _edit_admin_review_message(chat_id: int, message_id: int, text: str, reply_markup: Optional[Dict[str, Any]] = None) -> Tuple[bool, str, int]:
    """
    Edit admin review message with optional keyboard.
    
    Args:
        chat_id: Telegram chat ID
        message_id: Message ID to edit
        text: New text content
        reply_markup: Optional keyboard (None removes keyboard, dict keeps/updates it)
        
    Returns:
        Tuple of (success, status, retry_after)
    """
    try:
        ok, status, retry_after = tg_edit_queue.edit_message_text(
            chat_id=int(chat_id),
            message_id=int(message_id),
            text=text,
            reply_markup=reply_markup,
            log_prefix="[REVIEW] edit",
        )

        if ok:
            return True, "ok", 0

        if status in ("unchanged", "not_modified"):
            return False, "not_modified", 0

        if status in ("rate_limit", "chat_frozen"):
            return False, "rate_limit", retry_after

        return False, "error", 0
    except Exception:
        return False, "error", 0


def build_review_card(data: Dict[str, Any], prob: float, minute: int, fixture_id: int) -> str:
    """
    Build review card text for admin moderation.
    Uses Google Sheets metrics logic for consistency.
    """
    fixture_metrics = data.get("fixture", {}) or {}

    home_team = normalize_team_name(fixture_metrics.get("team_home_name"))
    away_team = normalize_team_name(fixture_metrics.get("team_away_name"))
    if not home_team:
        home_team = normalize_team_name(fixture_metrics.get("home_team"))
    if not away_team:
        away_team = normalize_team_name(fixture_metrics.get("away_team"))
    if not home_team:
        home_team = "Home"
    if not away_team:
        away_team = "Away"

    league_name = normalize_league_name(fixture_metrics.get("league_name"))
    league_country = normalize_league_name(fixture_metrics.get("league_country"))
    if not league_country:
        league_country = normalize_league_name(fixture_metrics.get("country"))
    league_id = _unwrap_metric_int(fixture_metrics.get("league_id"))
    persisted_league_rec = get_persisted_league_record(league_id)
    league_avg_goals = None
    if persisted_league_rec and persisted_league_rec.get("avg_goals") is not None:
        try:
            league_avg_goals = float(persisted_league_rec.get("avg_goals"))
        except Exception:
            league_avg_goals = None

    score_home = extract_value(fixture_metrics.get("score_home", 0), 0)
    score_away = extract_value(fixture_metrics.get("score_away", 0), 0)

    xg_home = get_any_metric(fixture_metrics, ["expected_goals"], "home") or 0.0
    xg_away = get_any_metric(fixture_metrics, ["expected_goals"], "away") or 0.0
    if xg_home == 0:
        xg_home = estimate_xg_from_metrics_combined(fixture_metrics, "home")
    if xg_away == 0:
        xg_away = estimate_xg_from_metrics_combined(fixture_metrics, "away")

    shots_total_home = int(get_any_metric(fixture_metrics, ["total_shots"], "home") or 0)
    shots_total_away = int(get_any_metric(fixture_metrics, ["total_shots"], "away") or 0)
    shots_on_home = int(get_any_metric(fixture_metrics, ["shots_on_target"], "home") or 0)
    shots_on_away = int(get_any_metric(fixture_metrics, ["shots_on_target"], "away") or 0)
    saves_home = int(get_any_metric(fixture_metrics, ["saves", "goalkeeper_saves"], "home") or 0)
    saves_away = int(get_any_metric(fixture_metrics, ["saves", "goalkeeper_saves"], "away") or 0)
    shots_in_box_home = int(get_any_metric(fixture_metrics, ["shots_insidebox", "inside_box"], "home") or 0)
    shots_in_box_away = int(get_any_metric(fixture_metrics, ["shots_insidebox", "inside_box"], "away") or 0)
    corners_home = int(get_any_metric(fixture_metrics, ["corner_kicks", "corners"], "home") or 0)
    corners_away = int(get_any_metric(fixture_metrics, ["corner_kicks", "corners"], "away") or 0)
    possession_home = float(get_any_metric(fixture_metrics, ["ball_possession", "passes_%"], "home") or 0)
    possession_away = float(get_any_metric(fixture_metrics, ["ball_possession", "passes_%"], "away") or 0)

    pressure_index = calculate_pressure_index(fixture_metrics)

    derived_xg_delta = float(xg_home) - float(xg_away)
    derived_shots_ratio = (float(shots_on_home) + 1.0) / (float(shots_on_away) + 1.0)
    derived_save_stress = calculate_save_stress(
        saves_home=saves_home,
        saves_away=saves_away,
        current_home_score=score_home,
        current_away_score=score_away,
        shots_on_target_home=shots_on_home,
        shots_on_target_away=shots_on_away
    )
    derived_possession_pressure = abs(float(possession_home) - float(possession_away)) / 100.0

    res = compute_lambda_and_probability(fixture_metrics, minute)
    prob_75 = res.get("prob_goal_either_to75") if res else None
    prob_90 = res.get("prob_goal_either_to90") if res else None

    minute_display = format_match_minute(fixture_metrics, minute)

    stats = {
        "home_team": home_team,
        "away_team": away_team,
        "league_name": league_name,
        "league_country": league_country,
        "league_avg_goals": league_avg_goals,
        "score_home": int(score_home),
        "score_away": int(score_away),
        "minute": minute,
        "minute_display": minute_display,
        "xg_home": xg_home,
        "xg_away": xg_away,
        "shots_total_home": shots_total_home,
        "shots_total_away": shots_total_away,
        "shots_on_target_home": shots_on_home,
        "shots_on_target_away": shots_on_away,
        "saves_home": saves_home,
        "saves_away": saves_away,
        "shots_in_box_home": shots_in_box_home,
        "shots_in_box_away": shots_in_box_away,
        "corners_home": corners_home,
        "corners_away": corners_away,
        "possession_home": possession_home,
        "possession_away": possession_away,
        "pressure_index": pressure_index,
        "match_id": fixture_id
    }

    computed = {
        "xg_delta": derived_xg_delta,
        "shots_ratio": derived_shots_ratio,
        "save_stress": derived_save_stress,
        "possession_pressure": derived_possession_pressure
    }

    probs = {
        "prob_75": prob_75,
        "prob_90": prob_90
    }

    return build_review_message(fixture_metrics, stats, computed, probs)


def calculate_signal_rating_value(data: Dict[str, Any]) -> float:
    """
    Calculate numeric signal rating for admin-review auto-approval.
    Returns rating on the same 0..12 scale used in review card.
    """
    fixture_metrics = data.get("fixture", {}) or {}

    league_factor, _league_avg, _league_source = _get_persisted_league_factor(fixture_metrics)
    _score01_final, match_score, _metrics = compute_match_score_v2(fixture_metrics, league_factor)
    return float(match_score)


def get_review_league_avg(data: Dict[str, Any]) -> Tuple[str, Optional[float]]:
    fixture_metrics = data.get("fixture", {}) or {}
    league_id = _unwrap_metric_int(fixture_metrics.get("league_id"))
    league_name = normalize_league_name(fixture_metrics.get("league_name"))
    rec = get_persisted_league_record(league_id)
    if rec and rec.get("avg_goals") is not None:
        try:
            return league_name, float(rec.get("avg_goals"))
        except Exception:
            pass
    return league_name, None


def can_auto_post_admin(score: float, data: Dict[str, Any], league_avg_goals: Optional[float]) -> Tuple[bool, str, Dict[str, Any]]:
    score_val = float(score)
    fixture_metrics = data.get("fixture", {}) or {}
    league_id = _unwrap_metric_int(fixture_metrics.get("league_id"))

    try:
        league_avg_val = float(league_avg_goals) if league_avg_goals is not None else 0.0
    except Exception:
        league_avg_val = 0.0

    sample_size = 0
    rec = get_persisted_league_record(league_id)
    if rec:
        try:
            sample_size = max(0, int(rec.get("sample_size") or 0))
        except Exception:
            sample_size = 0

    shots_home = int(get_any_metric(fixture_metrics, ["total_shots"], "home") or 0)
    shots_away = int(get_any_metric(fixture_metrics, ["total_shots"], "away") or 0)
    shots_on_target_home = int(get_any_metric(fixture_metrics, ["shots_on_target"], "home") or 0)
    shots_on_target_away = int(get_any_metric(fixture_metrics, ["shots_on_target"], "away") or 0)
    saves_home = int(get_any_metric(fixture_metrics, ["saves", "goalkeeper_saves"], "home") or 0)
    saves_away = int(get_any_metric(fixture_metrics, ["saves", "goalkeeper_saves"], "away") or 0)
    score_home = int(get_metric_from_fixture(fixture_metrics, "score_home", 0) or 0)
    score_away = int(get_metric_from_fixture(fixture_metrics, "score_away", 0) or 0)
    possession_home = float(get_any_metric(fixture_metrics, ["ball_possession", "passes_%"], "home") or 0)
    possession_away = float(get_any_metric(fixture_metrics, ["ball_possession", "passes_%"], "away") or 0)

    shots_on_target_total = shots_on_target_home + shots_on_target_away
    total_shots = shots_home + shots_away
    shots_diff = abs(shots_home - shots_away)
    save_stress = calculate_save_stress(
        saves_home=saves_home,
        saves_away=saves_away,
        current_home_score=score_home,
        current_away_score=score_away,
        shots_on_target_home=shots_on_target_home,
        shots_on_target_away=shots_on_target_away
    )
    possession_pressure = abs(float(possession_home) - float(possession_away)) / 100.0

    # Extract minute from fixture_metrics for minute-dependent thresholds
    minute = int((fixture_metrics.get("elapsed") or {}).get("value") or 0)
    
    # Apply minute-dependent thresholds
    if 25 <= minute <= 30:
        # Stricter thresholds for 25-30 minute window
        score_threshold = 9.0
        league_threshold = 2.85
        stress_threshold = 0.40
    elif 31 <= minute <= 35:
        # Medium thresholds for 31-35 minute window
        score_threshold = 8.5
        league_threshold = 2.75
        stress_threshold = 0.35
    elif 36 <= minute <= 45:
        # Stricter thresholds for 36-45 minute window
        score_threshold = 8.0
        league_threshold = 2.70
        stress_threshold = 0.33
    else:
        # Default thresholds for other windows
        score_threshold = 8.0
        league_threshold = 2.7
        stress_threshold = 0.33

    score_ok = score_val >= score_threshold
    league_ok = (league_avg_val >= league_threshold) and (sample_size > 0)
    stress_ok = save_stress >= stress_threshold

    suspicious = (
        save_stress <= 0.05
        and shots_on_target_total <= 3
        and total_shots <= 5
        and possession_pressure <= 0.25
        and shots_diff <= 1
    )

    auto_admin_allowed = score_ok and league_ok and stress_ok and (not suspicious)

    if auto_admin_allowed:
        reason = "ok"
    elif not score_ok:
        reason = f"score<{score_threshold}"
    elif league_avg_val < league_threshold:
        reason = f"league_avg<{league_threshold}"
    elif not stress_ok:
        reason = f"save_stress<{stress_threshold}"
    elif suspicious:
        reason = "suspicious_filter_hit"
    else:
        reason = f"league_avg<{league_threshold}"

    details = {
        "score": score_val,
        "league_avg": league_avg_val,
        "sample": sample_size,
        "save_stress": save_stress,
        "score_ok": score_ok,
        "league_ok": league_ok,
        "stress_ok": stress_ok,
        "suspicious": suspicious,
        "auto_admin_allowed": auto_admin_allowed,
        "minute": minute,
    }

    return auto_admin_allowed, reason, details


def _apply_auto_signal_prob_boost(base_prob_percent: float, score: float, league_avg: float, metric: str) -> float:
    try:
        base_prob = clamp(float(base_prob_percent) / 100.0, 0.0, 1.0)
        score_norm = clamp((float(score) - 8.0) / 4.0, 0.0, 1.0)
        league_norm = clamp((float(league_avg) - 2.7) / 1.0, 0.0, 1.0)

        linear_strength = 0.25 * score_norm + 0.10 * league_norm
        boost_strength = math.log1p(linear_strength) / math.log(2.0)
        boost_strength = clamp(boost_strength, 0.0, 1.0)

        adjusted_prob = base_prob + (1.0 - base_prob) * boost_strength
        adjusted_prob = min(adjusted_prob, 0.90)

        logger.info(
            "[PROB_BOOST] metric=%s base=%.4f score=%.1f league_avg=%.2f boost_strength=%.4f final=%.4f",
            metric,
            base_prob,
            float(score),
            float(league_avg),
            boost_strength,
            adjusted_prob,
        )
        return adjusted_prob * 100.0
    except Exception:
        logger.exception("[PROB_BOOST] failed to apply boost for metric=%s", metric)
        return float(base_prob_percent)

def send_review_to_admin(fixture_id: int, data: Dict[str, Any], prob: float, minute: int) -> bool:
    """
    Send review request to admin for moderation.
    Uses ADMIN_USER_ID from config.json (loaded at startup).
    
    Args:
        fixture_id: Match fixture ID
        data: Match data from collect_match_all
        prob: Probability value
        minute: Current match minute
        
    Returns:
        True if sent successfully, False otherwise
    """
    # Check if admin ID is configured
    if not ADMIN_USER_ID:
        logger.warning(f"[REVIEW] ADMIN_USER_ID not configured, review mode disabled for fixture_id={fixture_id}")
        return False

    if not validate_match_context_before_send(data):
        return False

    with state_lock:
        review_queue = state.setdefault("review_queue", {})
        review_tracking = state.get("review_tracking", {})
        admin_reviews = state.setdefault("admin_reviews", {})

        # Check if already sent or being tracked
        if str(fixture_id) in review_queue and review_queue[str(fixture_id)].get("review_sent"):
            logger.debug(f"[REVIEW] Already sent for fixture_id={fixture_id}, skipping")
            return False

        # Also check review_tracking to prevent duplicate messages for ongoing reviews
        if str(fixture_id) in review_tracking and not review_tracking[str(fixture_id)].get("done", False):
            logger.debug(f"[REVIEW] Review already being tracked for fixture_id={fixture_id}, skipping duplicate send")
            return False

        if str(fixture_id) in admin_reviews and not admin_reviews[str(fixture_id)].get("finished", False):
            logger.debug(f"[REVIEW] Admin review already exists for fixture_id={fixture_id}, skipping duplicate send")
            return False
    
    signal_rating = calculate_signal_rating_value(data)
    league_name, league_avg = get_review_league_avg(data)
    can_auto, auto_reason, auto_details = can_auto_post_admin(signal_rating, data, league_avg)

    # Build review card
    card_text = build_review_card(data, prob, minute, fixture_id)
    
    # Build inline keyboard with review buttons using centralized function
    keyboard = build_review_keyboard(fixture_id)
    
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {
            "chat_id": ADMIN_USER_ID,
            "text": card_text,
            "reply_markup": json.dumps(keyboard)
        }
        r = requests.post(url, data=payload, timeout=10)
        
        if r.ok:
            try:
                body = r.json()
                msg_id = body.get("result", {}).get("message_id")
            except Exception:
                msg_id = None
            
            # Save to review_queue
            # Extract score at signal time
            fixture = data.get("fixture", {}) or {}
            score_home_at_signal = int((fixture.get("score_home") or {}).get("value") or 0)
            score_away_at_signal = int((fixture.get("score_away") or {}).get("value") or 0)
            
            with state_lock:
                review_queue[str(fixture_id)] = {
                    "review_sent": True,
                    "review_ts": time.time(),
                    "review_decision": None,
                    "message_id": msg_id,
                    "data": data,
                    "prob": prob,
                    "minute": minute,
                    "approved_by_admin": False,
                    "signal_rating": signal_rating,
                }

                if msg_id:
                    admin_reviews[str(fixture_id)] = {
                        "message_id": int(msg_id),
                        "chat_id": int(ADMIN_USER_ID),
                        "created_ts": time.time(),
                        "last_edit_ts": time.time(),
                        "signal_minute": int(minute),
                        "score_home_at_signal": int(score_home_at_signal),
                        "score_away_at_signal": int(score_away_at_signal),
                        "sent_payload": {"text": card_text},
                        "finished": False
                    }
                    logger.info(f"[REVIEW] created fixture={fixture_id} msg_id={msg_id}")
                
                # Also save to review_tracking for monitoring and message editing
                if msg_id:
                    state.setdefault("review_tracking", {})[str(fixture_id)] = {
                        "chat_id": ADMIN_USER_ID,
                        "message_id": msg_id,
                        "signal_minute": minute,
                        "score_home_at_signal": score_home_at_signal,
                        "score_away_at_signal": score_away_at_signal,
                        "base_text": card_text,
                        "created_ts": time.time(),
                        "done": False
                    }
                    logger.info(f"[REVIEW_TRACK] added fixture={fixture_id} msg_id={msg_id}")
                
                mark_state_dirty()

            # Optional auto-post branch (review UX stays intact for non-eligible signals)
            auto_post_done = False
            if can_auto:
                with state_lock:
                    admin_auto_posted = state.setdefault("admin_auto_posted", {})
                    already_auto_posted = bool(admin_auto_posted.get(str(fixture_id), False))
                    already_has_message = str(fixture_id) in state.get("sent_matches", {})

                if already_auto_posted or already_has_message:
                    reason = "already_sent"
                    logger.info(
                        "[AUTO_ADMIN] score=%.1f league_avg=%.2f sample=%d save_stress=%.3f "
                        "score_ok=%s league_ok=%s stress_ok=%s suspicious=%s decision=REVIEW reason=%s",
                        float(auto_details.get("score", signal_rating)),
                        float(auto_details.get("league_avg", league_avg or 0.0)),
                        int(auto_details.get("sample", 0) or 0),
                        float(auto_details.get("save_stress", 0.0)),
                        bool(auto_details.get("score_ok", False)),
                        bool(auto_details.get("league_ok", False)),
                        bool(auto_details.get("stress_ok", False)),
                        bool(auto_details.get("suspicious", False)),
                        reason,
                    )
                else:
                    base_prob_75 = float(prob)
                    base_prob_90 = float(prob)
                    try:
                        fixture_metrics_auto = data.get("fixture", {}) or {}
                        minute_auto = int(minute or 0)
                        res_auto = compute_lambda_and_probability(fixture_metrics_auto, minute_auto)
                        if isinstance(res_auto, dict):
                            base_prob_75 = float(res_auto.get("prob_goal_either_to75", base_prob_75) or base_prob_75)
                            base_prob_90 = float(res_auto.get("prob_goal_either_to90", base_prob_90) or base_prob_90)
                    except Exception:
                        logger.exception("[PROB_BOOST] failed to fetch base probabilities for auto-post fixture=%s", fixture_id)

                    league_avg_for_boost = float(league_avg) if league_avg is not None else float(DEFAULT_LEAGUE_AVG_GOALS)
                    boosted_prob_75 = _apply_auto_signal_prob_boost(base_prob_75, signal_rating, league_avg_for_boost, metric="prob75")
                    boosted_prob_90 = _apply_auto_signal_prob_boost(base_prob_90, signal_rating, league_avg_for_boost, metric="prob90")

                    review_payload = {
                        "data": data,
                        "prob": boosted_prob_75,
                        "prob_90": boosted_prob_90,
                        "minute": minute,
                        "approved_by_admin": True,
                        "signal_rating": signal_rating,
                    }
                    auto_post_done = publish_signal_to_channel(fixture_id, review_info=review_payload)

                    if auto_post_done:
                        with state_lock:
                            review_queue[str(fixture_id)]["review_decision"] = "auto_sent"
                            review_queue[str(fixture_id)]["review_decision_ts"] = time.time()
                            review_queue[str(fixture_id)]["review_by"] = "auto"
                            review_queue[str(fixture_id)]["resolved"] = True
                            review_queue[str(fixture_id)]["approved_by_admin"] = True
                            state.setdefault("admin_auto_posted", {})[str(fixture_id)] = True

                            if str(fixture_id) in state.get("admin_reviews", {}):
                                state["admin_reviews"][str(fixture_id)]["finished"] = True
                            if str(fixture_id) in state.get("review_tracking", {}):
                                state["review_tracking"][str(fixture_id)]["done"] = True
                            mark_state_dirty()

                        if msg_id:
                            try:
                                edit_url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/editMessageReplyMarkup"
                                edit_payload = {
                                    "chat_id": ADMIN_USER_ID,
                                    "message_id": int(msg_id),
                                    "reply_markup": json.dumps({"inline_keyboard": []})
                                }
                                requests.post(edit_url, data=edit_payload, timeout=5)
                            except Exception:
                                logger.exception(f"[AUTO_ADMIN] Failed to remove keyboard for fixture={fixture_id}")

                        logger.info(
                            "[AUTO_ADMIN] score=%.1f league_avg=%.2f sample=%d save_stress=%.3f "
                            "score_ok=%s league_ok=%s stress_ok=%s suspicious=%s decision=AUTO reason=%s",
                            float(auto_details.get("score", signal_rating)),
                            float(auto_details.get("league_avg", league_avg or 0.0)),
                            int(auto_details.get("sample", 0) or 0),
                            float(auto_details.get("save_stress", 0.0)),
                            bool(auto_details.get("score_ok", False)),
                            bool(auto_details.get("league_ok", False)),
                            bool(auto_details.get("stress_ok", False)),
                            bool(auto_details.get("suspicious", False)),
                            auto_reason,
                        )
                    else:
                        logger.info(
                            "[AUTO_ADMIN] score=%.1f league_avg=%.2f sample=%d save_stress=%.3f "
                            "score_ok=%s league_ok=%s stress_ok=%s suspicious=%s decision=REVIEW reason=%s",
                            float(auto_details.get("score", signal_rating)),
                            float(auto_details.get("league_avg", league_avg or 0.0)),
                            int(auto_details.get("sample", 0) or 0),
                            float(auto_details.get("save_stress", 0.0)),
                            bool(auto_details.get("score_ok", False)),
                            bool(auto_details.get("league_ok", False)),
                            bool(auto_details.get("stress_ok", False)),
                            bool(auto_details.get("suspicious", False)),
                            "send_failed",
                        )
            else:
                logger.info(
                    "[AUTO_ADMIN] score=%.1f league_avg=%.2f sample=%d save_stress=%.3f "
                    "score_ok=%s league_ok=%s stress_ok=%s suspicious=%s decision=REVIEW reason=%s",
                    float(auto_details.get("score", signal_rating)),
                    float(auto_details.get("league_avg", league_avg or 0.0)),
                    int(auto_details.get("sample", 0) or 0),
                    float(auto_details.get("save_stress", 0.0)),
                    bool(auto_details.get("score_ok", False)),
                    bool(auto_details.get("league_ok", False)),
                    bool(auto_details.get("stress_ok", False)),
                    bool(auto_details.get("suspicious", False)),
                    auto_reason,
                )

            logger.info(f"[REVIEW] Sent to ADMIN_USER_ID={ADMIN_USER_ID} fixture_id={fixture_id} prob={prob:.1f}%")
            return True
        else:
            logger.warning(f"[REVIEW] Failed to send: {r.text}")
            return False
    except Exception:
        logger.exception(f"[REVIEW] Error sending review for fixture_id={fixture_id}")
        return False


def update_review_tracking(client: APISportsMetricsClient = None):
    """
    Monitor review messages: fetch match data, check if finished, edit message with final score.
    
    Only processes review messages where done=False.
    For finished matches, edits the original review message to append final score result.
    
    Args:
        client: APISportsMetricsClient instance for fetching match data (optional, tries to use global)
    """
    if not client:
        return  # Can't fetch data without client
    
    with state_lock:
        review_tracking = state.get("review_tracking", {})
        if not review_tracking:
            return
        
        # Get list of unfinished reviews
        pending_reviews = [
            (fid, info) for fid, info in review_tracking.items() 
            if isinstance(info, dict) and not info.get("done", False)
        ]
    
    if not pending_reviews:
        return
    
    for fixture_id_str, tracking_info in pending_reviews:
        try:
            fixture_id = int(fixture_id_str)
        except (ValueError, TypeError):
            logger.warning(f"[REVIEW_TRACK] Invalid fixture_id in review_tracking: {fixture_id_str}")
            continue

        with state_lock:
            admin_review = state.get("admin_reviews", {}).get(fixture_id_str)
            if isinstance(admin_review, dict) and not admin_review.get("finished", False):
                continue
        
        try:
            # Fetch current match data
            data = client.collect_match_all(fixture_id)
            if not data:
                logger.debug(f"[REVIEW_TRACK] No data available for fixture={fixture_id}")
                continue
            
            fixture = data.get("fixture", {}) or {}
            
            # Check if match is finished
            elapsed_label = (fixture.get("elapsed_label") or {}).get("value") or ""
            elapsed_minutes = int((fixture.get("elapsed") or {}).get("value") or 0)
            
            is_finished = False
            if isinstance(elapsed_label, str) and elapsed_label.strip().lower() in ("ft", "finished", "full time"):
                is_finished = True
            if elapsed_minutes >= 95:
                is_finished = True
            
            if not is_finished:
                # Match not finished yet, skip
                continue
            
            # Match is finished - get final score
            final_home = int((fixture.get("score_home") or {}).get("value") or 0)
            final_away = int((fixture.get("score_away") or {}).get("value") or 0)
            
            # Extract signal score from tracking info
            signal_home = tracking_info.get("score_home_at_signal", 0)
            signal_away = tracking_info.get("score_away_at_signal", 0)
            
            # Calculate: did goal occur after signal?
            signal_total = signal_home + signal_away
            final_total = final_home + final_away
            is_plus = final_total > signal_total
            
            # Build result suffix
            if is_plus:
                result_suffix = f"\n\n✅ Финальный счет: {final_home}:{final_away}"
            else:
                result_suffix = f"\n\n❌ Финальный счет: {final_home}:{final_away}"
            
            # Combine old text with result
            base_text = tracking_info.get("base_text", "")
            new_text = base_text + result_suffix
            
            # Edit Telegram message
            chat_id = tracking_info.get("chat_id")
            message_id = tracking_info.get("message_id")
            
            if chat_id and message_id:
                try:
                    ok, status, retry_after = tg_edit_queue.edit_message_text(
                        chat_id=int(chat_id),
                        message_id=int(message_id),
                        text=new_text,
                        parse_mode="HTML",
                        log_prefix="[REVIEW_TRACK] edit",
                        allow_plain_fallback=True,
                    )

                    if ok or status in ("unchanged", "not_modified"):
                        with state_lock:
                            state["review_tracking"][fixture_id_str]["done"] = True
                            mark_state_dirty()
                        logger.info(f"[REVIEW_TRACK] finalized fixture={fixture_id} result={'PLUS' if is_plus else 'MINUS'} {final_home}:{final_away}")
                    else:
                        logger.warning(f"[REVIEW_TRACK] Failed to edit message for fixture={fixture_id}: status={status} retry_after={retry_after}")
                        # Still mark as done to avoid retrying failed edits
                        with state_lock:
                            state["review_tracking"][fixture_id_str]["done"] = True
                            mark_state_dirty()
                except Exception as e:
                    logger.exception(f"[REVIEW_TRACK] Error editing message for fixture={fixture_id}: {e}")
                    # Mark done anyway to prevent infinite retries
                    with state_lock:
                        state["review_tracking"][fixture_id_str]["done"] = True
                        mark_state_dirty()
            else:
                # No chat_id or message_id, mark as done
                logger.warning(f"[REVIEW_TRACK] Missing chat_id or message_id for fixture={fixture_id}")
                with state_lock:
                    state["review_tracking"][fixture_id_str]["done"] = True
                    mark_state_dirty()
        
        except Exception as e:
            logger.exception(f"[REVIEW_TRACK] Unexpected error processing fixture={fixture_id}: {e}")


_admin_review_thread: Optional[threading.Thread] = None
_admin_review_stop = threading.Event()


def admin_review_daemon(client: APISportsMetricsClient, interval: int = ADMIN_REVIEW_INTERVAL):
    """Background daemon that updates admin review messages in DM."""
    logger.info("[REVIEW] admin review daemon started")
    while not _admin_review_stop.wait(interval):
        if not client:
            continue
        try:
            with state_lock:
                admin_reviews = state.get("admin_reviews", {})
                pending = [
                    (fid, info) for fid, info in admin_reviews.items()
                    if isinstance(info, dict) and not info.get("finished", False)
                ]

            if not pending:
                continue

            for fixture_id_str, info in pending:
                try:
                    fixture_id = int(fixture_id_str)
                except (ValueError, TypeError):
                    logger.warning(f"[REVIEW] invalid fixture_id in admin_reviews: {fixture_id_str}")
                    continue

                # Check if admin already made decision on this review
                with state_lock:
                    review_queue = state.get("review_queue", {})
                    review_info = review_queue.get(str(fixture_id), {})
                    if review_info.get("review_decision"):
                        logger.info(f"[REVIEW] Skipping updates for fixture={fixture_id} - already resolved as {review_info.get('review_decision')}")
                        # Mark as finished to stop future updates
                        if fixture_id_str in state.get("admin_reviews", {}):
                            state["admin_reviews"][fixture_id_str]["finished"] = True
                            state["admin_reviews"][fixture_id_str]["last_edit_ts"] = time.time()
                            mark_state_dirty()
                        continue

                last_edit_ts = float(info.get("last_edit_ts", 0))
                if time.time() - last_edit_ts < ADMIN_REVIEW_EDIT_GUARD:
                    continue

                chat_id = info.get("chat_id")
                message_id = info.get("message_id")
                if not chat_id or not message_id:
                    logger.warning(f"[REVIEW] missing chat_id/message_id for fixture={fixture_id}")
                    with state_lock:
                        if fixture_id_str in state.get("admin_reviews", {}):
                            state["admin_reviews"][fixture_id_str]["finished"] = True
                            state["admin_reviews"][fixture_id_str]["last_edit_ts"] = time.time()
                            mark_state_dirty()
                    continue

                try:
                    data = client.collect_match_all(fixture_id)
                except Exception as e:
                    logger.warning(f"[REVIEW] api error fixture={fixture_id}: {e}")
                    continue

                if not data:
                    logger.warning(f"[REVIEW] no data fixture={fixture_id}")
                    continue

                fixture = data.get("fixture", {}) or {}
                minute = _admin_review_get_minute(fixture, info.get("signal_minute", 0))
                is_finished = _admin_review_is_finished(fixture)

                final_line = None
                is_plus = False
                if is_finished:
                    final_home, final_away = _admin_review_extract_score(fixture)
                    signal_home = int(info.get("score_home_at_signal", 0))
                    signal_away = int(info.get("score_away_at_signal", 0))
                    signal_score = (signal_home, signal_away)
                    final_score = (final_home, final_away)
                    final_total = final_score[0] + final_score[1]
                    signal_total = signal_score[0] + signal_score[1]
                    scored_goals_after_signal = recompute_goals_after_signal(
                        fixture_id,
                        data.get("events", []) or [],
                        signal_score,
                        final_score,
                    )
                    is_plus = (final_total > signal_total) or bool(scored_goals_after_signal)
                    badge = "✅" if is_plus else "❌"
                    header_text = "🚨 Сигнал от админа"
                    text = render_final_message({
                        "data": data,
                        "fixture_id": fixture_id,
                        "signal_home": signal_home,
                        "signal_away": signal_away,
                        "final_home": final_home,
                        "final_away": final_away,
                        "badge": badge,
                        "has_goal_after_signal": is_plus,
                    }, header_text=header_text)
                    logger.info(
                        "[MESSAGE_MODE] FINAL rendered match=%s header=\"%s\" badge=%s final=%s-%s signal=%s-%s",
                        fixture_id,
                        header_text,
                        badge,
                        final_home,
                        final_away,
                        signal_home,
                        signal_away,
                    )
                else:
                    text = build_admin_review_text(
                        data,
                        minute,
                        fixture_id,
                        include_monitoring=True,
                        final_line=final_line
                    )

                # Determine keyboard state:
                # - If match finished: remove keyboard (reply_markup=None removes buttons)
                # - If match ongoing: keep keyboard (so admin can still send/ignore)
                if is_finished:
                    keyboard = None  # Remove buttons on final update
                    logger.info(f"[REVIEW] Final update for fixture={fixture_id}, removing keyboard")
                else:
                    keyboard = build_review_keyboard(fixture_id)  # Keep buttons during match
                    logger.debug(f"[REVIEW] Intermediate update for fixture={fixture_id}, keeping keyboard")

                ok, status, retry_after = _edit_admin_review_message(int(chat_id), int(message_id), text, reply_markup=keyboard)
                if status == "rate_limit":
                    with state_lock:
                        if fixture_id_str in state.get("admin_reviews", {}):
                            state["admin_reviews"][fixture_id_str]["last_edit_ts"] = time.time()
                            mark_state_dirty()
                    continue

                if status in ("ok", "not_modified"):
                    with state_lock:
                        if fixture_id_str in state.get("admin_reviews", {}):
                            state["admin_reviews"][fixture_id_str]["last_edit_ts"] = time.time()
                            if is_finished:
                                state["admin_reviews"][fixture_id_str]["finished"] = True
                            mark_state_dirty()

                    if is_finished:
                        logger.info(
                            f"[REVIEW] finished fixture={fixture_id} result={'WIN' if is_plus else 'LOSS'}"
                        )
                        with state_lock:
                            if fixture_id_str in state.get("review_tracking", {}):
                                state["review_tracking"][fixture_id_str]["done"] = True
                                mark_state_dirty()
                    else:
                        logger.info(f"[REVIEW] updated fixture={fixture_id} minute={minute}")
                elif status == "error":
                    logger.warning(f"[REVIEW] update failed fixture={fixture_id}")

        except Exception as e:
            logger.warning(f"[REVIEW] admin review daemon error: {e}")

    logger.info("[REVIEW] admin review daemon stopped")


def start_admin_review_daemon(client: APISportsMetricsClient):
    """Start admin review daemon thread."""
    global _admin_review_thread
    if _admin_review_thread and _admin_review_thread.is_alive():
        return
    _admin_review_stop.clear()
    t = threading.Thread(target=admin_review_daemon, args=(client, ADMIN_REVIEW_INTERVAL), daemon=True)
    _admin_review_thread = t
    t.start()


def publish_signal_to_channel(fixture_id: int, review_info: Optional[Dict[str, Any]] = None) -> bool:
    """
    Publish signal to channel (used after REVIEW approval).
    
    Args:
        fixture_id: Match fixture ID
        
    Returns:
        True if published successfully, False otherwise
    """
    if review_info is None:
        with state_lock:
            review_queue = state.get("review_queue", {})
            review_info = review_queue.get(str(fixture_id))
    
    if not review_info:
        logger.error(f"[REVIEW] No review info for fixture_id={fixture_id}")
        return False
    
    data = review_info.get("data")
    prob = review_info.get("prob", 0.0)
    minute = review_info.get("minute", 0)
    
    if not data:
        logger.error(f"[REVIEW] No data in review_info for fixture_id={fixture_id}")
        return False
    
    if not validate_match_context_before_send(data):
        return False

    fixture_metrics = data.get("fixture", {}) or {}
    
    if "fixture_id" not in fixture_metrics:
        fixture_metrics["fixture_id"] = fixture_id
        
    try:
        # Get initial score
        initial_score = (
            int((fixture_metrics.get("score_home") or {}).get("value") or 0),
            int((fixture_metrics.get("score_away") or {}).get("value") or 0)
        )
            
        # Calculate probability for display (use existing compute logic)
        res = compute_lambda_and_probability(fixture_metrics, minute)
        prob_actual_90 = review_info.get("prob_90")
        if prob_actual_90 is None:
            prob_actual_90 = res.get("prob_goal_either_to90", 0.0)
        
        # Format initial signal message: immutable snapshot header only
        is_admin_approved = review_info.get("approved_by_admin", False)
        snapshot_data = _build_signal_snapshot_data(
            collected=data,
            prob_display=prob,
            signal_score=initial_score,
            signal_minute=minute,
            prob_display_90=prob_actual_90,
            is_admin_approved=is_admin_approved,
        )
        header_text = build_signal_header(snapshot_data)
        msg = render_live_message(
            current_data=data,
            signal_score=initial_score,
            prob_display=prob,
            prob_display_90=prob_actual_90,
            is_admin_approved=is_admin_approved,
        )
        
        # Send to channel
        mid_msg = send_to_telegram(
            msg,
            match_id=fixture_id,
            score_at_signal=initial_score,
            signal_minute=int(minute),
        )

        if mid_msg:
            save_signal_snapshot_state(
                    match_id=fixture_id,
                    header_text=header_text,
                    signal_score_home=initial_score[0],
                    signal_score_away=initial_score[1],
                    signal_minute=minute,
                    message_id=int(mid_msg),
                    chat_id=int(TELEGRAM_CHAT_ID),
                    prob_to75=prob,
                    prob_to90=prob_actual_90,
                )
            # Calculate initial PressureIndex
            initial_pressure = calculate_pressure_index(fixture_metrics)
            initial_momentum = 0.0
            
            # Extract probability values
            prob_next_15 = res.get("prob_next_15", prob)
            prob_until_end = res.get("prob_until_end", prob)
            league_factor_used = res.get("league_factor", 1.0)
            _score01_final, match_score_value, _score_metrics = compute_match_score_v2(fixture_metrics, league_factor_used)
            
            # Create match snapshot for JSONL logging
            fixture = fixture_metrics
            league_id = fixture.get("league_id", {}).get("value") if isinstance(fixture.get("league_id"), dict) else fixture.get("league_id")
            league_name = fixture.get("league_name", {}).get("value") if isinstance(fixture.get("league_name"), dict) else fixture.get("league_name")
            season = fixture.get("league_season", {}).get("value") if isinstance(fixture.get("league_season"), dict) else fixture.get("league_season")
            home_team_id = fixture.get("team_home_id", {}).get("value") if isinstance(fixture.get("team_home_id"), dict) else fixture.get("team_home_id")
            home_team_name = fixture.get("team_home_name", {}).get("value") if isinstance(fixture.get("team_home_name"), dict) else fixture.get("team_home_name")
            away_team_id = fixture.get("team_away_id", {}).get("value") if isinstance(fixture.get("team_away_id"), dict) else fixture.get("team_away_id")
            away_team_name = fixture.get("team_away_name", {}).get("value") if isinstance(fixture.get("team_away_name"), dict) else fixture.get("team_away_name")
            
            status_obj = fixture.get("status", {}).get("value") if isinstance(fixture.get("status"), dict) else fixture.get("status") or {}
            if isinstance(status_obj, dict):
                status_short = str(status_obj.get("short") or "").upper()
            else:
                status_short = ""
            
            xg_home = float(get_any_metric(fixture, ["expected_goals"], "home") or 0.0)
            xg_away = float(get_any_metric(fixture, ["expected_goals"], "away") or 0.0)
            shots_home = int(get_any_metric(fixture, ["total_shots"], "home") or 0)
            shots_away = int(get_any_metric(fixture, ["total_shots"], "away") or 0)
            shots_on_target_home = int(get_any_metric(fixture, ["shots_on_target"], "home") or 0)
            shots_on_target_away = int(get_any_metric(fixture, ["shots_on_target"], "away") or 0)
            saves_home = int(get_any_metric(fixture, ["saves"], "home") or 0)
            saves_away = int(get_any_metric(fixture, ["saves"], "away") or 0)
            shots_in_box_home = int(get_any_metric(fixture, ["shots_inside_box"], "home") or 0)
            shots_in_box_away = int(get_any_metric(fixture, ["shots_inside_box"], "away") or 0)
            corners_home = int(get_any_metric(fixture, ["corner_kicks"], "home") or 0)
            corners_away = int(get_any_metric(fixture, ["corner_kicks"], "away") or 0)
            possession_home = float(get_any_metric(fixture, ["ball_possession"], "home") or 0.0)
            possession_away = float(get_any_metric(fixture, ["ball_possession"], "away") or 0.0)
            
            pressure_index = calculate_pressure_index(fixture)
            xg_delta = xg_home - xg_away
            shots_ratio = (shots_on_target_home + 1.0) / (shots_on_target_away + 1.0) if shots_on_target_away + 1.0 > 0 else 1.0
            
            total_saves = saves_home + saves_away
            total_goals = initial_score[0] + initial_score[1]
            total_sot = shots_on_target_home + shots_on_target_away
            if total_sot <= 1.0:
                save_stress = 0.0
            else:
                save_raw = (total_saves + 0.5 * total_goals) / 4.0
                volume_factor_saves = min(1.0, total_sot / 4.0)
                save_stress = min(save_raw * volume_factor_saves, 1.0)
            
            possession_diff = abs(possession_home - possession_away)
            if possession_diff > 1.0:
                possession_pressure = possession_diff / 100.0
            else:
                possession_pressure = possession_diff
            
            league_factor = float(res.get("league_factor", 1.0) or 1.0)
            team_mix_factor = float(res.get("team_mix_factor", 1.0) or 1.0)
            combined_M = float(res.get("combined_m", 1.0) or 1.0)
            
            snapshot = {
                "match_id": fixture_id,
                "league_id": league_id,
                "league_name": league_name,
                "season": season,
                "home_team_id": home_team_id,
                "home_team_name": home_team_name,
                "away_team_id": away_team_id,
                "away_team_name": away_team_name,
                "minute": minute,
                "status_short": status_short,
                "signal_ts_utc": datetime.utcnow().isoformat(),
                "score_home": initial_score[0],
                "score_away": initial_score[1],
                "xg_home": xg_home,
                "xg_away": xg_away,
                "shots_home": shots_home,
                "shots_away": shots_away,
                "shots_on_target_home": shots_on_target_home,
                "shots_on_target_away": shots_on_target_away,
                "saves_home": saves_home,
                "saves_away": saves_away,
                "shots_in_box_home": shots_in_box_home,
                "shots_in_box_away": shots_in_box_away,
                "corners_home": corners_home,
                "corners_away": corners_away,
                "possession_home": possession_home,
                "possession_away": possession_away,
                "pressure_index": pressure_index,
                "xg_delta": xg_delta,
                "shots_ratio": shots_ratio,
                "save_stress": save_stress,
                "possession_pressure": possession_pressure,
                "prob_goal_75": prob,
                "prob_goal_90": prob_actual_90,
                "league_factor": league_factor,
                "team_mix_factor": team_mix_factor,
                "combined_M": combined_M,
                "match_score": match_score_value,
                "signal_source": "admin" if is_admin_approved else "bot",
                "schema_version": "1.0",
                "prob_model_version": "v2",
                "match_score_version": "v2.5"
            }
            
            save_snapshot(snapshot)
            
            # Save match data to Google Sheets
            save_match_to_sheet(
                fixture_id, data, minute, prob_next_15, prob_until_end, 
                signal_sent=True,
                pressure_index=initial_pressure,
                momentum=initial_momentum,
                signal_source="admin" if is_admin_approved else "bot",
                league_factor=league_factor_used,
                match_score=match_score_value,
            )
            
            # Update state
            with state_lock:
                state.setdefault("match_initial_score", {})[str(fixture_id)] = initial_score
                state.setdefault("match_initial_minute", {})[str(fixture_id)] = minute
                state.setdefault("match_goal_status", {})[str(fixture_id)] = {
                    "valid_goals_after_send": False,
                    "last_goal_time": None,
                    "persistent_goal_line": None,
                    "final_line": None,
                    "cancellations": [],
                    "score": initial_score,
                    "processed_ids": [],
                    "finalized": False,
                    "last_reported_minute": minute,
                    "last_pressure_index": initial_pressure,
                    "approved_by_admin": is_admin_approved
                }
                state.setdefault("match_processed_event_ids", {})[str(fixture_id)] = []
                state.setdefault("match_sent_at", {})[str(fixture_id)] = time.time()
                state.setdefault("match_prob_status", {})[str(fixture_id)] = prob
                
                # Save signal date
                signal_date = get_msk_date()
                msk_time = get_msk_datetime().strftime("%H:%M:%S")
                state.setdefault("match_signal_info", {})[str(fixture_id)] = {
                    "signal_date": signal_date,
                    "is_finished": False
                }
                logger.info(f"[STATS] Match {fixture_id}: signal sent at {msk_time} MSK, assigned to date {signal_date}")
                
                if fixture_id not in state.setdefault("monitored_matches", []):
                    state["monitored_matches"].append(fixture_id)
                
                mark_state_dirty()
            
            logger.info(f"[REVIEW] Signal published to channel for fixture_id={fixture_id}")
            update_persistent_fixture_tracking(fixture_id, fixture_metrics, sent_text=msg)
            return True
        else:
            logger.error(f"[REVIEW] Failed to send message to channel for fixture_id={fixture_id}")
            return False
    
    except Exception:
        logger.exception(f"[REVIEW] Error publishing signal for fixture_id={fixture_id}")
        return False

# -------------------------
# Daily statistics tracking
# -------------------------
def get_msk_date() -> str:
    """Get current date in MSK timezone as YYYY-MM-DD string."""
    return now_msk().strftime("%Y-%m-%d")

def get_msk_datetime() -> datetime:
    """Get current datetime in MSK timezone."""
    return now_msk()

def update_daily_stats(match_id: int, had_goal_after_signal: bool, first_goal_minute: Optional[int] = None):
    """
    Update daily statistics when match is finalized.
    
    Statistics are tracked per signal_date (determined at signal send time).
    Each match is counted towards the day when its signal was sent,
    regardless of when the match actually finishes.
    """
    # Get signal date for this match
    with state_lock:
        signal_date = state.get("match_signal_info", {}).get(str(match_id), {}).get("signal_date")
    
    if not signal_date:
        logger.warning(f"[STATS] Match {match_id} has no signal_date - using current date as fallback")
        signal_date = get_msk_date()
    
    with state_lock:
        state.setdefault("daily_stats", {})
        if signal_date not in state["daily_stats"]:
            state["daily_stats"][signal_date] = {
                "signals": 0,
                "plus": 0,
                "minus": 0
            }
        
        stats = state["daily_stats"][signal_date]
        stats["signals"] += 1
        
        if had_goal_after_signal:
            stats["plus"] += 1
        else:
            stats["minus"] += 1
        
        mark_state_dirty()
        logger.info(f"[STATS] Updated for {signal_date}: signals={stats['signals']}, plus={stats['plus']}, minus={stats['minus']}")

def calculate_period_stats(match_ids: List[str]) -> Dict[str, Any]:
    """
    Calculate statistics for a list of matches.
    
    SINGLE SOURCE OF TRUTH: counts statistics ONLY from match state,
    not from daily_stats counters.
    
    CRITICAL: Only FINISHED matches are counted in statistics.
    Unfinished matches are completely excluded from plus/minus calculation.
    
    Args:
        match_ids: List of match IDs to analyze
        
    Returns:
        Dictionary with aggregated statistics:
        - signals: total number of FINISHED matches (= number of signals)
        - plus: matches with first goal after signal
        - minus: matches without first goal after signal
        - before_75: matches with goal before/at 75 min
        - avg_first_goal_minute: average minute of first goal (rounded, or None)
        - count_до_60: matches with first goal <= 60 min
        - count_до_75: matches with first goal 61-75 min (excluding <=60)
        - count_after_75: matches with first goal > 75 min
    """
    with state_lock:
        match_goal_status = state.get("match_goal_status", {})
        match_signal_info = state.get("match_signal_info", {})
        
        # Filter: only count FINISHED matches
        finished_match_ids = [
            match_id for match_id in match_ids
            if match_signal_info.get(str(match_id), {}).get("is_finished", False)
        ]
        
        # Log excluded unfinished matches
        excluded_count = len(match_ids) - len(finished_match_ids)
        if excluded_count > 0:
            logger.info(f"[STATS] Excluded {excluded_count} unfinished match(es) from statistics calculation")
        
        # Count signals (only FINISHED matches)
        total_signals = len(finished_match_ids)
        
        # Count matches with goals and collect goal data
        first_goal_minutes = []
        count_до_60 = 0
        count_до_75_only = 0  # 61-75 range
        count_after_75 = 0  # > 75 range
        
        for match_id in finished_match_ids:
            mstatus = match_goal_status.get(match_id, {})
            
            # Check if match has first goal after signal
            # ТОЛЬКО реальные целочисленные значения, без дефолтов
            goal_minute = mstatus.get("first_goal_minute_after_signal")
            if isinstance(goal_minute, int):
                # Записываем ТОЛЬКО если это целое число (int)
                first_goal_minutes.append(goal_minute)
                
                # Use stored flags if available, otherwise calculate
                if "goal_before_60" in mstatus:
                    if mstatus.get("goal_before_60"):
                        count_до_60 += 1
                    elif mstatus.get("goal_before_75") and not mstatus.get("goal_before_60"):
                        count_до_75_only += 1
                    if mstatus.get("goal_after_75"):
                        count_after_75 += 1
                else:
                    # Fallback: calculate from minute
                    if goal_minute <= 60:
                        count_до_60 += 1
                    elif goal_minute <= 75:
                        count_до_75_only += 1
                    else:  # goal_minute > 75
                        count_after_75 += 1
        
        # Calculate plus/minus (only from FINISHED matches)
        total_plus = len(first_goal_minutes)
        total_minus = total_signals - total_plus
        
        # Count matches with goal before/at 75 (for legacy "До 75 минуты" metric)
        # Only count FINISHED matches
        before_75 = sum(
            1 for match_id in finished_match_ids
            if match_goal_status.get(match_id, {}).get("goal_before_75", False)
        )
        
        # Calculate average first goal minute
        # Средняя минута = None если нет реальных данных
        avg_first_goal_minute = (
            round(sum(first_goal_minutes) / len(first_goal_minutes))
            if first_goal_minutes else None
        )
    
    return {
        "signals": total_signals,
        "plus": total_plus,
        "minus": total_minus,
        "before_75": before_75,
        "avg_first_goal_minute": avg_first_goal_minute,
        "count_до_60": count_до_60,
        "count_до_75": count_до_75_only,
        "count_after_75": count_after_75,
        "has_goals": len(first_goal_minutes) > 0
    }

def gsheets_fetch_rows_as_dicts() -> list:
    """
    Fetch all rows from Google Sheets and return as list of dicts.
    Keys are column headers (lowercase), values are cell values.
    Returns empty list if Sheets not available.
    """
    global _gsheets_sheet
    
    if not GSHEETS_AVAILABLE or _gsheets_sheet is None:
        logger.warning("[SHEETS] Google Sheets not available")
        return []
    
    try:
        with _gsheets_lock:
            all_values = _gsheets_sheet.get_all_values()
        
        if not all_values or len(all_values) < 2:
            logger.warning("[SHEETS] Sheet is empty or has no header row")
            return []
        
        # First row is header
        headers = [h.strip().lower() for h in all_values[0]]
        
        # Convert rows to dicts
        rows_dicts = []
        for row in all_values[1:]:
            if not row:  # Skip empty rows
                continue
            row_dict = {}
            for i, header in enumerate(headers):
                value = row[i] if i < len(row) else ""
                row_dict[header] = value
            rows_dicts.append(row_dict)
        
        return rows_dicts
    except Exception:
        logger.exception("[SHEETS] Error fetching rows from Google Sheets")
        return []


def gsheets_fetch_recent_rows_as_dicts(max_rows: int = 2000) -> list:
    """
    Fetch only the latest rows from Google Sheets as list of dicts.
    Reads header row plus tail range to avoid scanning the whole sheet.
    """
    global _gsheets_sheet

    if not GSHEETS_AVAILABLE or _gsheets_sheet is None:
        logger.warning("[SHEETS] Google Sheets not available")
        return []

    try:
        with _gsheets_lock:
            headers_raw = _gsheets_sheet.row_values(1)
            if not headers_raw:
                return []
            headers = [str(h or "").strip().lower() for h in headers_raw]

            row_count = int(getattr(_gsheets_sheet, "row_count", 0) or 0)
            col_count = int(getattr(_gsheets_sheet, "col_count", 0) or len(headers))
            if row_count <= 1:
                return []

            start_row = max(2, row_count - max(1, int(max_rows or 1)) + 1)
            end_col = col_index_to_letter(max(1, col_count))
            rng = f"A{start_row}:{end_col}{row_count}"
            values = _gsheets_sheet.get(rng)

        rows_dicts: List[Dict[str, Any]] = []
        for row in values or []:
            if not row:
                continue
            if not any(str(x).strip() for x in row):
                continue
            row_dict: Dict[str, Any] = {}
            for i, header in enumerate(headers):
                row_dict[header] = row[i] if i < len(row) else ""
            rows_dicts.append(row_dict)
        return rows_dicts
    except Exception:
        logger.exception("[SHEETS] Error fetching recent rows from Google Sheets")
        return []


def _estimate_match_start_msk(row: Dict[str, Any]) -> Optional[datetime]:
    signal_dt_msk = _parse_sheet_datetime_msk(row.get("время сигнала (utc)", ""))
    signal_minute = _parse_sheet_int(row.get("минута сигнала", ""))
    if not signal_dt_msk or signal_minute is None:
        return None
    return signal_dt_msk - timedelta(minutes=signal_minute)


def get_report_window_msk(report_date) -> Tuple[datetime, datetime]:
    """
    Get [start, end) report window for a Moscow calendar day.
    """
    if isinstance(report_date, datetime):
        target_date = report_date.astimezone(_MSK_TZ).date()
    else:
        target_date = report_date
    start_msk = datetime.combine(target_date, datetime.min.time()).replace(tzinfo=_MSK_TZ)
    end_msk = start_msk + timedelta(days=1)
    return start_msk, end_msk


def _parse_api_datetime_utc(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    try:
        s = str(value).strip()
        if not s:
            return None
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _get_relevant_report_match_ids(report_date_msk) -> List[int]:
    """
    Return fixture IDs relevant for daily stats.

    Relevant match:
    - was sent to channel (exists in state['sent_matches'])
    - belongs to report date by signal_date in match_signal_info
    """
    if isinstance(report_date_msk, str):
        report_date_str = report_date_msk
    elif isinstance(report_date_msk, datetime):
        report_date_str = report_date_msk.astimezone(_MSK_TZ).date().strftime("%Y-%m-%d")
    else:
        report_date_str = report_date_msk.strftime("%Y-%m-%d")

    relevant_ids: Set[int] = set()
    with state_lock:
        sent_matches = state.get("sent_matches", {})
        match_signal_info = state.get("match_signal_info", {})

        if not isinstance(sent_matches, dict):
            return []

        for fixture_id_str in sent_matches.keys():
            try:
                fixture_id = int(fixture_id_str)
            except Exception:
                continue

            info = match_signal_info.get(str(fixture_id), {})
            signal_date = str((info or {}).get("signal_date") or "").strip()
            if signal_date == report_date_str:
                relevant_ids.add(fixture_id)

    return sorted(relevant_ids)


def get_kickoff_utc(fixture_id: int, client: Optional[APISportsMetricsClient] = None) -> Tuple[Optional[datetime], str]:
    """
    Get fixture kickoff datetime in UTC with persist_matches cache.
    Returns (kickoff_utc, source) where source in {persist, api, missing}.
    """
    fixture_key = str(int(fixture_id))

    with matches_state_lock:
        rec = matches_state.get(fixture_key)
        if isinstance(rec, dict):
            cached = rec.get("kickoff_utc")
            parsed = _parse_api_datetime_utc(cached)
            if parsed is not None:
                return parsed, "persist"

    if client is None:
        return None, "missing"

    try:
        raw_fixture = client.fetch_fixture(int(fixture_id)) or {}
        fixture_obj = raw_fixture.get("fixture") if isinstance(raw_fixture, dict) else {}
        if not isinstance(fixture_obj, dict):
            fixture_obj = {}
        kickoff_raw = fixture_obj.get("date") or fixture_obj.get("timestamp")

        kickoff_utc: Optional[datetime] = None
        if isinstance(kickoff_raw, (int, float)):
            kickoff_utc = datetime.fromtimestamp(float(kickoff_raw), tz=timezone.utc)
        else:
            kickoff_utc = _parse_api_datetime_utc(kickoff_raw)

        if kickoff_utc is None:
            return None, "missing"

        with matches_state_lock:
            rec2 = matches_state.setdefault(fixture_key, {})
            if not isinstance(rec2, dict):
                rec2 = {}
                matches_state[fixture_key] = rec2
            rec2["kickoff_utc"] = kickoff_utc.isoformat()
            rec2["last_updated_ts"] = float(time.time())
        save_matches_state()
        return kickoff_utc, "api"
    except Exception:
        logger.exception("[STATS] kickoff fetch failed id=%s", fixture_id)
        return None, "missing"


def _collect_report_matches_finish_state(
    report_date_msk,
    client: Optional[APISportsMetricsClient] = None,
) -> Dict[str, Any]:
    """
    Collect finish-state for relevant report matches only.

    Relevant match = sent to channel and belongs to report day by signal_date.
    """
    if isinstance(report_date_msk, str):
        target_date = datetime.strptime(report_date_msk, "%Y-%m-%d").date()
    elif isinstance(report_date_msk, datetime):
        target_date = report_date_msk.astimezone(_MSK_TZ).date()
    else:
        target_date = report_date_msk

    start_msk, end_msk = get_report_window_msk(target_date)
    local_client = client or APISportsMetricsClient(api_key=API_FOOTBALL_KEY, host=API_FOOTBALL_HOST, cache_ttl=CACHE_TTL)

    relevant_ids = _get_relevant_report_match_ids(target_date)
    all_items: List[Dict[str, Any]] = []
    for fixture_id in relevant_ids:
        is_finished = False
        with state_lock:
            info = state.get("match_signal_info", {}).get(str(fixture_id), {})
            is_finished = bool((info or {}).get("is_finished", False))

        if not is_finished:
            is_finished = _is_finished_match_for_stats(fixture_id, {}, local_client)

        kickoff_utc, _kickoff_source = get_kickoff_utc(fixture_id, client=local_client)
        kickoff_msk = kickoff_utc.astimezone(_MSK_TZ) if kickoff_utc is not None else None

        all_items.append(
            {
                "fixture_id": int(fixture_id),
                "kickoff_utc": kickoff_utc,
                "kickoff_msk": kickoff_msk,
                "status_short": "FT" if is_finished else "LIVE",
                "is_finished": bool(is_finished),
            }
        )

    unfinished_ids = sorted([int(x["fixture_id"]) for x in all_items if not bool(x.get("is_finished", False))])
    total = len(all_items)
    finished = total - len(unfinished_ids)

    return {
        "report_date": target_date.strftime("%Y-%m-%d"),
        "start_msk": start_msk,
        "end_msk": end_msk,
        "query_dates": [],
        "matches": all_items,
        "report_matches_total": total,
        "report_matches_finished": finished,
        "unfinished_ids": unfinished_ids,
        "unfinished_matches": len(unfinished_ids),
        "all_finished": len(unfinished_ids) == 0,
    }


def are_all_report_matches_finished(
    report_date_msk,
    client: Optional[APISportsMetricsClient] = None,
) -> bool:
    """
    Return True only when ALL fixtures with kickoff_msk in report day window are finished.
    Finished statuses are defined by FINISHED_STATUSES: FT, AET, PEN.
    """
    summary = _collect_report_matches_finish_state(report_date_msk=report_date_msk, client=client)
    return bool(summary.get("all_finished", False))


def _row_has_final_data(row: Dict[str, Any]) -> bool:
    goal_flag = str(row.get("гол до конца матча", "")).strip()
    if goal_flag in ("0", "1"):
        return True
    first_goal_min = str(row.get("минута первого гола", "")).strip()
    if first_goal_min and first_goal_min.upper() != "N/A":
        return True
    first_goal_time = str(row.get("время до следующего гола", "")).strip()
    if first_goal_time and first_goal_time.upper() != "N/A":
        return True
    score_val = str(row.get("счет", "")).strip()
    if score_val and "-" in score_val:
        return True
    return False


def _is_finished_match_for_stats(fixture_id: int, row: Dict[str, Any], client: Optional[APISportsMetricsClient]) -> bool:
    if _row_has_final_data(row):
        return True

    fixture_key = str(int(fixture_id))
    with matches_state_lock:
        rec = matches_state.get(fixture_key)
        if isinstance(rec, dict):
            if bool(rec.get("is_finished", False)):
                return True
            status_short = str(rec.get("last_status") or "").upper()
            if status_short in ("FT", "AET", "PEN", "CANC", "ABD", "AWD", "WO"):
                return True

    if client is None:
        return False

    try:
        raw_fixture = client.fetch_fixture(int(fixture_id)) or {}
        fixture_obj = raw_fixture.get("fixture") if isinstance(raw_fixture, dict) else {}
        status_obj = fixture_obj.get("status") if isinstance(fixture_obj, dict) else {}
        status_short = ""
        status_long = ""
        if isinstance(status_obj, dict):
            status_short = str(status_obj.get("short") or "").upper()
            status_long = str(status_obj.get("long") or "").strip().lower()

        is_finished = (
            status_short in ("FT", "AET", "PEN", "CANC", "ABD", "AWD", "WO")
            or status_long in ("match finished", "finished")
        )

        if is_finished:
            with matches_state_lock:
                rec2 = matches_state.setdefault(fixture_key, {})
                if not isinstance(rec2, dict):
                    rec2 = {}
                    matches_state[fixture_key] = rec2
                rec2["is_finished"] = True
                rec2["last_status"] = status_short or "FT"
                rec2["last_updated_ts"] = float(time.time())
            save_matches_state()
        return is_finished
    except Exception:
        logger.exception("[STATS] finished status fetch failed id=%s", fixture_id)
        return False


def get_candidate_report_date_msk(now_dt_msk: Optional[datetime] = None):
    now_val = now_dt_msk.astimezone(_MSK_TZ) if isinstance(now_dt_msk, datetime) else now_msk()
    return now_val.date() - timedelta(days=1)


def collect_report_matches(
    report_date_msk,
    client: Optional[APISportsMetricsClient] = None,
    max_rows: int = 2500,
) -> Dict[str, Any]:
    if isinstance(report_date_msk, str):
        target_date = datetime.strptime(report_date_msk, "%Y-%m-%d").date()
    elif isinstance(report_date_msk, datetime):
        target_date = report_date_msk.astimezone(_MSK_TZ).date()
    else:
        target_date = report_date_msk

    start_msk, end_msk = get_report_window_msk(target_date)
    rows = gsheets_fetch_recent_rows_as_dicts(max_rows=max_rows)
    rows_loaded = len(rows)
    relevant_ids = set(_get_relevant_report_match_ids(target_date))

    unique_by_match: Dict[int, Tuple[int, Dict[str, Any]]] = {}
    duplicates_removed = 0
    for idx, row in enumerate(rows):
        raw_match_id = str(row.get("id матча", "")).strip()
        if not raw_match_id:
            continue
        try:
            match_id = int(float(raw_match_id))
        except Exception:
            continue

        previous = unique_by_match.get(match_id)
        if previous is None:
            unique_by_match[match_id] = (idx, row)
            continue

        _, prev_row = previous
        prev_final = _row_has_final_data(prev_row)
        cur_final = _row_has_final_data(row)
        if cur_final and not prev_final:
            unique_by_match[match_id] = (idx, row)
            duplicates_removed += 1
        elif cur_final == prev_final and idx >= previous[0]:
            unique_by_match[match_id] = (idx, row)
            duplicates_removed += 1
        else:
            duplicates_removed += 1

    matches_in_window: List[Dict[str, Any]] = []
    for match_id, (_idx, row) in unique_by_match.items():
        if match_id not in relevant_ids:
            continue

        kickoff_utc, source = get_kickoff_utc(match_id, client=client)
        if kickoff_utc is None:
            continue
        kickoff_msk = kickoff_utc.astimezone(_MSK_TZ)
        is_finished = _is_finished_match_for_stats(match_id, row, client)
        matches_in_window.append(
            {
                "match_id": int(match_id),
                "row": row,
                "kickoff_utc": kickoff_utc,
                "kickoff_msk": kickoff_msk,
                "source": source,
                "is_finished": bool(is_finished),
            }
        )

    return {
        "report_date": target_date,
        "start_msk": start_msk,
        "end_msk": end_msk,
        "rows_loaded": rows_loaded,
        "duplicates_removed": duplicates_removed,
        "unique_match_ids": len(unique_by_match),
        "matches": matches_in_window,
    }


def are_all_matches_finished(matches: List[Dict[str, Any]]) -> Tuple[bool, List[int]]:
    unfinished_ids: List[int] = []
    for item in matches or []:
        if not bool(item.get("is_finished", False)):
            try:
                unfinished_ids.append(int(item.get("match_id")))
            except Exception:
                continue
    return (len(unfinished_ids) == 0, unfinished_ids)


def build_daily_report(report_date_msk, matches: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if isinstance(report_date_msk, str):
        target_date = datetime.strptime(report_date_msk, "%Y-%m-%d").date()
    elif isinstance(report_date_msk, datetime):
        target_date = report_date_msk.astimezone(_MSK_TZ).date()
    else:
        target_date = report_date_msk

    if not matches:
        return None

    plus = 0
    minus = 0
    goals_u60 = 0
    goals_u75 = 0
    goals_after75 = 0
    first_goal_minutes: List[int] = []

    for item in matches:
        row = item.get("row") or {}
        goal_flag = str(row.get("гол до конца матча", "")).strip()
        if goal_flag == "1":
            plus += 1
        elif goal_flag == "0":
            minus += 1

        minute_val = _parse_sheet_int(row.get("минута первого гола", ""))
        if minute_val is not None:
            first_goal_minutes.append(int(minute_val))
            if minute_val <= 60:
                goals_u60 += 1
            elif minute_val <= 75:
                goals_u75 += 1
            else:
                goals_after75 += 1

    success_pct = None
    denom = plus + minus
    if denom > 0:
        success_pct = (plus / denom) * 100.0

    avg_first_goal_minute = None
    if first_goal_minutes:
        avg_first_goal_minute = round(sum(first_goal_minutes) / len(first_goal_minutes))

    formatted_date = target_date.strftime("%d.%m.%Y")
    lines = [
        f"📊 Итоги дня | {formatted_date}",
        "",
        f"🚨 Сигналов: {len(matches)}",
        "",
    ]

    if plus > 0 or minus > 0:
        lines.append(f"✅ Плюсов: {plus}")
        lines.append(f"❌ Минусов: {minus}")
        lines.append("")
        if success_pct is not None:
            lines.append(f"📈 Проходимость дня: {success_pct:.0f}%")

    if first_goal_minutes:
        lines.append("")
        lines.append("⚽ Реализация:")
        lines.append(f"• До 60 минуты: {goals_u60}")
        lines.append(f"• До 75 минуты: {goals_u75}")
        lines.append(f"• После 75 минуты: {goals_after75}")

    if avg_first_goal_minute is not None:
        lines.append("")
        lines.append(f"⏱️ Средняя минута первого гола: {avg_first_goal_minute}′")

    return {
        "message": "\n".join(lines),
        "report_date": target_date.strftime("%Y-%m-%d"),
        "final_matches_count": len(matches),
        "plus": plus,
        "minus": minus,
        "goals_u60": goals_u60,
        "goals_u75": goals_u75,
        "goals_after75": goals_after75,
        "avg_first_goal_minute": avg_first_goal_minute,
        "success_pct": success_pct,
    }


def build_daily_report_from_sheets(
    report_date_msk,
    client: Optional[APISportsMetricsClient] = None,
    lookback_days: int = 3,
    max_rows: int = 2000,
) -> Optional[Dict[str, Any]]:
    # keep signature for compatibility; lookback_days intentionally ignored in strict kickoff-day logic
    collected = collect_report_matches(report_date_msk=report_date_msk, client=client, max_rows=max_rows)
    matches = list(collected.get("matches") or [])
    all_finished, _unfinished_ids = are_all_matches_finished(matches)
    
    logger.info(f"[GSHEETS_STATS] report_date={report_date_msk} matches={len(matches)} all_finished={all_finished}")
    
    if not matches or not all_finished:
        logger.info(f"[GSHEETS_STATS] report not ready: matches={len(matches)} all_finished={all_finished}")
        return None
    return build_daily_report(report_date_msk=report_date_msk, matches=matches)


def filter_rows_by_moscow_day(rows: list, target_date_msk) -> list:
    """
    Filter rows by Moscow date (00:00:00 to 23:59:59 MSK).
    
    Args:
        rows: List of row dicts from Google Sheets
        target_date_msk: date object (YYYY-MM-DD) or datetime in MSK timezone
        
    Returns:
        List of rows that fall within the target day in MSK timezone
    """
    from datetime import date as date_class
    
    if isinstance(target_date_msk, datetime):
        target_date = target_date_msk.date()
    else:
        target_date = target_date_msk
    
    # Define day window in MSK
    day_start = datetime.combine(target_date, datetime.min.time())
    day_start = day_start.replace(tzinfo=_MSK_TZ)
    
    day_end = datetime.combine(target_date, datetime.max.time())
    day_end = day_end.replace(tzinfo=_MSK_TZ)
    
    filtered = []
    for row in rows:
        start_dt_msk = _estimate_match_start_msk(row)
        if start_dt_msk and day_start <= start_dt_msk <= day_end:
            filtered.append(row)
    
    return filtered


def compute_day_stats_from_rows(rows: list) -> dict:
    """
    Compute statistics from filtered rows.
    
    Rows dict keys are lowercase Russian (e.g., "гол до конца матча", "минута первого гола").
    This matches output from gsheets_fetch_recent_rows_as_dicts() which lowercases all headers.
    
    Returns dict with:
        - total_signals: number of rows
        - plus: matches with 'гол до конца матча' == "1"
        - minus: matches with 'гол до конца матча' == "0"
        - goals_u60: matches with 'минута первого гола' <= 60
        - goals_u75: matches with 'минута первого гола' in 61..75
        - goals_after75: matches with 'минута первого гола' > 75
        - avg_first_goal_minute: average goal minute (or None)
        - success_pct: (plus / (plus+minus)) * 100 (or None if plus+minus==0)
        - has_goals: True if any matches have goal info
    """
    total_signals = len(rows)
    plus = 0
    minus = 0
    first_goal_minutes = []
    goals_u60 = 0
    goals_u75 = 0
    goals_after75 = 0
    
    rows_processed = 0
    rows_with_goals = 0
    
    for row in rows:
        rows_processed += 1
        
        # Count plus/minus by 'гол до конца матча' field (lowercase, from gsheets_fetch_recent_rows_as_dicts)
        goal_flag = str(row.get("гол до конца матча", "")).strip()
        if goal_flag == "1":
            plus += 1
        elif goal_flag == "0":
            minus += 1
        # If N/A or empty - skip (don't count in plus/minus)
        
        # Count goal distribution by 'минута первого гола' (lowercase)
        minute_val = row.get("минута первого гола", "")
        minute_str = str(minute_val).strip()
        
        if minute_str and minute_str.upper() != "N/A":
            try:
                minute_int = int(float(minute_str))
                first_goal_minutes.append(minute_int)
                rows_with_goals += 1
                
                if minute_int <= 60:
                    goals_u60 += 1
                elif minute_int <= 75:
                    goals_u75 += 1
                else:
                    goals_after75 += 1
            except (ValueError, TypeError):
                # Skip invalid minute values
                pass
    
    # Calculate success percentage
    total_finished = plus + minus
    success_pct = None
    if total_finished > 0:
        success_pct = (plus / total_finished) * 100
    
    # Calculate average first goal minute
    avg_first_goal_minute = None
    if first_goal_minutes:
        avg_first_goal_minute = round(sum(first_goal_minutes) / len(first_goal_minutes))
    
    logger.debug(f"[GSHEETS_STATS] compute_day: processed={rows_processed} with_goals={rows_with_goals} plus={plus} minus={minus} success={success_pct}%")
    
    return {
        "total_signals": total_signals,
        "plus": plus,
        "minus": minus,
        "goals_u60": goals_u60,
        "goals_u75": goals_u75,
        "goals_after75": goals_after75,
        "avg_first_goal_minute": avg_first_goal_minute,
        "success_pct": success_pct,
        "has_goals": len(first_goal_minutes) > 0
    }


def _group_rows_by_match_id(rows: list) -> Dict[str, List[Dict[str, Any]]]:
    """Group rows by match ID (using lowercase 'id матча' key from Google Sheets)."""
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        match_id = str(row.get("id матча", "")).strip()
        if not match_id:
            continue
        grouped.setdefault(match_id, []).append(row)
    return grouped


def _get_unfinished_match_ids(rows: list) -> List[str]:
    """
    Get match IDs that have unfinalized outcomes.
    Checks the 'гол до конца матча' field (lowercase Russian).
    """
    grouped = _group_rows_by_match_id(rows)
    unfinished = []
    for match_id, items in grouped.items():
        all_finished = True
        for row in items:
            goal_flag = str(row.get("гол до конца матча", "")).strip()
            if goal_flag not in ("0", "1"):
                all_finished = False
                break
        if not all_finished:
            unfinished.append(match_id)
    return unfinished


def format_daily_stats_message(date_str: str) -> str:
    """
    Format daily statistics message for Telegram.
    Uses Google Sheets as single source of truth.
    
    Args:
        date_str: Date in YYYY-MM-DD format (MSK)
        
    Returns:
        Formatted message text or None if no data
    """
    try:
        payload = build_daily_report_from_sheets(date_str, client=None, lookback_days=3, max_rows=2000)
        if not payload:
            return None
        return str(payload.get("message") or "") or None
    except Exception:
        logger.exception("[SHEETS] Failed to format daily stats message for %s", date_str)
        return None

def format_red_eye_strategy_message() -> str:
    """Format RED EYE strategy text for Telegram."""
    return (
        "👁️♦️ RED EYE | Goal Signals\n"
        "\n"
        "Стратегия основана на поэтапных ставках до забитого гола\n"
        "и рассчитана на работу на дистанции.\n"
        "\n"
        "⸻\n"
        "\n"
        "♦️ Общие правила\n"
        "\n"
        "На один матч выделяется 40% от банка.\n"
        "Все расчёты ведутся от текущего баланса.\n"
        "Сигнал бота — это начало сценария, а не одна ставка.\n"
        "\n"
        "Каждый шаг используется только при соблюдении\n"
        "минимального коэффициента.\n"
        "Если коэффициент ниже — шаг пропускается.\n"
        "\n"
        "⸻\n"
        "\n"
        "♦️ Пример при банке 1000 ₽\n"
        "\n"
        "На матч выделяется 400 ₽.\n"
        "\n"
        "⸻\n"
        "\n"
        "♦️ Шаг 1 — ранний вход (15–30 мин)\n"
        "\n"
        "Ставка: 10% (40 ₽)\n"
        "Событие: Гол в первом тайме\n"
        "Минимальный коэффициент: 1.50\n"
        "\n"
        "Если гол забит — серия закрыта с прибылью.\n"
        "\n"
        "⸻\n"
        "\n"
        "♦️ Шаг 2 — основной вход (30–55 мин)\n"
        "\n"
        "Ставка: 20% (80 ₽)\n"
        "Событие: Гол до 60-й минуты\n"
        "Минимальный коэффициент: 1.75\n"
        "\n"
        "⸻\n"
        "\n"
        "♦️ Шаг 3 — усиление (55–75 мин)\n"
        "\n"
        "Ставка: 30% (120 ₽)\n"
        "Событие: Гол до 75-й минуты\n"
        "Минимальный коэффициент: 2.20\n"
        "\n"
        "Если коэффициент ниже — шаг пропускается.\n"
        "\n"
        "⸻\n"
        "\n"
        "♦️ Шаг 4 — финальный вход (после 75 мин)\n"
        "\n"
        "Ставка: 40% (160 ₽)\n"
        "Событие: ТБ 0.5 / Гол до конца матча\n"
        "Минимальный коэффициент: 2.65\n"
        "\n"
        "Последняя попытка закрыть серию в плюс.\n"
        "\n"
        "⸻\n"
        "\n"
        "♦️ Если вы зашли в матч позже\n"
        "\n"
        "Пропущенные шаги не догоняются.\n"
        "Используется текущий шаг по минуте матча.\n"
        "\n"
        "⸻\n"
        "\n"
        "♦️ Если матч проигран\n"
        "\n"
        "Матч без голов = минус 40% банка.\n"
        "Следующий матч считается от нового баланса.\n"
        "\n"
        "В среднем один минус отыгрывается за 5–9\n"
        "плюсовых матчей.\n"
        "\n"
        "⸻\n"
        "\n"
        "♦️ Ключевые принципы\n"
        "\n"
        "Коэффициент важнее ставки.\n"
        "Пропуск шага — часть стратегии.\n"
        "Дисциплина важнее эмоций.\n"
        "\n"
        "⸻\n"
        "\n"
        "♦️ Итог\n"
        "\n"
        "Бот указывает направление.\n"
        "Результат определяет дисциплина."
    )

_MSK_TZ = MSK
_MONTH_NAMES_RU = {
    1: "Январь",
    2: "Февраль",
    3: "Март",
    4: "Апрель",
    5: "Май",
    6: "Июнь",
    7: "Июль",
    8: "Август",
    9: "Сентябрь",
    10: "Октябрь",
    11: "Ноябрь",
    12: "Декабрь",
}

def _parse_sheet_datetime_msk(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    s = str(value).strip()
    if not s or s == "N/A":
        return None

    # Google Sheets serial date number
    try:
        if s.replace(".", "", 1).isdigit():
            serial = float(s)
            base = datetime(1899, 12, 30, tzinfo=_MSK_TZ)
            return base + timedelta(days=serial)
    except Exception:
        pass

    # ISO or common string formats
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(_MSK_TZ)
    except Exception:
        pass

    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%d.%m.%Y %H:%M:%S",
        "%d.%m.%Y %H:%M",
        "%d.%m.%Y",
    ):
        try:
            dt = datetime.strptime(s, fmt)
            dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(_MSK_TZ)
        except Exception:
            continue

    return None

def _parse_sheet_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    s = str(value).strip()
    if not s or s == "N/A":
        return None
    try:
        return int(float(s))
    except Exception:
        return None

def compute_stats_from_sheet(window_start_msk: datetime, title: str) -> str:
    """
    Aggregate weekly/monthly statistics directly from Google Sheets.
    """
    global _gsheets_sheet

    if not GSHEETS_AVAILABLE or _gsheets_sheet is None:
        return "Нет данных за выбранный период"

    if window_start_msk.tzinfo is None:
        window_start_msk = window_start_msk.replace(tzinfo=_MSK_TZ)
    else:
        window_start_msk = window_start_msk.astimezone(_MSK_TZ)

    with _gsheets_lock:
        rows = _gsheets_sheet.get_all_values()

    if not rows or len(rows) < 2:
        return "Нет данных за выбранный период"

    headers = [h.strip().lower() for h in rows[0]]
    try:
        date_idx = headers.index("время сигнала (utc)")
        goal_idx = headers.index("гол до конца матча")
        minute_idx = headers.index("минута первого гола")
    except ValueError:
        return "Нет данных за выбранный период"

    signals = 0
    plus = 0
    minus = 0
    count_60 = 0
    count_75 = 0
    count_75_plus = 0
    first_goal_minutes: List[int] = []

    for row in rows[1:]:
        if not row:
            continue

        date_val = row[date_idx] if date_idx < len(row) else ""
        dt_msk = _parse_sheet_datetime_msk(date_val)
        if not dt_msk or dt_msk < window_start_msk:
            continue

        goal_val = row[goal_idx] if goal_idx < len(row) else ""
        goal_str = str(goal_val).strip()
        if goal_str not in ("0", "1"):
            continue

        goal_until_end = int(goal_str)
        signals += 1

        if goal_until_end == 1:
            plus += 1
            minute_val = row[minute_idx] if minute_idx < len(row) else ""
            first_minute = _parse_sheet_int(minute_val)
            if first_minute is not None:
                first_goal_minutes.append(first_minute)
                if first_minute <= 60:
                    count_60 += 1
                elif first_minute <= 75:
                    count_75 += 1
                else:
                    count_75_plus += 1
        else:
            minus += 1

    if signals == 0:
        return "Нет данных за выбранный период"

    pass_rate = round(plus / signals * 100) if signals > 0 else 0
    avg_minute = round(sum(first_goal_minutes) / len(first_goal_minutes)) if first_goal_minutes else None

    lines = [
        title,
        "",
        f"🚨 Сигналов: {signals}",
        "",
        f"✅ Плюсов: {plus}",
        f"❌ Минусов: {minus}",
        "",
        f"📈 Проходимость: {pass_rate}%",
        "",
        "⚽ Реализация:",
        f"• До 60 минуты: {count_60}",
        f"• До 75 минуты: {count_75}",
        f"• После 75 минуты: {count_75_plus}",
    ]

    if avg_minute is not None:
        lines.append("")
        lines.append(f"⏱️ Средняя минута первого гола: {avg_minute}′")

    return "\n".join(lines)

def format_weekly_stats_message() -> str:
    """Format weekly statistics message for Telegram (last 7 days from signal send time)."""
    now_msk = datetime.now(_MSK_TZ)
    window_start = now_msk - timedelta(days=7)
    title = "📅 Итоги недели (последние 7 дней)"
    return compute_stats_from_sheet(window_start, title)

def format_monthly_stats_message() -> str:
    """Format monthly statistics message for Telegram (current calendar month)."""
    now_msk = datetime.now(_MSK_TZ)
    window_start = now_msk.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    month_name = _MONTH_NAMES_RU.get(now_msk.month, now_msk.strftime("%B"))
    title = f"🗓 Итоги месяца ({month_name} {now_msk.year})"
    return compute_stats_from_sheet(window_start, title)

def send_daily_stats():
    """Send daily statistics report to Telegram (deprecated, use check_and_send_daily_stats_if_ready)."""
    date_str = get_msk_date()
    msg = format_daily_stats_message(date_str)
    
    if msg:
        try:
            logger.info(f"[STATS] Sending daily stats: chat_id={TELEGRAM_CHAT_ID}, date={date_str}")
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            payload = {"chat_id": TELEGRAM_CHAT_ID, "text": msg}
            r = requests.post(url, data=payload, timeout=10)
            if r.ok:
                logger.info(f"[STATS] Daily report sent for {date_str}")
                # Mark report as sent
                with state_lock:
                    state.setdefault("daily_stats_sent", {})[date_str] = True
                mark_state_dirty()
            else:
                logger.warning(f"[STATS] Failed to send daily report: {r.text}")
        except Exception:
            logger.exception("[STATS] Error sending daily report")
    else:
        logger.info(f"[STATS] No data to send for {date_str}")

def unpin_chat_message(message_id: int) -> bool:
    """Unpin a specific message in the chat (deprecated - no longer used)."""
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/unpinChatMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "message_id": message_id}
        r = requests.post(url, data=payload, timeout=10)
        if r.ok:
            logger.info(f"[TG] Unpinned message {message_id}")
            return True
        else:
            logger.warning(f"[TG] Failed to unpin message {message_id}: {r.text}")
            return False
    except Exception:
        logger.exception(f"[TG] Error unpinning message {message_id}")
        return False

def pin_chat_message(message_id: int) -> bool:
    """Pin a message in the chat without notification (deprecated - no longer used)."""
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/pinChatMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "message_id": message_id,
            "disable_notification": True
        }
        r = requests.post(url, data=payload, timeout=10)
        if r.ok:
            logger.info(f"[TG] Pinned message {message_id}")
            return True
        else:
            logger.warning(f"[TG] Failed to pin message {message_id}: {r.text}")
            return False
    except Exception:
        logger.exception(f"[TG] Error pinning message {message_id}")
        return False

def get_bot_username() -> Optional[str]:
    """Get bot username via Telegram API."""
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getMe"
        r = requests.get(url, timeout=10)
        if r.ok:
            data = r.json()
            if data.get("ok"):
                username = data.get("result", {}).get("username")
                logger.info(f"[TG] Bot username: {username}")
                return username
        logger.warning(f"[TG] Failed to get bot username: {r.text}")
        return None
    except Exception:
        logger.exception("[TG] Error getting bot username")
        return None

def get_standard_keyboard() -> Dict[str, Any]:
    """Get standard navigation keyboard for all bot messages."""
    return {
        "inline_keyboard": [
            [
                {"text": "📊 Итоги недели", "callback_data": "stats_week"}
            ],
            [
                {"text": "📅 Итоги месяца", "callback_data": "stats_month"}
            ],
            [
                {"text": "👁️♦️ Как работать с сигналами", "callback_data": "instruction"}
            ]
        ]
    }

def get_channel_keyboard() -> Dict[str, Any]:
    """Get callback-based keyboard for channel messages (daily stats)."""
    return {
        "inline_keyboard": [
            [
                {"text": "📊 Итоги недели", "callback_data": "stats_week"}
            ],
            [
                {"text": "📅 Итоги месяца", "callback_data": "stats_month"}
            ],
            [
                {"text": "👁️♦️ Как работать с сигналами", "callback_data": "instruction"}
            ]
        ]
    }

def send_permanent_instruction_message():
    """Send permanent instruction message to the channel (only once)."""
    with persistent_state_lock:
        if persistent_state.get("instruction_message_id"):
            return
    
    # Hardcoded bot username to avoid initialization issues
    BOT_USERNAME = "Test_Z1z1Z_Bot"
    
    instruction_text = (
        "👁️ Инструкция\n\n"
        "Чтобы получать недельную и месячную статистику,\n"
        "перейдите в личные сообщения бота 🔻"
    )
    
    # Create URL button that opens bot with /start
    keyboard = {
        "inline_keyboard": [
            [
                {
                    "text": "🔘Открыть бота",
                    "url": f"https://t.me/{BOT_USERNAME}?start=strategy"
                }
            ]
        ]
    }
    
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": instruction_text,
            "reply_markup": json.dumps(keyboard)
        }
        r = requests.post(url, data=payload, timeout=10)
        
        if r.ok:
            try:
                response_data = r.json()
                msg_id = response_data.get("result", {}).get("message_id")
            except Exception:
                msg_id = None
            
            if msg_id:
                logger.info(f"[TG] Instruction message sent, message_id={msg_id}")
                # Pin the instruction message
                if pin_chat_message(msg_id):
                    with persistent_state_lock:
                        persistent_state["instruction_message_id"] = int(msg_id)
                    save_persistent_state()
                    logger.info(f"[TG] Instruction message pinned: {msg_id}")
        else:
            logger.warning(f"[TG] Failed to send instruction message: {r.text}")
    except Exception:
        logger.exception("[TG] Error sending instruction message")

def handle_message(message: Dict[str, Any]):
    """Handle incoming messages (commands like /start)."""
    try:
        from_user = message.get("from", {})
        user_id = from_user.get("id")
        chat_id = message.get("chat", {}).get("id")
        chat_type = message.get("chat", {}).get("type")
        text = message.get("text", "")
        
        # Only handle private messages
        if chat_type != "private":
            return
        
        logger.info(f"[TG] Received message from user {user_id}: {text}")
        
        if text == "/start" or text.startswith("/start "):
            # Parse deep-link parameter from /start command
            start_param = None
            if text.startswith("/start "):
                parts = text.split(maxsplit=1)
                if len(parts) > 1:
                    start_param = parts[1].strip()
                    logger.info(f"[TG] Deep-link parameter: {start_param}")
            
            # Check if this is the first time user is starting the bot
            with persistent_state_lock:
                started_users = persistent_state.setdefault("started_users", {})
                is_first_start = str(user_id) not in started_users

                now_iso = datetime.now(timezone.utc).isoformat()
                if is_first_start:
                    started_users[str(user_id)] = {
                        "first_seen": now_iso,
                        "last_seen": now_iso,
                        "ts": now_iso,
                    }
                else:
                    rec = started_users.get(str(user_id), {})
                    if not isinstance(rec, dict):
                        rec = {}
                    first_seen = rec.get("first_seen") or rec.get("ts") or now_iso
                    rec["first_seen"] = first_seen
                    rec["last_seen"] = now_iso
                    rec["ts"] = first_seen
                    started_users[str(user_id)] = rec
            if is_first_start:
                save_persistent_state()
                logger.info(f"[TG] User {user_id} started bot for the first time")
            
            # Send welcome message ONLY on first start
            if is_first_start:
                welcome_text = (
                    "👁️ Вы подписались на получение статистики.\n"
                    "Используйте кнопки ниже 🔻"
                )
                
                send_url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
                send_payload = {
                    "chat_id": user_id,
                    "text": welcome_text,
                    "reply_markup": json.dumps(get_standard_keyboard())
                }
                r = requests.post(send_url, data=send_payload, timeout=10)
                
                if r.ok:
                    logger.info(f"[TG] Welcome message sent to user {user_id} (first start)")
                else:
                    logger.warning(f"[TG] Failed to send welcome message to user {user_id}: {r.text}")
            else:
                logger.info(f"[TG] User {user_id} restarted bot (welcome message skipped)")
            
            # Send requested content if deep-link parameter is present (regardless of first/repeat start)
            if start_param:
                # Check rate limit for stat requests via /start (separate limits for each action)
                rate_action = None
                now_ts = None
                if start_param in ("stats_week", "stats_month"):
                    now_ts = time.time()
                    # Map start_param to action name
                    action_map = {"stats_week": "weekly", "stats_month": "monthly"}
                    rate_action = action_map.get(start_param)
                    
                    can_request, wait_sec = can_user_request(user_id, rate_action, now_ts, window=60)
                    
                    if not can_request:
                        logger.info(f"[RATE] user={user_id} blocked wait={wait_sec}s action={rate_action}")
                        # Silent ignore - don't send any message
                        return
                
                content_msg = None
                content_type = None
                
                if start_param == "stats_week":
                    content_msg = format_weekly_stats_message()
                    content_type = "weekly stats"
                elif start_param == "stats_month":
                    content_msg = format_monthly_stats_message()
                    content_type = "monthly stats"
                elif start_param == "strategy":
                    content_msg = format_red_eye_strategy_message()
                    content_type = "strategy"
                
                if content_msg:
                    try:
                        content_url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
                        content_payload = {
                            "chat_id": user_id,
                            "text": content_msg,
                            "reply_markup": json.dumps(get_standard_keyboard())
                        }
                        content_r = requests.post(content_url, data=content_payload, timeout=10)
                        
                        if content_r.ok:
                            logger.info(f"[TG] Sent {content_type} to user {user_id} via deep-link")
                            # Mark rate limit ONLY after successful send
                            if rate_action and now_ts:
                                mark_user_request(user_id, rate_action, now_ts)
                        else:
                            logger.warning(f"[TG] Failed to send {content_type}: {content_r.text}")
                    except Exception as e:
                        logger.exception(f"[TG] Error sending {content_type}: {e}")
                        # Don't mark rate limit on error - let user retry
    
    except Exception:
        logger.exception("[TG] Error handling message")

def handle_callback_query(callback_query: Dict[str, Any]):
    """Handle callback query from inline keyboard buttons."""
    try:
        callback_id = callback_query.get("id")
        callback_data = callback_query.get("data")
        from_user = callback_query.get("from", {})
        user_id = from_user.get("id")
        
        logger.info(f"[TG] Received callback: {callback_data} from user {user_id}")
        
        # Always answer callback to prevent infinite loading
        answer_url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/answerCallbackQuery"
        
        # Handle REVIEW mode callbacks (review_send:<fixture_id> or review_skip:<fixture_id>)
        if callback_data.startswith("review_"):
            try:
                action, fixture_id_str = callback_data.split(":", 1)
                fixture_id = int(fixture_id_str)
                
                with state_lock:
                    review_queue = state.get("review_queue", {})
                    review_info = review_queue.get(str(fixture_id))
                
                if not review_info:
                    logger.warning(f"[REVIEW] No review info for fixture_id={fixture_id}")
                    answer_payload = {"callback_query_id": callback_id, "text": "Сигнал не найден", "show_alert": True}
                    requests.post(answer_url, data=answer_payload, timeout=5)
                    return
                
                # Check if already decided
                if review_info.get("review_decision"):
                    logger.info(f"[REVIEW] Already decided for fixture_id={fixture_id}: {review_info['review_decision']}")
                    answer_payload = {"callback_query_id": callback_id, "text": "Уже обработано", "show_alert": True}
                    requests.post(answer_url, data=answer_payload, timeout=5)
                    return
                
                if action == "review_send":
                    # SEND TO CHANNEL
                    logger.info(f"[REVIEW] Admin approved fixture_id={fixture_id}")
                    
                    # Mark as approved by admin BEFORE publishing
                    with state_lock:
                        review_queue[str(fixture_id)]["approved_by_admin"] = True
                        mark_state_dirty()
                    
                    # Publish signal to channel
                    success = publish_signal_to_channel(fixture_id)
                    
                    if success:
                        # Update review decision and mark as resolved
                        with state_lock:
                            review_queue[str(fixture_id)]["review_decision"] = "sent"
                            review_queue[str(fixture_id)]["review_decision_ts"] = time.time()
                            review_queue[str(fixture_id)]["review_by"] = "admin"
                            review_queue[str(fixture_id)]["resolved"] = True
                            
                            # Also mark in admin_reviews to stop daemon updates
                            admin_reviews = state.get("admin_reviews", {})
                            if str(fixture_id) in admin_reviews:
                                admin_reviews[str(fixture_id)]["finished"] = True
                            
                            mark_state_dirty()
                        
                        # Edit message to remove keyboard
                        message = callback_query.get("message", {})
                        message_id = message.get("message_id")
                        chat_id = message.get("chat", {}).get("id")
                        
                        if message_id and chat_id:
                            edit_url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/editMessageReplyMarkup"
                            edit_payload = {
                                "chat_id": chat_id,
                                "message_id": message_id,
                                "reply_markup": json.dumps({"inline_keyboard": []})
                            }
                            requests.post(edit_url, data=edit_payload, timeout=5)
                            logger.info(f"[REVIEW] Removed keyboard for fixture={fixture_id} after admin decision")
                        
                        answer_payload = {"callback_query_id": callback_id, "text": "✅ Сигнал отправлен в канал", "show_alert": False}
                        requests.post(answer_url, data=answer_payload, timeout=5)
                        logger.info(f"[REVIEW] Signal published for fixture_id={fixture_id}, marked as resolved")
                    else:
                        answer_payload = {"callback_query_id": callback_id, "text": "❌ Ошибка отправки", "show_alert": True}
                        requests.post(answer_url, data=answer_payload, timeout=5)
                        logger.error(f"[REVIEW] Failed to publish signal for fixture_id={fixture_id}")
                    
                    return
                
                elif action == "review_skip":
                    # SKIP (IGNORE)
                    logger.info(f"[REVIEW] Admin skipped fixture_id={fixture_id}")
                    
                    # Update review decision and mark as resolved
                    with state_lock:
                        review_queue[str(fixture_id)]["review_decision"] = "skipped"
                        review_queue[str(fixture_id)]["review_decision_ts"] = time.time()
                        review_queue[str(fixture_id)]["resolved"] = True
                        
                        # Also mark in admin_reviews to stop daemon updates
                        admin_reviews = state.get("admin_reviews", {})
                        if str(fixture_id) in admin_reviews:
                            admin_reviews[str(fixture_id)]["finished"] = True
                        
                        mark_state_dirty()
                    
                    # Edit message to remove keyboard
                    message = callback_query.get("message", {})
                    message_id = message.get("message_id")
                    chat_id = message.get("chat", {}).get("id")
                    
                    if message_id and chat_id:
                        edit_url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/editMessageReplyMarkup"
                        edit_payload = {
                            "chat_id": chat_id,
                            "message_id": message_id,
                            "reply_markup": json.dumps({"inline_keyboard": []})
                        }
                        requests.post(edit_url, data=edit_payload, timeout=5)
                        logger.info(f"[REVIEW] Removed keyboard for fixture={fixture_id} after admin decision")
                    
                    answer_payload = {"callback_query_id": callback_id, "text": "❌ Сигнал пропущен", "show_alert": False}
                    requests.post(answer_url, data=answer_payload, timeout=5)
                    logger.info(f"[REVIEW] Signal skipped for fixture_id={fixture_id}, marked as resolved")
                    return
            
            except Exception:
                logger.exception(f"[REVIEW] Error handling review callback: {callback_data}")
                answer_payload = {"callback_query_id": callback_id, "text": "Ошибка обработки", "show_alert": True}
                requests.post(answer_url, data=answer_payload, timeout=5)
                return
        
        # Check if user has started the bot before processing callback
        with persistent_state_lock:
            user_started = persistent_state.get("started_users", {})
            has_started = str(user_id) in user_started
        
        # If user has not started the bot, show instruction and stop processing
        if not has_started:
            logger.info(f"[TG] User {user_id} has not started bot yet, showing alert")
            answer_payload = {
                "callback_query_id": callback_id,
                "text": "Чтобы открыть статистику, перейдите в закреплённое сообщение и нажмите «🔘Открыть бота»",
                "show_alert": True
            }
            requests.post(answer_url, data=answer_payload, timeout=5)
            return
        
        # Check rate limit for personal request actions (separate limit for each action)
        rate_action = None
        now_ts = None
        if callback_data in ("stats_week", "stats_month", "instruction"):
            now_ts = time.time()
            # Map callback_data to action name
            action_map = {"stats_week": "weekly", "stats_month": "monthly", "instruction": "instruction"}
            rate_action = action_map.get(callback_data)
            
            can_request, wait_sec = can_user_request(user_id, rate_action, now_ts, window=60)
            
            if not can_request:
                logger.info(f"[RATE] user={user_id} blocked wait={wait_sec}s action={rate_action}")
                
                # Silent ignore: just close the loading in Telegram without sending message
                answer_payload = {"callback_query_id": callback_id, "text": "", "show_alert": False}
                requests.post(answer_url, data=answer_payload, timeout=5)
                return
        
        # Handle different callback actions with unified navigation
        # Mark rate limit ONLY after successful send
        try:
            if callback_data == "stats_week":
                msg = format_weekly_stats_message()
                if msg and user_id:
                    send_url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
                    send_payload = {
                        "chat_id": user_id,
                        "text": msg,
                        "reply_markup": json.dumps(get_standard_keyboard())
                    }
                    r = requests.post(send_url, data=send_payload, timeout=10)
                    
                    if r.ok:
                        logger.info(f"[TG] Weekly stats sent to user {user_id}")
                        # Mark rate limit ONLY after successful send
                        if rate_action and now_ts:
                            mark_user_request(user_id, rate_action, now_ts)
                    else:
                        logger.warning(f"[TG] Failed to send weekly stats to user {user_id}: {r.text}")
            
            elif callback_data == "stats_month":
                msg = format_monthly_stats_message()
                if msg and user_id:
                    send_url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
                    send_payload = {
                        "chat_id": user_id,
                        "text": msg,
                        "reply_markup": json.dumps(get_standard_keyboard())
                    }
                    r = requests.post(send_url, data=send_payload, timeout=10)
                    
                    if r.ok:
                        logger.info(f"[TG] Monthly stats sent to user {user_id}")
                        # Mark rate limit ONLY after successful send
                        if rate_action and now_ts:
                            mark_user_request(user_id, rate_action, now_ts)
                    else:
                        logger.warning(f"[TG] Failed to send monthly stats to user {user_id}: {r.text}")
            
            elif callback_data == "instruction":
                msg = format_red_eye_strategy_message()
                if msg and user_id:
                    send_url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
                    send_payload = {
                        "chat_id": user_id,
                        "text": msg,
                        "reply_markup": json.dumps(get_standard_keyboard())
                    }
                    r = requests.post(send_url, data=send_payload, timeout=10)
                    
                    if r.ok:
                        logger.info(f"[TG] Instruction sent to user {user_id}")
                        # Mark rate limit ONLY after successful send
                        if rate_action and now_ts:
                            mark_user_request(user_id, rate_action, now_ts)
                    else:
                        logger.warning(f"[TG] Failed to send instruction to user {user_id}: {r.text}")
        except Exception as e:
            # If error occurs, don't mark rate limit - let user retry
            logger.exception(f"[TG] Error handling callback action {callback_data}: {e}")
        
        # Always answer callback query to remove loading state
        answer_payload = {"callback_query_id": callback_id}
        requests.post(answer_url, data=answer_payload, timeout=5)
        
    except Exception:
        logger.exception("[TG] Error handling callback query")
        # Even on error, try to answer callback
        try:
            if callback_id:
                answer_url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/answerCallbackQuery"
                answer_payload = {"callback_query_id": callback_id}
                requests.post(answer_url, data=answer_payload, timeout=5)
        except:
            pass

# Review timeout checker daemon
_review_timeout_thread: Optional[threading.Thread] = None
_review_timeout_stop = threading.Event()

def review_timeout_daemon():
    """Background thread that checks for expired review requests and auto-skips them."""
    logger.info("[REVIEW] Timeout checker daemon started")
    
    while not _review_timeout_stop.wait(60):  # Check every minute
        try:
            now_ts = time.time()
            timeout_seconds = REVIEW_TIMEOUT_MINUTES * 60
            
            with state_lock:
                review_queue = state.get("review_queue", {})
                expired_ids = []
                
                for fixture_id_str, review_info in review_queue.items():
                    # Skip if already decided
                    if review_info.get("review_decision"):
                        continue
                    
                    # Check if sent and timeout expired
                    if review_info.get("review_sent"):
                        review_ts = review_info.get("review_ts", 0)
                        elapsed = now_ts - review_ts
                        
                        if elapsed > timeout_seconds:
                            expired_ids.append(fixture_id_str)
                
                # Mark expired as timeout_skipped
                for fixture_id_str in expired_ids:
                    review_queue[fixture_id_str]["review_decision"] = "timeout_skipped"
                    review_queue[fixture_id_str]["review_decision_ts"] = now_ts
                    logger.info(f"[REVIEW] Auto-skipped due to timeout: fixture_id={fixture_id_str}")
                
                if expired_ids:
                    mark_state_dirty()
        
        except Exception:
            logger.exception("[REVIEW] Error in timeout checker daemon")
    
    logger.info("[REVIEW] Timeout checker daemon stopped")

def start_review_timeout_daemon():
    """Start the review timeout checker daemon."""
    global _review_timeout_thread
    if _review_timeout_thread and _review_timeout_thread.is_alive():
        logger.info("[REVIEW] Timeout daemon already running")
        return
    _review_timeout_stop.clear()
    t = threading.Thread(target=review_timeout_daemon, daemon=True)
    _review_timeout_thread = t
    t.start()
    logger.info("[REVIEW] Timeout daemon thread started")

# Callback handler daemon
_callback_handler_thread: Optional[threading.Thread] = None
_callback_handler_stop = threading.Event()
_last_update_id = 0

# Global session for Telegram API with retry mechanism
_telegram_session = None

def _init_telegram_session():
    """Initialize requests.Session with retry mechanism for Telegram API."""
    global _telegram_session
    if _telegram_session is None:
        session = requests.Session()
        # Configure retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1.0,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        _telegram_session = session
        logger.info("[TG] Telegram API session initialized with retry mechanism")
    return _telegram_session

def callback_handler_daemon():
    """Background thread that processes Telegram updates for callback queries and messages."""
    global _last_update_id
    logger.info("[TG] Callback handler daemon started")
    
    # Initialize session with retry mechanism
    session = _init_telegram_session()
    
    while not _callback_handler_stop.wait(2):  # Check every 2 seconds
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates"
            params = {
                "offset": _last_update_id + 1,
                "timeout": 10,
                "allowed_updates": ["callback_query", "message"]
            }
            # Use session with timeouts: (connect_timeout, read_timeout)
            r = session.get(url, params=params, timeout=(5, 30))
            
            if r.ok:
                data = r.json()
                if data.get("ok") and data.get("result"):
                    for update in data["result"]:
                        update_id = update.get("update_id")
                        if update_id:
                            _last_update_id = max(_last_update_id, update_id)
                        
                        if "callback_query" in update:
                            handle_callback_query(update["callback_query"])
                        elif "message" in update:
                            handle_message(update["message"])
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as e:
            # Handle network errors gracefully without traceback spam
            logger.warning(f"[TG] Network error in callback handler: {type(e).__name__}: {str(e)[:100]}")
            time.sleep(3)  # Wait before retry
        except Exception as e:
            # Log other exceptions with full traceback
            logger.exception(f"[TG] Unexpected error in callback handler daemon: {e}")
            time.sleep(2)
    
    logger.info("[TG] Callback handler daemon stopped")

def start_callback_handler_daemon():
    """Start the callback handler daemon."""
    global _callback_handler_thread
    if _callback_handler_thread and _callback_handler_thread.is_alive():
        return
    _callback_handler_stop.clear()
    t = threading.Thread(target=callback_handler_daemon, daemon=True)
    _callback_handler_thread = t
    t.start()
    logger.info("[TG] Callback handler daemon thread started")

def daily_stats_checker_daemon():
    """Background thread that checks yesterday(MSK) daily report readiness."""
    logger.info("[DAILY_STATS] checker daemon started")

    while True:
        try:
            time.sleep(60)
            maybe_send_daily_report()
        except Exception:
            logger.exception("[DAILY_STATS] checker daemon iteration failed")

def start_daily_stats_checker_daemon():
    """Start the daily stats checker daemon."""
    t = threading.Thread(target=daily_stats_checker_daemon, daemon=True)
    t.start()
    logger.info("[STATS] Daily stats checker daemon thread started")


last_daily_skip_log_date = None


def maybe_send_daily_report(max_rows: int = 2500) -> None:
    now_dt_msk = get_msk_datetime()
    report_date = get_candidate_report_date_msk(now_dt_msk)
    report_date_str = report_date.strftime("%Y-%m-%d")
    start_msk, end_msk = get_report_window_msk(report_date)

    logger.info("[DAILY_STATS] now_msk=%s", now_dt_msk.isoformat())
    logger.info("[DAILY_STATS] candidate_report_date=%s", report_date_str)
    logger.info("[DAILY_STATS] report_window_msk start=%s end=%s", start_msk.isoformat(), end_msk.isoformat())

    with persistent_state_lock:
        last_sent = persistent_state.get("daily_report_last_sent_date") or persistent_state.get("last_daily_stats_sent_date_msk")
    if str(last_sent or "") == report_date_str:
        logger.info("[DAILY_STATS] already_sent report_date=%s", report_date_str)
        return

    api_client = APISportsMetricsClient(api_key=API_FOOTBALL_KEY, host=API_FOOTBALL_HOST, cache_ttl=CACHE_TTL)

    completion = _collect_report_matches_finish_state(report_date_msk=report_date, client=api_client)
    report_matches_total = int(completion.get("report_matches_total", 0) or 0)
    report_matches_finished = int(completion.get("report_matches_finished", 0) or 0)
    unfinished_matches = int(completion.get("unfinished_matches", 0) or 0)
    unfinished_ids = list(completion.get("unfinished_ids") or [])

    logger.info("[DAILY_STATS] report_matches_total=%s", report_matches_total)
    logger.info("[DAILY_STATS] report_matches_finished=%s", report_matches_finished)
    logger.info("[DAILY_STATS] unfinished_matches=%s", unfinished_matches)
    if unfinished_ids:
        logger.info("[DAILY_STATS] unfinished_ids=%s", unfinished_ids)

    if not bool(completion.get("all_finished", False)):
        logger.info("[DAILY_STATS] waiting_for_all_matches_to_finish report_date=%s", report_date_str)
        return

    collected = collect_report_matches(report_date_msk=report_date, client=api_client, max_rows=max_rows)
    rows_loaded = int(collected.get("rows_loaded", 0) or 0)
    matches = list(collected.get("matches") or [])

    logger.info("[DAILY_STATS] rows_loaded=%s", rows_loaded)
    logger.info("[DAILY_STATS] unique_matches_in_window=%s", len(matches))

    all_finished, unfinished_ids = are_all_matches_finished(matches)
    logger.info("[DAILY_STATS] unfinished_matches=%s", len(unfinished_ids))
    if unfinished_ids:
        logger.info("[DAILY_STATS] unfinished_ids=%s", unfinished_ids)
        logger.info("[DAILY_STATS] waiting_for_all_matches_to_finish report_date=%s", report_date_str)
        return

    if not matches:
        logger.info("[DAILY_STATS] waiting_for_all_matches_to_finish report_date=%s", report_date_str)
        return

    if not all_finished:
        logger.info("[DAILY_STATS] waiting_for_all_matches_to_finish report_date=%s", report_date_str)
        return

    payload_stats = build_daily_report(report_date_msk=report_date, matches=matches)
    if not payload_stats:
        logger.info("[DAILY_STATS] waiting_for_all_matches_to_finish report_date=%s", report_date_str)
        return

    msg = str(payload_stats.get("message") or "").strip()
    if not msg:
        logger.info("[DAILY_STATS] waiting_for_all_matches_to_finish report_date=%s", report_date_str)
        return

    logger.info("[DAILY_STATS] sending_report report_date=%s", report_date_str)

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": msg,
        "reply_markup": json.dumps(get_channel_keyboard())
    }
    r = requests.post(url, data=payload, timeout=10)
    if r.ok:
        logger.info("[DAILY_STATS] report sent report_date=%s", report_date_str)
    else:
        logger.warning("[DAILY_STATS] send failed report_date=%s err=%s", report_date_str, r.text)

    pinned_id = None
    with persistent_state_lock:
        pinned_id = persistent_state.get("pinned_daily_msg_id") or persistent_state.get("pinned_daily_stats_message_id")
        if pinned_id is None and STATS_MESSAGE_ID:
            persistent_state["pinned_daily_msg_id"] = int(STATS_MESSAGE_ID)
            persistent_state["pinned_daily_stats_message_id"] = int(STATS_MESSAGE_ID)
            pinned_id = int(STATS_MESSAGE_ID)
            save_persistent_state()

    if pinned_id:
        ok, status, retry_after = tg_edit_queue.edit_message_text(
            chat_id=int(TELEGRAM_CHAT_ID),
            message_id=int(pinned_id),
            text=msg,
            reply_markup=get_channel_keyboard(),
            log_prefix="[DAILY_STATS] edit",
        )
        if ok:
            logger.info("[DAILY_STATS] pinned message updated id=%s", pinned_id)
        elif status in ("rate_limit", "chat_frozen"):
            logger.info("[DAILY_STATS] pinned edit delayed retry_after=%ss", retry_after)
        elif status not in ("unchanged", "not_modified"):
            logger.warning("[DAILY_STATS] pinned edit failed status=%s", status)

    if r.ok:
        with persistent_state_lock:
            persistent_state["daily_report_last_sent_date"] = report_date_str
            persistent_state["last_daily_stats_sent_date_msk"] = report_date_str
            stats_state = persistent_state.setdefault("stats", {})
            if not isinstance(stats_state, dict):
                stats_state = {}
                persistent_state["stats"] = stats_state
            stats_state["last_daily_stats_date_msk"] = report_date_str
        save_persistent_state()

        with state_lock:
            state.setdefault("daily_stats_sent", {})[report_date_str] = True
            mark_state_dirty()

def check_and_send_daily_stats_if_ready(signal_date: str):
    """
    Compatibility wrapper.
    Daily reporting now always targets yesterday in MSK, regardless of incoming hint date.
    """
    if signal_date:
        logger.debug("[DAILY_STATS] external date hint ignored=%s", signal_date)
    maybe_send_daily_report(max_rows=2500)

# -------------------------
# Metric helpers & decision functions
# (unchanged)
# -------------------------
_MISSING_LOG_DEBOUNCE = 60
_last_missing_log_time: Dict[int, float] = {}

def _should_log_missing(match_id: Optional[int]) -> bool:
    try:
        if match_id is None:
            return True
        now = time.time()
        last = _last_missing_log_time.get(int(match_id))
        if last is None or (now - last) > _MISSING_LOG_DEBOUNCE:
            _last_missing_log_time[int(match_id)] = now
            return True
        return False
    except Exception:
        return True

def get_any_metric(fixture: Dict[str, Any], base_keys: List[str], side: str) -> Optional[float]:
    if not fixture:
        return None
    candidates = []
    for bk in base_keys:
        candidates.append(f"{bk}_{side}")
        parts = bk.split("_")
        cap = "_".join([p.capitalize() for p in parts])
        candidates.append(f"{cap}_{side}")
        if bk == "shots_on_target":
            candidates += [f"Shots_on_Goal_{side}", f"shots_on_goal_{side}", f"shotsOnTarget_{side}"]
        if bk == "total_shots":
            candidates += [f"Total_shots_{side}", f"totalShots_{side}"]
        if bk == "corner_kicks":
            candidates += [f"corners_{side}", f"Corner_kicks_{side}"]
        if bk in ("big_chances","dangerous_attacks"):
            candidates += [f"big_chances_{side}", f"Dangerous_attacks_{side}", f"Big_chances_{side}"]
        if bk == "expected_goals":
            candidates += [f"expected_goals_{side}", f"expectedGoals_{side}"]
        if bk == "attacks":
            candidates += [f"Attacks_{side}", f"attacks_{side}", f"attack_{side}"]
        if bk == "saves":
            candidates += [f"Saves_{side}", f"saves_{side}"]
        if bk == "shots_inside_box":
            candidates += [f"shots_inside_box_{side}", f"Shots_inside_box_{side}", f"shotsInsideBox_{side}", f"inside_box_shots_{side}", f"shots_inside_the_box_{side}"]
    for bk in base_keys:
        candidates.append(bk)
        candidates.append(bk.capitalize())

    seen = set()
    for c in candidates:
        if c in seen:
            continue
        seen.add(c)
        v = fixture.get(c)
        if isinstance(v, dict) and "value" in v:
            v = v["value"]
        if v is None:
            continue
        try:
            if isinstance(v, str):
                s = v.strip().replace("%", "").replace(",", ".")
                if s == "":
                    continue
                num = float(s)
            else:
                num = float(v)
            return num
        except Exception:
            continue

    try:
        lower = {k.lower(): v for k, v in fixture.items() if isinstance(k, str)}
        for bk in base_keys:
            bk_flat = bk.replace("_","")
            for k, v in lower.items():
                if bk_flat in k.replace("_",""):
                    if isinstance(v, dict) and "value" in v:
                        try:
                            return float(v["value"])
                        except Exception:
                            continue
                    try:
                        return float(v)
                    except Exception:
                        continue
    except Exception:
        pass

    return None

def _has_enough_stats_for_xg(fixture_metrics: Dict[str, Any]) -> bool:
    keys_to_check = ["total_shots", "shots_on_target", "big_chances", "dangerous_attacks", "corner_kicks", "attacks"]
    for side in ("home","away"):
        for k in keys_to_check:
            val = get_any_metric(fixture_metrics, [k], side)
            try:
                if val is not None and float(val) > 0:
                    return True
            except Exception:
                continue
    return False

def _to_float(value: Any, default: float = 0.0) -> float:
    """
    Безопасно конвертирует любое значение в float.
    Обрабатывает None, пустые строки, "N/A" и другие нестандартные типы.
    """
    if value is None or value == "" or value == "N/A":
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default

def _to_int(value: Any, default: int = 0) -> int:
    """
    Безопасно конвертирует любое значение в int.
    Обрабатывает None, пустые строки, "N/A" и другие нестандартные типы.
    """
    if value is None or value == "" or value == "N/A":
        return default
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return default

def compute_xg_fallback(
    shots_total: float,
    shots_on_target: float,
    shots_in_box: float,
    corners: float,
    pressure_index: float,
    possession_pressure: float,
    save_stress: float
) -> float:
    """
    Вычисляет xG fallback (когда API не предоставляет xG).
    
    Формула основана на метриках матча:
    - Удары в штрафной (Bi) и вне штрафной (outside_box)
    - Удары в створ (SoT)
    - Угловые (C)
    - Давление матча (PI), давление на владение (PS), нагрузка на вратаря (SS)
    
    Безопасно обрабатывает None, строки и другие типы данных.
    Гарантирует xG >= 0.0 и в разумных пределах (обычно 0-3).
    
    Args:
        shots_total: Общее число ударов команды
        shots_on_target: Удары в створ
        shots_in_box: Удары из штрафной
        corners: Угловые
        pressure_index: Индекс давления матча
        possession_pressure: Давление владения мячом
        save_stress: Нагрузка на вратаря
        
    Returns:
        xG команды (float >= 0.0)
    """
    # Конвертируем все входы в float/int
    S = _to_int(shots_total, 0)      # Общее число ударов
    SoT = _to_int(shots_on_target, 0)  # Удары в створ
    Bi = _to_int(shots_in_box, 0)    # Удары из штрафной
    C = _to_int(corners, 0)           # Угловые
    PI = _to_float(pressure_index, 0.0)  # Pressure index
    PS = _to_float(possession_pressure, 0.0)  # Possession pressure
    SS = _to_float(save_stress, 0.0)   # Save stress
    
    # Защита от противоречивых данных
    # Если shots_in_box > total_shots, ограничиваем shots_in_box
    if Bi > S and S > 0:
        Bi = S
    
    # Вычисляем удары вне штрафной
    outside_box = max(0, S - Bi)
    
    # === BASE: Базовый xG из прямых метрик ===
    # 0.11 за удар из штрафной
    # 0.03 за удар вне штрафной
    # 0.04 за удар в створ
    xg_base = (
        0.11 * Bi +
        0.03 * outside_box +
        0.04 * SoT
    )
    
    # === BONUSES (clamp): Бонусы с ограничениями ===
    # Бонус за угловые: макс 0.25
    bonus_corners = min(0.25, 0.02 * C)
    
    # Бонусы давления: каждый отдельно ограничен
    bonus_pressure_index = min(0.35, 0.015 * PI)
    bonus_possession_pressure = min(0.25, 0.20 * PS)
    bonus_save_stress = min(0.25, 0.15 * SS)
    bonus_pressure = bonus_pressure_index + bonus_possession_pressure + bonus_save_stress
    
    # === RAW: Сырой xG ===
    xg_raw = xg_base + bonus_corners + bonus_pressure
    
    # === CAPS: Ограничение сверху по шапкам ===
    # Шапка 1: на основе ударов в створ
    # max_xg = 0.35 * SoT + 0.15
    cap_by_sot = 0.35 * SoT + 0.15
    
    # Шапка 2: на основе ударов из штрафной
    # max_xg = 0.22 * Bi + 0.25
    cap_by_box = 0.22 * Bi + 0.25
    
    # Берём более строгую шапку
    xg_cap = min(cap_by_sot, cap_by_box)
    
    # === FINAL: Финальный xG ===
    xg_final = max(0.0, min(xg_raw, xg_cap))
    
    # Гарантируем, что нет NaN
    if math.isnan(xg_final):
        xg_final = 0.0
    
    return xg_final

def has_value(v: Any) -> bool:
    """
    Проверить наличие значения.
    Возвращает True если v is not None и v != "N/A" и v != "".
    Ноль (0) считается валидным значением.
    
    Args:
        v: Значение для проверки
        
    Returns:
        True если значение присутствует и валидно, False иначе
    """
    return v is not None and v != "N/A" and v != ""


def num(v: Any) -> float:
    """
    Безопасно преобразовать значение в float.
    Если v is None или "N/A" или "", возвращает 0.0.
    Ноль (0) преобразуется в 0.0 корректно.
    
    Args:
        v: Значение для преобразования
        
    Returns:
        float значение или 0.0 если значение отсутствует
    """
    if v is None or v == "N/A" or v == "":
        return 0.0
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _clamp(value: float, lo: float, hi: float) -> float:
    """Ограничить значение в диапазоне [lo, hi]."""
    return max(lo, min(hi, value))


def compute_coverage_score(fixture_metrics: Dict[str, Any]) -> Tuple[float, List[str]]:
    """
    Вычислить coverage_score для матча с неполной статистикой.
    
    Оценивает какой процент от требуемых метрик присутствует в данных.
    Используется только для FALLBACK режима.
    
    Требуемые метрики (4 группы):
    1. shots_total (обе стороны должны иметь значение)
    2. shots_on_target (обе стороны)
    3. shots_in_box (обе стороны)
    4. saves (обе стороны)
    
    Веса (сумма 1.0):
    - shots_total: 0.30
    - shots_on_target: 0.30
    - shots_in_box: 0.25
    - saves: 0.15
    
    Args:
        fixture_metrics: Словарь метрик матча
        
    Returns:
        Tuple[coverage_score, missing_list]:
        - coverage_score: float [0.0, 1.0] - доля присутствующих метрик
        - missing_list: List[str] - названия отсутствующих блоков
    """
    weights = {
        "shots_total": 0.30,
        "shots_on_target": 0.30,
        "shots_in_box": 0.25,
        "saves": 0.15
    }
    
    missing_list = []
    score = 0.0
    
    # ДИАГНОСТИКА: логируем проверку каждой группы
    debug_info = {}
    
    # Проверяем наличие каждой группы метрик
    for key, weight in weights.items():
        present = True
        
        if key == "shots_total":
            val_home = get_any_metric(fixture_metrics, ["total_shots"], "home")
            val_away = get_any_metric(fixture_metrics, ["total_shots"], "away")
        elif key == "shots_on_target":
            val_home = get_any_metric(fixture_metrics, ["shots_on_target"], "home")
            val_away = get_any_metric(fixture_metrics, ["shots_on_target"], "away")
        elif key == "shots_in_box":
            val_home = get_any_metric(fixture_metrics, ["shots_insidebox", "inside_box"], "home")
            val_away = get_any_metric(fixture_metrics, ["shots_insidebox", "inside_box"], "away")
        elif key == "saves":
            val_home = get_any_metric(fixture_metrics, ["saves"], "home")
            val_away = get_any_metric(fixture_metrics, ["saves"], "away")
        
        # Диагностика: сохраняем значения для логирования
        debug_info[key] = {
            "home": val_home,
            "away": val_away,
            "has_home": has_value(val_home),
            "has_away": has_value(val_away)
        }
        
        # Проверяем что обе стороны имеют значение (не None, не "N/A", не "")
        # Ноль (0) считается валидным значением!
        if not has_value(val_home) or not has_value(val_away):
            present = False
        
        if present:
            score += weight
        else:
            missing_list.append(key)
    
    # Логируем детали if что-то missing
    if missing_list:
        match_id = fixture_metrics.get("match_id") or fixture_metrics.get("id")
        logger.debug(f"[COVERAGE_DEBUG] match={match_id} score={score:.2f} missing={missing_list} details={debug_info}")
    
    
    return score, missing_list


def estimate_team_xg(shots_total: Any, shots_on_target: Any, shots_in_box: Any, 
                     corners: Any, saves_opponent: Any, possession: Any, 
                     pressure_index: Any) -> float:
    """
    Оценить xG для одной команды на основе её метрик.
    
    Использует формулу подобранную для стабильности.
    Все входные параметры безопасно преобразуются в float (отсутствующие → 0.0).
    
    Формула:
    est_xg = 0.05 * shots_total +
             0.18 * shots_on_target +
             0.10 * shots_in_box +
             0.03 * corners +
             0.04 * max(0, saves_opponent - 1) +
             0.006 * pressure_index +
             0.004 * max(0, possession - 50)
    
    Args:
        shots_total: Всего ударов команды
        shots_on_target: Ударов в створ
        shots_in_box: Ударов из штрафной
        corners: Угловых ударов
        saves_opponent: Сэйвов противника
        possession: Владение мячом (%)
        pressure_index: Индекс давления матча (глобальная метрика)
        
    Returns:
        Оцененное xG (float, ограничено до 4.5)
    """
    # Безопасное преобразование всех метрик в числа
    st = num(shots_total)
    sot = num(shots_on_target)
    sib = num(shots_in_box)
    c = num(corners)
    so = num(saves_opponent)
    poss = num(possession)
    pi = num(pressure_index)
    
    # Вычисляем xG по формуле
    est_xg = (
        0.05 * st +
        0.18 * sot +
        0.10 * sib +
        0.03 * c +
        0.04 * max(0.0, so - 1.0) +
        0.006 * pi +
        0.004 * max(0.0, poss - 50.0)
    )
    
    # Ограничиваем максимум
    est_xg = _clamp(est_xg, 0.0, FALLBACK_XG_CLAMP_MAX)
    
    return est_xg


def estimate_xg(stats: Dict[str, Any]) -> Tuple[float, float, float]:
    """
    Оценить xG для обеих команд когда реальные данные отсутствуют.
    
    Ожидает простой dict со следующими ключами:
    - shots_total_home/away
    - shots_on_target_home/away
    - shots_in_box_home/away
    - corners_home/away
    - saves_home/away
    - possession_home/away
    - pressure_index (одно значение на матч)
    
    Args:
        stats: Словарь метрик матча в нормализованном формате
        
    Returns:
        Tuple[est_xg_home, est_xg_away, est_total_xg]
    """
    # Вычисляем xG для домашней команды
    est_home = estimate_team_xg(
        shots_total=stats.get("shots_total_home"),
        shots_on_target=stats.get("shots_on_target_home"),
        shots_in_box=stats.get("shots_in_box_home"),
        corners=stats.get("corners_home"),
        saves_opponent=stats.get("saves_away"),
        possession=stats.get("possession_home"),
        pressure_index=stats.get("pressure_index"),
    )
    
    # Вычисляем xG для гостевой команды
    est_away = estimate_team_xg(
        shots_total=stats.get("shots_total_away"),
        shots_on_target=stats.get("shots_on_target_away"),
        shots_in_box=stats.get("shots_in_box_away"),
        corners=stats.get("corners_away"),
        saves_opponent=stats.get("saves_home"),
        possession=stats.get("possession_away"),
        pressure_index=stats.get("pressure_index"),
    )
    
    # Вычисляем суммарный xG с ограничением
    est_total = _clamp(est_home + est_away, 0.0, FALLBACK_TOTAL_XG_CLAMP_MAX)
    
    return est_home, est_away, est_total


def estimate_xg_for_fallback(fixture_metrics: Dict[str, Any], side: str) -> float:
    """
    Оценить xG для команды в режиме FALLBACK (когда xG отсутствует или матч неполный).
    
    Формула разработана для стабильности: xG обычно не превышает 4-5 за тайм.
    
    Args:
        fixture_metrics: Словарь метрик матча
        side: "home" или "away"
        
    Returns:
        Оцененное значение xG (float >= 0.0, clamped к 4.5)
    """
    # Извлекаем метрики для команды
    shots_total = _to_float(get_any_metric(fixture_metrics, ["total_shots"], side), 0.0)
    shots_on_target = _to_float(get_any_metric(fixture_metrics, ["shots_on_target"], side), 0.0)
    shots_in_box = _to_float(get_any_metric(fixture_metrics, ["shots_insidebox", "inside_box"], side), 0.0)
    corners = _to_float(get_any_metric(fixture_metrics, ["corner_kicks", "corners"], side), 0.0)
    
    # Метрики противника
    saves_opponent = _to_float(get_any_metric(fixture_metrics, ["saves"], "away" if side == "home" else "home"), 0.0)
    
    # Глобальные метрики
    possession = _to_float(get_any_metric(fixture_metrics, ["ball_possession", "passes_%"], side), 50.0)
    pressure_index = _to_float(get_any_metric(fixture_metrics, ["pressure_index"], side) or 
                               get_any_metric(fixture_metrics, ["pressure_index"], None), 0.0)
    
    # Логирование недостающих метрик при использовании 0
    missing = []
    if get_any_metric(fixture_metrics, ["total_shots"], side) is None:
        missing.append("shots_total")
    if get_any_metric(fixture_metrics, ["shots_on_target"], side) is None:
        missing.append("shots_on_target")
    if get_any_metric(fixture_metrics, ["shots_insidebox", "inside_box"], side) is None:
        missing.append("shots_in_box")
    if get_any_metric(fixture_metrics, ["corner_kicks", "corners"], side) is None:
        missing.append("corners")
    
    if missing:
        logger.debug(f"[FALLBACK] {side} uses 0 for missing metrics: {missing}")
    
    # Формула: подобрана для стабильности
    est_xg = (
        0.05 * shots_total +
        0.18 * shots_on_target +
        0.10 * shots_in_box +
        0.03 * corners +
        0.04 * max(0.0, saves_opponent - 1.0) +
        0.006 * pressure_index +
        0.004 * max(0.0, possession - 50.0)
    )
    
    # Ограничиваем максимум
    est_xg = _clamp(est_xg, 0.0, FALLBACK_XG_CLAMP_MAX)
    
    return est_xg


def estimate_xg(fixture_metrics: Dict[str, Any]) -> Tuple[float, float, float]:
    """
    Оценить suммарный xG для матча, если real xG отсутствует.
    
    Args:
        fixture_metrics: Словарь метрик матча
        
    Returns:
        Tuple[est_xg_home, est_xg_away, est_total_xg]
    """
    est_home = estimate_xg_for_fallback(fixture_metrics, "home")
    est_away = estimate_xg_for_fallback(fixture_metrics, "away")
    est_total = _clamp(est_home + est_away, 0.0, FALLBACK_TOTAL_XG_CLAMP_MAX)
    
    return est_home, est_away, est_total


def is_full_match(fixture_metrics: Dict[str, Any]) -> bool:
    """
    Проверить является ли матч "полным" по данным.
    
    Полный матч = наличие всех 4 групп метрик:
    - shots_total_home/away
    - shots_on_target_home/away
    - shots_in_box_home/away
    - saves_home/away
    
    xG не требуется (часто отсутствует).
    
    Args:
        fixture_metrics: Словарь метрик матча
        
    Returns:
        True если есть все 4 группы, False иначе
    """
    coverage, _ = compute_coverage_score(fixture_metrics)
    # Если все 4 группы присутствуют, coverage = 1.0
    return coverage >= 1.0


def should_send_signal(prob: float, base_threshold: float, fixture_metrics: Dict[str, Any]) -> bool:
    """
    Определить нужно ли отправлять сигнал на основе вероятности и наличия данных.
    
    Логика:
    1. Если матч "полный" по данным:
       - NORMAL режим: return (prob >= base_threshold)
    
    2. Если матч "неполный":
       - FALLBACK режим:
         - Если coverage < FALLBACK_MIN_COVERAGE: блокируем (слишком мало данных)
         - Иначе: вычисляем final_threshold с штрафом
         - Если xG отсутствует: оцениваем его
         - Решение: (prob >= final_threshold) OR (est_total_xg >= ESTIMATED_XG_GATE)
    
    Args:
        prob: Вероятность гола (в процентах, 0-100)
        base_threshold: Базовый порог (напр. PROB_SEND_THRESHOLD)
        fixture_metrics: Словарь метрик матча
        
    Returns:
        True если отправлять сигнал, False иначе
    """
    # Проверяем полноту матча
    is_full = is_full_match(fixture_metrics)
    
    if is_full:
        # NORMAL режим: как раньше
        decision = prob >= base_threshold
        logger.info(f"[DECISION] NORMAL mode: prob={prob:.2f} >= threshold={base_threshold:.2f} -> {decision}")
        return decision
    
    # FALLBACK режим
    coverage, missing = compute_coverage_score(fixture_metrics)
    
    # Получаем match_id и minute для логирования
    match_id = get_fixture_id(fixture_metrics)
    try:
        minute_val = fixture_metrics.get("elapsed")
        if isinstance(minute_val, dict):
            minute = int(minute_val.get("value", 0))
        else:
            minute = int(minute_val or 0)
    except (TypeError, ValueError):
        minute = 0
    
    # БЛОКИРОВКА: если нет статистики (coverage == 0.0), блокируем fixture
    if coverage == 0.0:
        if match_id is not None:
            block_no_stats_fixture(match_id, minute, coverage)
            block_no_stats_match_long(str(match_id), "coverage=0.00")
        return False
    
    # Проверяем минимальное покрытие
    if coverage < FALLBACK_MIN_COVERAGE:
        logger.info(
            f"[FALLBACK] BLOCKED coverage too low: {coverage:.2f} < {FALLBACK_MIN_COVERAGE} "
            f"missing={missing}"
        )
        return False
    
    # Вычисляем final_threshold с штрафом за неполноту
    final_threshold = base_threshold + (1.0 - coverage) * FALLBACK_PENALTY_MAX
    
    # Проверяем наличие real xG
    xg_home = get_any_metric(fixture_metrics, ["expected_goals"], "home")
    xg_away = get_any_metric(fixture_metrics, ["expected_goals"], "away")
    
    # Если real xG отсутствует, оцениваем
    if not has_value(xg_home) or not has_value(xg_away):
        # Собираем метрики в простой dict для estimate_xg
        stats = {
            "shots_total_home": get_any_metric(fixture_metrics, ["total_shots"], "home"),
            "shots_total_away": get_any_metric(fixture_metrics, ["total_shots"], "away"),
            "shots_on_target_home": get_any_metric(fixture_metrics, ["shots_on_target"], "home"),
            "shots_on_target_away": get_any_metric(fixture_metrics, ["shots_on_target"], "away"),
            "shots_in_box_home": get_any_metric(fixture_metrics, ["shots_insidebox", "inside_box"], "home"),
            "shots_in_box_away": get_any_metric(fixture_metrics, ["shots_insidebox", "inside_box"], "away"),
            "corners_home": get_any_metric(fixture_metrics, ["corner_kicks", "corners"], "home"),
            "corners_away": get_any_metric(fixture_metrics, ["corner_kicks", "corners"], "away"),
            "saves_home": get_any_metric(fixture_metrics, ["saves"], "home"),
            "saves_away": get_any_metric(fixture_metrics, ["saves"], "away"),
            "possession_home": get_any_metric(fixture_metrics, ["ball_possession", "passes_%"], "home"),
            "possession_away": get_any_metric(fixture_metrics, ["ball_possession", "passes_%"], "away"),
            "pressure_index": get_any_metric(fixture_metrics, ["pressure_index"], "home") or 
                             get_any_metric(fixture_metrics, ["pressure_index"], "away") or
                             get_any_metric(fixture_metrics, ["pressure_index"], None),
        }
        est_home, est_away, est_total_xg = estimate_xg(stats)
        logger.debug(f"[FALLBACK] estimated xG: home={est_home:.2f} away={est_away:.2f} total={est_total_xg:.2f}")
    else:
        # Используем real xG
        try:
            est_total_xg = num(xg_home) + num(xg_away)
        except (TypeError, ValueError):
            # На случай если что-то пошло не так, используем оценку
            stats = {
                "shots_total_home": get_any_metric(fixture_metrics, ["total_shots"], "home"),
                "shots_total_away": get_any_metric(fixture_metrics, ["total_shots"], "away"),
                "shots_on_target_home": get_any_metric(fixture_metrics, ["shots_on_target"], "home"),
                "shots_on_target_away": get_any_metric(fixture_metrics, ["shots_on_target"], "away"),
                "shots_in_box_home": get_any_metric(fixture_metrics, ["shots_insidebox", "inside_box"], "home"),
                "shots_in_box_away": get_any_metric(fixture_metrics, ["shots_insidebox", "inside_box"], "away"),
                "corners_home": get_any_metric(fixture_metrics, ["corner_kicks", "corners"], "home"),
                "corners_away": get_any_metric(fixture_metrics, ["corner_kicks", "corners"], "away"),
                "saves_home": get_any_metric(fixture_metrics, ["saves"], "home"),
                "saves_away": get_any_metric(fixture_metrics, ["saves"], "away"),
                "possession_home": get_any_metric(fixture_metrics, ["ball_possession", "passes_%"], "home"),
                "possession_away": get_any_metric(fixture_metrics, ["ball_possession", "passes_%"], "away"),
                "pressure_index": get_any_metric(fixture_metrics, ["pressure_index"], "home") or 
                                 get_any_metric(fixture_metrics, ["pressure_index"], "away") or
                                 get_any_metric(fixture_metrics, ["pressure_index"], None),
            }
            est_home, est_away, est_total_xg = estimate_xg(stats)
    
    # Решение: prob >= final_threshold ИЛИ est_total_xg >= gate
    decision = (prob >= final_threshold) or (est_total_xg >= ESTIMATED_XG_GATE)
    
    logger.info(
        f"[FALLBACK] cov={coverage:.2f} base={base_threshold:.2f} final={final_threshold:.2f} "
        f"prob={prob:.2f} est_xg={est_total_xg:.2f} gate={ESTIMATED_XG_GATE:.2f} "
        f"missing={missing} -> {'SEND' if decision else 'BLOCK'}"
    )
    
    return decision


def estimate_xg_from_metrics_combined(fixture_metrics: Dict[str,Any], side: str) -> float:
    """
    Получает xG для команды: если есть в API — используем, иначе считаем через fallback.
    
    Args:
        fixture_metrics: Словарь метрик матча
        side: "home" или "away"
        
    Returns:
        xG команды (float >= 0.0)
    """
    # Попытаемся получить xG из API
    eg = get_any_metric(fixture_metrics, ["expected_goals"], side)
    if eg is not None and eg != 0:
        try:
            api_xg = float(eg)
            if api_xg > 0.0:
                return api_xg
        except (TypeError, ValueError):
            pass
    
    # Если xG не в API или равен 0/None — считаем через fallback
    shots_total = _to_int(get_any_metric(fixture_metrics, ["total_shots"], side), 0)
    shots_on_target = _to_int(get_any_metric(fixture_metrics, ["shots_on_target"], side), 0)
    shots_in_box = _to_int(get_any_metric(fixture_metrics, ["shots_insidebox", "inside_box"], side), 0)
    corners = _to_int(get_any_metric(fixture_metrics, ["corner_kicks", "corners"], side), 0)
    
    # Получаем глобальные бонусные метрики
    # Примечание: pressure_index, possession_pressure, save_stress часто глобальные (не по командам)
    pressure_index = _to_float(get_any_metric(fixture_metrics, ["pressure_index"], "home") or 
                               get_any_metric(fixture_metrics, ["pressure_index"], None), 0.0)
    possession_pressure = _to_float(get_any_metric(fixture_metrics, ["possession_pressure"], "home") or 
                                    get_any_metric(fixture_metrics, ["possession_pressure"], None), 0.0)
    save_stress = _to_float(get_any_metric(fixture_metrics, ["save_stress"], side), 0.0)
    
    # Вычисляем xG через fallback
    xg_fallback = compute_xg_fallback(
        shots_total=shots_total,
        shots_on_target=shots_on_target,
        shots_in_box=shots_in_box,
        corners=corners,
        pressure_index=pressure_index,
        possession_pressure=possession_pressure,
        save_stress=save_stress
    )
    
    return xg_fallback

def get_metric_from_fixture(fixture: Dict[str,Any], key: str, default=None):
    v = fixture.get(key)
    if isinstance(v, dict) and "value" in v:
        return v["value"]
    return fixture.get(key, default)

def compute_lambda_and_probability(fixture_metrics: Dict[str,Any], minute: int, home_side: str="home", away_side: str="away") -> Dict[str,Any]:
    """
    Compute lambda intensities and multiple probability metrics.
    
    Returns:
        Dictionary with:
        - lambda_home, lambda_away: goal intensities per minute
        - prob_next_15: probability of goal in next 15 minutes (%)
        - prob_until_end: probability of goal until match end (%)
        - prob_goal_either_to75: legacy metric (%)
        - prob_goal_either_to90: probability to 90 min (%)
        - prob_next_goal_home, prob_next_goal_away: probabilities by team (%)
        - pred_xg_home_to_75, pred_xg_away_to_75: predicted xG values
    """
    try:
        eg_h = get_any_metric(fixture_metrics, ["expected_goals"], "home")
        eg_a = get_any_metric(fixture_metrics, ["expected_goals"], "away")
        try:
            eg_h = float(eg_h) if eg_h not in (None, "", 0) else None
            eg_a = float(eg_a) if eg_a not in (None, "", 0) else None
        except Exception:
            eg_h = eg_a = None

        est_h = estimate_xg_from_metrics_combined(fixture_metrics, "home")
        est_a = estimate_xg_from_metrics_combined(fixture_metrics, "away")
        
        score_h = int(get_metric_from_fixture(fixture_metrics, "score_home", 0) or 0)
        score_a = int(get_metric_from_fixture(fixture_metrics, "score_away", 0) or 0)
        
        # Используем остаточный xG вместо текущего
        raw_xg_h = eg_h if eg_h is not None else est_h
        raw_xg_a = eg_a if eg_a is not None else est_a
        curr_xg_h = max(0.0, raw_xg_h - score_h * 0.6) if raw_xg_h is not None else 0.0
        curr_xg_a = max(0.0, raw_xg_a - score_a * 0.6) if raw_xg_a is not None else 0.0

        lam_h_emp = max(0.001, curr_xg_h / max(1.0, minute)) if curr_xg_h is not None else 0.001
        lam_a_emp = max(0.001, curr_xg_a / max(1.0, minute)) if curr_xg_a is not None else 0.001
        diff = score_h - score_a
        if diff > 0:
            mult_h_score = max(0.4, 1.0 - 0.12 * diff)
            mult_a_score = 1.0 + 0.15 * diff
        elif diff < 0:
            mult_a_score = max(0.4, 1.0 - 0.12 * (-diff))
            mult_h_score = 1.0 + 0.15 * (-diff)
        else:
            mult_h_score = mult_a_score = 1.0

        attacks_h = float(get_any_metric(fixture_metrics, ["attacks"], "home") or 0)
        attacks_a = float(get_any_metric(fixture_metrics, ["attacks"], "away") or 0)
        tempo_h = 1.0 + 0.12 * ((attacks_h / max(1.0, 12.0)) - 1.0)
        tempo_a = 1.0 + 0.12 * ((attacks_a / max(1.0, 12.0)) - 1.0)
        tempo_h = max(0.6, min(2.0, tempo_h))
        tempo_a = max(0.6, min(2.0, tempo_a))

        possession_h = float(get_any_metric(fixture_metrics, ["ball_possession", "passes_%"], "home") or 50) / 50.0
        possession_a = float(get_any_metric(fixture_metrics, ["ball_possession", "passes_%"], "away") or 50) / 50.0
        possession_h = max(0.6, min(1.4, possession_h))
        possession_a = max(0.6, min(1.4, possession_a))

        mult_h = mult_h_score * tempo_h * possession_h
        mult_a = mult_a_score * tempo_a * possession_a

        lam_h = max(0.001, min(1.0, lam_h_emp * mult_h))
        lam_a = max(0.001, min(1.0, lam_a_emp * mult_a))

        remain = max(0, 75 - (minute or 0))
        prob_any = 1.0 - math.exp(- (lam_h + lam_a) * remain)
        prob_home = (lam_h / (lam_h + lam_a)) * prob_any if (lam_h + lam_a) > 0 else 0.0
        prob_away = (lam_a / (lam_h + lam_a)) * prob_any if (lam_h + lam_a) > 0 else 0.0

        remain_90 = max(0, 90 - (minute or 0))
        prob_any_90 = 1.0 - math.exp(- (lam_h + lam_a) * remain_90)
        
        # Calculate prob_next_15: probability of goal in next 15 minutes
        # Always calculate, even after 75th minute (window just shifts)
        time_window_15 = 15
        prob_next_15 = 1.0 - math.exp(- (lam_h + lam_a) * time_window_15)
        
        # Calculate prob_until_end: probability of goal until match end
        # Estimate match end time (90 min + potential stoppage time)
        # Use remaining time to 90 as baseline, since we don't know exact stoppage time
        time_until_end = max(0, 90 - (minute or 0))
        # If already past 90, assume small remaining time (e.g., 5 minutes for stoppage)
        if time_until_end == 0 and minute >= 90:
            time_until_end = max(0, 95 - minute)  # Assume up to 95th minute
        
        prob_until_end = 1.0 - math.exp(- (lam_h + lam_a) * time_until_end)
        # prob_until_end should be >= prob_next_15 in most cases
        # (unless we're very close to the end)

        league_info = _extract_league_context(fixture_metrics)
        league_name = str(league_info.get("name") or "")
        league_id = league_info.get("league_id")
        team_boosts = calc_match_team_boost_v2(fixture_metrics)
        league_factor = float(team_boosts.get("league_factor", 1.0) or 1.0)
        team_mix_factor = float(team_boosts.get("team_mix_factor", 1.0) or 1.0)
        combined_m = float(team_boosts.get("combined_m", 1.0) or 1.0)
        factor_source = str(team_boosts.get("league_factor_source") or "default")
        persist_avg_goals = float(team_boosts.get("league_avg_goals", GLOBAL_GOALS_BASE) or GLOBAL_GOALS_BASE)
        
        # Store base probabilities (before league adjustment) for logging
        base_prob_to75 = round(prob_any * 100, 2)
        base_prob_to90 = round(prob_any_90 * 100, 2)
        base_prob_next15 = round(prob_next_15 * 100, 2)
        base_prob_until_end = round(prob_until_end * 100, 2)

        p75_final = _apply_intensity_scaling(prob_any, combined_m, FINAL_PROB_CAP)
        p90_final = _apply_intensity_scaling(prob_any_90, combined_m, FINAL_PROB_CAP)
        adj_prob_to75 = p75_final * 100.0
        adj_prob_to90 = p90_final * 100.0

        p15_final = _apply_intensity_scaling(prob_next_15, combined_m, FINAL_PROB_CAP)
        pend_final = _apply_intensity_scaling(prob_until_end, combined_m, FINAL_PROB_CAP)
        ph_final = _apply_intensity_scaling(prob_home, combined_m, FINAL_PROB_CAP)
        pa_final = _apply_intensity_scaling(prob_away, combined_m, FINAL_PROB_CAP)

        fixture_id = get_fixture_id(fixture_metrics)
        logger.info(
            "[BOOST_V2] "
            f"fixture_id={fixture_id} "
            f"base_prob_75={prob_any:.6f} "
            f"base_prob_90={prob_any_90:.6f} "
            f"league_avg_goals={team_boosts.get('league_avg_goals')} "
            f"league_factor={league_factor:.6f} "
            f"league_avg_team={team_boosts.get('league_avg_team')} "
            f"home_matches={team_boosts.get('home_matches')} "
            f"home_avg_scored_raw={team_boosts.get('home_avg_scored_raw')} "
            f"home_avg_conceded_raw={team_boosts.get('home_avg_conceded_raw')} "
            f"home_gf_used={team_boosts.get('home_gf_used')} "
            f"home_ga_used={team_boosts.get('home_ga_used')} "
            f"home_attack_factor={team_boosts.get('home_attack_factor')} "
            f"home_defense_factor={team_boosts.get('home_defense_factor')} "
            f"away_matches={team_boosts.get('away_matches')} "
            f"away_avg_scored_raw={team_boosts.get('away_avg_scored_raw')} "
            f"away_avg_conceded_raw={team_boosts.get('away_avg_conceded_raw')} "
            f"away_gf_used={team_boosts.get('away_gf_used')} "
            f"away_ga_used={team_boosts.get('away_ga_used')} "
            f"away_attack_factor={team_boosts.get('away_attack_factor')} "
            f"away_defense_factor={team_boosts.get('away_defense_factor')} "
            f"team_mix_factor={team_mix_factor:.6f} "
            f"combined_M={combined_m:.6f} "
            f"final_prob_75={p75_final:.6f} "
            f"final_prob_90={p90_final:.6f}"
        )

        return {
            "lambda_home": lam_h, "lambda_away": lam_a,
            "prob_next_15": round(p15_final * 100.0, 2),
            "prob_until_end": round(pend_final * 100.0, 2),
            "prob_goal_either_to75": round(adj_prob_to75, 2),
            "prob_goal_either_to90": round(adj_prob_to90, 2),
            "prob_next_goal_home": round(ph_final * 100.0, 2),
            "prob_next_goal_away": round(pa_final * 100.0, 2),
            "pred_xg_home_to_75": round((curr_xg_h or 0.0) + lam_h * remain, 3),
            "pred_xg_away_to_75": round((curr_xg_a or 0.0) + lam_a * remain, 3),
            # League adjustment metadata (for logging)
            "league_id": league_id,
            "league_name": league_name,
            "league_avg_goals": persist_avg_goals,
            "league_factor": round(league_factor, 3),
            "league_factor_source": factor_source,
            "home_team_boost": round(float(team_boosts.get("home_attack_factor", 1.0) or 1.0), 3),
            "away_team_boost": round(float(team_boosts.get("away_attack_factor", 1.0) or 1.0), 3),
            "match_team_boost": round(combined_m, 3),
            "home_attack_factor": round(float(team_boosts.get("home_attack_factor", 1.0) or 1.0), 3),
            "away_attack_factor": round(float(team_boosts.get("away_attack_factor", 1.0) or 1.0), 3),
            "home_defense_factor": round(float(team_boosts.get("home_defense_factor", 1.0) or 1.0), 3),
            "away_defense_factor": round(float(team_boosts.get("away_defense_factor", 1.0) or 1.0), 3),
            "team_mix_factor": round(team_mix_factor, 3),
            "combined_m": round(combined_m, 3),
            "base_prob_to75": base_prob_to75,
            "base_prob_to90": base_prob_to90,
            "adj_prob_to75": round(adj_prob_to75, 2),
            "adj_prob_to90": round(adj_prob_to90, 2),
        }
    except Exception:
        logger.exception("compute_lambda_and_probability")
        return {"lambda_home":0.001,"lambda_away":0.001,"prob_goal_either_to75":0.0,"prob_next_goal_home":0.0,"prob_next_goal_away":0.0,"pred_xg_home_to_75":0.0,"pred_xg_away_to_75":0.0}

# -------------------------
# Monitoring & handling events after sending a signal
# -------------------------
def _make_event_key(ev: Dict[str, Any]) -> str:
    eid = ev.get("event_id")
    if eid:
        return f"evt:{eid}"
    minute = ev.get("minute")
    typ = (ev.get("type") or "")
    team = (ev.get("team_name") or "") or str(ev.get("raw", {}).get("team", {}))
    player = (ev.get("player_name") or "") or str(ev.get("raw", {}).get("player", {}))
    note = (ev.get("note") or "")
    return f"evt__{minute}__{typ}__{team}__{player}__{hash(note)}"

def _event_happened_after_signal(match_id: int, ev: Dict[str,Any]) -> bool:
    try:
        with state_lock:
            sent_ts = state.get("match_sent_at", {}).get(str(match_id))
            sent_min = state.get("match_initial_minute", {}).get(str(match_id))
        ev_raw = ev.get("raw") or {}
        ev_time = ev_raw.get("time") or {}
        ev_ts = None
        if isinstance(ev_time, dict):
            ev_ts = ev_time.get("timestamp") or ev_time.get("time")
        if not ev_ts:
            ev_ts = ev_raw.get("timestamp")
        if ev_ts and sent_ts:
            try:
                ev_ts_f = float(ev_ts)
                return float(ev_ts_f) >= float(sent_ts)
            except Exception:
                pass
        ev_min = ev.get("minute")
        if ev_min is None:
            try:
                if isinstance(ev_time, dict):
                    ev_min = ev_time.get("elapsed")
                else:
                    ev_min = ev_raw.get("elapsed")
            except Exception:
                ev_min = None
        try:
            if ev_min is not None:
                ev_min_i = int(ev_min)
            else:
                return True
        except Exception:
            try:
                ev_min_i = int(float(ev_min))
            except Exception:
                return True
        if sent_min is None:
            return True
        try:
            sent_min_i = int(sent_min)
        except Exception:
            try:
                sent_min_i = int(float(sent_min))
            except Exception:
                return True
        if ev_min_i < sent_min_i:
            return False
        return True
    except Exception:
        return True

def _score_change_allowed_fallback(match_id: int) -> bool:
    try:
        with state_lock:
            sent_ts = state.get("match_sent_at", {}).get(str(match_id))
            init_score_exists = str(match_id) in state.get("match_initial_score", {})
        return bool(sent_ts and init_score_exists)
    except Exception:
        return False

def recompute_goals_after_signal(match_id: int, events: List[Dict[str, Any]], init_score: Tuple[int, int], cur_score: Tuple[int, int]) -> List[int]:
    """
    Rebuild list of goal minutes after signal based ONLY on valid (counted) events.
    Also limits list length to actual number of goals after signal based on score.
    
    This ensures that:
    - Only counted goals appear (no cancelled/VAR disallowed)
    - Number of goal minutes matches actual goal count
    - Works correctly during match and after finish
    
    Args:
        match_id: Match ID
        events: List of match events
        init_score: Score at signal time (home, away)
        cur_score: Current score (home, away)
    
    Returns:
        Sorted list of goal minutes (only counted goals, limited to actual goal count)
    """
    minutes = []
    
    for ev in events or []:
        # Only events after signal
        if not _event_happened_after_signal(match_id, ev):
            continue
        
        # Only valid counted goals (not cancelled/disallowed)
        typ = (ev.get("type") or "").lower()
        if "goal" not in typ:
            continue
        
        if not _is_valid_counted_goal(ev):
            continue
        
        # Use new extraction function that handles extra time correctly
        m = _extract_event_minute(ev)
        if m is None:
            continue
        
        minutes.append(m)
    
    # Remove duplicates and sort chronologically
    minutes = sorted(set(minutes))
    
    # Limit by actual number of goals after signal (based on score)
    goals_after_signal = max(0, (cur_score[0] + cur_score[1]) - (init_score[0] + init_score[1]))
    
    if goals_after_signal == 0:
        return []
    
    if len(minutes) > goals_after_signal:
        # Take first N minutes (chronologically) to match actual goal count
        # This handles cases where we have extra minutes due to processing issues
        minutes = minutes[:goals_after_signal]
    
    return minutes

# -------------------------
# perform_monitor_update (modified to implement temporary goal/cancellation messages)
# -------------------------
def perform_monitor_update(match_id: int, client: APISportsMetricsClient):
    try:
        if is_ignored_match(match_id):
            logger.debug(f"[MONITOR] Skipping update for ignored match {match_id}")
            return

        data = client.collect_match_all(match_id)
        if not data:
            logger.debug(f"[MONITOR] No data for match {match_id}")
            return
        fixture = data.get("fixture", {}) or {}
        events = data.get("events", []) or []

        minute = int((fixture.get("elapsed") or {}).get("value") or 0)
        status_short = _normalize_status_short(fixture)
        current_home = int((fixture.get("score_home") or {}).get("value") or 0)
        current_away = int((fixture.get("score_away") or {}).get("value") or 0)
        current_score = (current_home, current_away)
        is_terminal_status = status_short in ("FT", "AET", "PEN", "CANC", "ABD", "AWD", "WO")

        with state_lock:
            tracked_rec = state.get("tracked_matches", {}).get(str(match_id))
            sent_mid = state.get("sent_matches", {}).get(str(match_id))

        if sent_mid and not isinstance(tracked_rec, dict):
            logger.warning("[TRACK_MISS] fixture=%s sent but not tracked", match_id)
            with state_lock:
                init_score = state.get("match_initial_score", {}).get(str(match_id), current_score)
                init_minute = state.get("match_initial_minute", {}).get(str(match_id), minute)
            try:
                init_home = int(init_score[0])
                init_away = int(init_score[1])
            except Exception:
                init_home, init_away = current_home, current_away

            add_tracked_match(
                fixture_id=int(match_id),
                message_id=int(sent_mid),
                chat_id=int(TELEGRAM_CHAT_ID),
                score_home=init_home,
                score_away=init_away,
                status="LIVE",
                sent_at_ts=time.time(),
                signal_minute=int(init_minute or 0),
            )
            with state_lock:
                tracked_rec = state.get("tracked_matches", {}).get(str(match_id))

        tracked_msg_id = None
        if isinstance(tracked_rec, dict):
            try:
                tracked_msg_id = int(tracked_rec.get("message_id"))
            except Exception:
                tracked_msg_id = sent_mid
        else:
            tracked_msg_id = sent_mid

        if tracked_msg_id:
            logger.info("[TRACK] updating fixture=%s msg_id=%s minute=%s", match_id, tracked_msg_id, minute)

        def _get_signal_score_snapshot() -> Tuple[int, int]:
            with state_lock:
                sent_rec = state.get("sent", {}).get(str(match_id), {})
                tracked_local = state.get("tracked_matches", {}).get(str(match_id), {})
                init_score_raw = state.setdefault("match_initial_score", {}).get(str(match_id), current_score)

            signal_home = None
            signal_away = None

            if isinstance(sent_rec, dict) and isinstance(sent_rec.get("signal_score"), dict):
                signal_home = sent_rec["signal_score"].get("home")
                signal_away = sent_rec["signal_score"].get("away")

            if (signal_home is None or signal_away is None) and isinstance(tracked_local, dict):
                score_sig = tracked_local.get("signal_score")
                score_at = tracked_local.get("score_at_signal")
                source = score_sig if isinstance(score_sig, dict) else (score_at if isinstance(score_at, dict) else {})
                signal_home = source.get("home", signal_home)
                signal_away = source.get("away", signal_away)

            try:
                if signal_home is None or signal_away is None:
                    signal_home = int(init_score_raw[0])
                    signal_away = int(init_score_raw[1])
                return int(signal_home), int(signal_away)
            except Exception:
                return int(current_home), int(current_away)

        prev_tracking = get_persistent_fixture_tracking(match_id)
        if not prev_tracking:
            with state_lock:
                state.setdefault("match_initial_score", {})[str(match_id)] = current_score
                state.setdefault("match_initial_minute", {})[str(match_id)] = minute
                state.setdefault("match_goal_status", {})[str(match_id)] = {
                    "score": current_score,
                    "finalized": False,
                    "last_reported_minute": minute,
                    "last_pressure_index": calculate_pressure_index(fixture),
                }
                mark_state_dirty()

            prob_snapshot = compute_lambda_and_probability(fixture, minute).get("prob_goal_either_to75", 0.0)
            signal_score_snapshot = _get_signal_score_snapshot()
            header_text = render_live_message(
                current_data=data,
                signal_score=signal_score_snapshot,
                prob_display=prob_snapshot,
                prob_display_90=None,
                is_admin_approved=False,
            )
            live_footer = build_live_footer(
                current_data=data,
                scored_goals_after_signal=[],
                flash_goal_until_ts=0.0,
                now_ts=time.time(),
                force_final=False,
            )
            msg_snapshot = compose_signal_message(header_text, live_footer)
            with state_lock:
                has_sent_message = str(match_id) in state.get("sent_matches", {})
            if has_sent_message:
                edit_telegram_message_improved(msg_snapshot, match_id)
            update_persistent_fixture_tracking(match_id, fixture, sent_text=msg_snapshot)
            logger.info("[MONITOR] match=%s initialized from official scoreboard without event replay", match_id)
            return

        try:
            last_home = int(prev_tracking.get("last_home_score", prev_tracking.get("last_score_home", 0)) or 0)
            last_away = int(prev_tracking.get("last_away_score", prev_tracking.get("last_score_away", 0)) or 0)
        except Exception:
            last_home, last_away = current_home, current_away

        last_total = last_home + last_away
        current_total = current_home + current_away
        score_changed = (current_home != last_home) or (current_away != last_away)
        goal_happened = current_total > last_total

        # Key metrics for null-update protection
        xg_h = get_any_metric(fixture, ["expected_goals"], "home") or 0.0
        xg_a = get_any_metric(fixture, ["expected_goals"], "away") or 0.0

        total_shots_h = int(get_any_metric(fixture, ["total_shots"], "home") or 0)
        total_shots_a = int(get_any_metric(fixture, ["total_shots"], "away") or 0)
        shots_on_h = int(get_any_metric(fixture, ["shots_on_target"], "home") or 0)
        shots_on_a = int(get_any_metric(fixture, ["shots_on_target"], "away") or 0)
        corners_h = int(get_any_metric(fixture, ["corner_kicks","corners"], "home") or 0)
        corners_a = int(get_any_metric(fixture, ["corner_kicks","corners"], "away") or 0)
        saves_h = int(get_any_metric(fixture, ["saves"], "home") or 0)
        saves_a = int(get_any_metric(fixture, ["saves"], "away") or 0)
        possession_h = float(get_any_metric(fixture, ["ball_possession","passes_%"], "home") or 0.0)
        possession_a = float(get_any_metric(fixture, ["ball_possession","passes_%"], "away") or 0.0)

        is_null_update = (
            minute == 0 and
            float(xg_h) == 0.0 and float(xg_a) == 0.0 and
            (total_shots_h + total_shots_a) == 0 and
            (shots_on_h + shots_on_a) == 0 and
            (int(round(possession_h)) == 0 and int(round(possession_a)) == 0)
        )
        if is_null_update:
            logger.debug(f"[MONITOR] Ignoring null/zero update for match {match_id} (minute=0 and metrics zero).")
            return

        signal_score_snapshot = _get_signal_score_snapshot()

        with state_lock:
            init_score = tuple(signal_score_snapshot)
            st = state.setdefault("match_goal_status", {}).get(str(match_id))
            if st is None:
                state.setdefault("match_goal_status", {})[str(match_id)] = {
                    "score": current_score,
                    "finalized": False,
                    "last_reported_minute": None,
                    "last_pressure_index": calculate_pressure_index(fixture),
                }
            st = state["match_goal_status"][str(match_id)]

        try:
            init_home_i = int(init_score[0])
            init_away_i = int(init_score[1])
        except Exception:
            init_home_i, init_away_i = current_home, current_away
        init_score = (init_home_i, init_away_i)

        scored_goals_after_signal = recompute_goals_after_signal(match_id, events, init_score, current_score)
        now_ts = time.time()
        with state_lock:
            prev_goals = state.setdefault("match_goals_after_signal", {}).get(str(match_id), [])
            if not isinstance(prev_goals, list):
                prev_goals = []

            tracked = state.setdefault("tracked_matches", {})
            tracked_goal_state = tracked.get(str(match_id), {}) if isinstance(tracked, dict) else {}
            if not isinstance(tracked_goal_state, dict):
                tracked_goal_state = {}
            prev_tracked_goals = tracked_goal_state.get("last_known_scored_goals", prev_goals)
            if not isinstance(prev_tracked_goals, list):
                prev_tracked_goals = []

            flash_goal_until_ts = float(tracked_goal_state.get("flash_goal_until_ts", 0.0) or 0.0)

            if len(scored_goals_after_signal) > len(prev_tracked_goals):
                flash_goal_until_ts = now_ts + 90
            elif len(scored_goals_after_signal) < len(prev_tracked_goals):
                flash_goal_until_ts = 0.0

            if scored_goals_after_signal != prev_goals:
                state.setdefault("match_goals_after_signal", {})[str(match_id)] = list(scored_goals_after_signal)
                mark_state_dirty()

        with state_lock:
            tracked = state.setdefault("tracked_matches", {})
            tracked_rec = tracked.get(str(match_id)) if isinstance(tracked, dict) else None
            if isinstance(tracked_rec, dict):
                tracked_rec["last_known_scored_goals"] = list(scored_goals_after_signal)
                tracked_rec["flash_goal_until_ts"] = float(flash_goal_until_ts)
                tracked_rec["last_known_score"] = {"home": int(current_home), "away": int(current_away)}
                tracked[str(match_id)] = tracked_rec

            sent_map = state.setdefault("sent", {})
            sent_rec = sent_map.get(str(match_id))
            if isinstance(sent_rec, dict):
                sent_rec["last_known_scored_goals"] = list(scored_goals_after_signal)
                sent_rec["flash_goal_until_ts"] = float(flash_goal_until_ts)
                sent_rec["last_known_score"] = {"home": int(current_home), "away": int(current_away)}
                sent_map[str(match_id)] = sent_rec
            mark_state_dirty()

        # Key metrics non-zero condition for editing
        key_metrics_nonzero = any([
            (float(xg_h) > 0.0 or float(xg_a) > 0.0),
            (total_shots_h + total_shots_a) > 0,
            (shots_on_h + shots_on_a) > 0,
            int(round(possession_h)) > 0 or int(round(possession_a)) > 0,
            (corners_h + corners_a) > 0,
            (saves_h + saves_a) > 0
        ])

        def editing_allowed() -> bool:
            if st.get("finalized", False):
                return False
            if st.get("last_reported_minute") is None:
                return minute >= 1
            try:
                if minute > int(st.get("last_reported_minute")):
                    return True
                return False
            except Exception:
                return False

        can_edit_now = editing_allowed()

        elapsed_label = (fixture.get("elapsed_label") or {}).get("value") or ""
        status_value_raw = fixture.get("status")
        status_value = ""
        if isinstance(status_value_raw, dict):
            status_value = str(status_value_raw.get("value") or "").strip().lower()
        else:
            status_value = str(status_value_raw or "").strip().lower()

        is_finished = status_short in FINISHED_STATUSES
        if status_value in ("finished", "match finished"):
            is_finished = True
        if isinstance(elapsed_label, str) and elapsed_label.strip().lower() in ("ft", "finished", "match finished"):
            is_finished = True

        if is_finished:
            with state_lock:
                signal_info = state.get("match_signal_info", {}).get(str(match_id))
                if isinstance(signal_info, dict) and not bool(signal_info.get("is_finished", False)):
                    signal_info["is_finished"] = True
                    logger.info("[TRACK] match finished fixture=%s status=%s", match_id, status_short)
                    mark_state_dirty()

        need_refresh = can_edit_now or score_changed or is_finished

        res = None
        if need_refresh:
            res = compute_lambda_and_probability(fixture, minute)
            prob_now = res.get("prob_goal_either_to75", 0.0)
        else:
            with state_lock:
                prob_now = state.setdefault("match_prob_status", {}).get(str(match_id), 0.0)

        updated = False

        if need_refresh and not st.get("finalized", False) and not is_finished:
            is_admin_approved = st.get("approved_by_admin", False)
            prob_90 = res.get("prob_goal_either_to90") if res else None
            main_text = render_live_message(
                current_data=data,
                signal_score=signal_score_snapshot,
                prob_display=prob_now,
                prob_display_90=prob_90,
                is_admin_approved=is_admin_approved,
            )
            live_footer = build_live_footer(
                current_data=data,
                scored_goals_after_signal=scored_goals_after_signal,
                flash_goal_until_ts=flash_goal_until_ts,
                now_ts=now_ts,
                force_final=False,
            )
            msg_regular = compose_signal_message(main_text, live_footer)
            if edit_telegram_message_improved(msg_regular, match_id):
                updated = True
                mark_tracked_match_update(match_id, current_home, current_away, edited=True, status="LIVE")
                current_pressure = calculate_pressure_index(fixture)
                with state_lock:
                    previous_pressure = state["match_goal_status"][str(match_id)].get("last_pressure_index", current_pressure)
                    momentum = calculate_momentum(previous_pressure, current_pressure)
                    state["match_goal_status"][str(match_id)]["last_pressure_index"] = current_pressure
                    state["match_goal_status"][str(match_id)]["last_reported_minute"] = minute
                    state["match_goal_status"][str(match_id)]["score"] = current_score

                if not is_finished:
                    prob_next_15 = res.get("prob_next_15", 0.0) if res else 0.0
                    prob_until_end = res.get("prob_until_end", 0.0) if res else 0.0
                    save_match_to_sheet(
                        match_id,
                        data,
                        minute,
                        prob_next_15,
                        prob_until_end,
                        signal_sent=False,
                        pressure_index=current_pressure,
                        momentum=momentum,
                    )

        with state_lock:
            finalized_flag = state["match_goal_status"][str(match_id)].get("finalized", False)

        if is_finished and not finalized_flag:
            cur_score = (
                int((fixture.get("score_home") or {}).get("value") or 0),
                int((fixture.get("score_away") or {}).get("value") or 0),
            )

            signal_home = int(signal_score_snapshot[0])
            signal_away = int(signal_score_snapshot[1])
            final_home = int(cur_score[0])
            final_away = int(cur_score[1])

            with state_lock:
                is_admin_approved = state["match_goal_status"][str(match_id)].get("approved_by_admin", False)

            header_text = "🚨 Сигнал от админа" if is_admin_approved else "🚨 Сигнал"

            msg = render_final_message({
                "data": data,
                "fixture_id": match_id,
                "signal_home": signal_home,
                "signal_away": signal_away,
                "final_home": final_home,
                "final_away": final_away,
                "goal_minutes": scored_goals_after_signal,
            }, header_text=header_text)
            logger.info(
                "[MESSAGE_MODE] FINAL rendered match=%s header=\"%s\" final=%s-%s signal=%s-%s goals=%s",
                match_id,
                header_text,
                final_home,
                final_away,
                signal_home,
                signal_away,
                scored_goals_after_signal,
            )

            edited_final = edit_telegram_message_improved(msg, match_id)
            if edited_final:
                final_badge = "✅" if bool(scored_goals_after_signal) else "❌"
                final_text = f"{final_badge} Финальный счёт: {cur_score[0]} - {cur_score[1]}"

                with state_lock:
                    state["match_goal_status"][str(match_id)]["finalized"] = True
                    state["match_goal_status"][str(match_id)]["final_line"] = final_text
                    state["match_goal_status"][str(match_id)]["last_reported_minute"] = None

                    # Mark match as finished for daily stats tracking
                    if str(match_id) in state.get("match_signal_info", {}):
                        state["match_signal_info"][str(match_id)]["is_finished"] = True
                        logger.info(f"[STATS] Match {match_id} marked as finished")

                    # Label Google Sheets rows with goal_next_15 (only once)
                    labeled = state.get("labeled_matches", [])
                    already_labeled = match_id in labeled

                    if not already_labeled and GSHEETS_AVAILABLE:
                        try:
                            # Extract all goal minutes from events
                            all_events = data.get("events", [])
                            goals_minutes = []
                            for ev in all_events:
                                ev_type = ev.get("event_type", "")
                                # Include goals but exclude own goals
                                if ev_type == "goal":
                                    detail = (ev.get("detail") or "").lower()
                                    if "own" not in detail:
                                        minute = ev.get("elapsed")
                                        if minute is not None:
                                            try:
                                                goals_minutes.append(int(minute))
                                            except (ValueError, TypeError):
                                                pass

                            # Label all rows for this match
                            label_match_rows(match_id, goals_minutes)

                            # Mark as labeled
                            state.setdefault("labeled_matches", []).append(match_id)
                            logger.info(f"[GSHEETS] Match {match_id} labeled with {len(goals_minutes)} goals")
                        except Exception as e:
                            logger.exception(f"[GSHEETS] Error labeling match {match_id}: {e}")

                    mark_tracked_match_finished(match_id, cur_score[0], cur_score[1])
                    state["monitored_matches"] = [m for m in state.get("monitored_matches", []) if m != match_id]
                    ACTIVE_FIXTURES.discard(int(match_id))
                    state.setdefault("excluded_matches", []).append(match_id)
                    mark_state_dirty()

                update_persistent_fixture_tracking(match_id, fixture, sent_text=msg, force_finished=True)
                updated = True
            
            # GSHEETS: Finalize outcome columns when match ends
            finalize_match_outcomes(match_id, client)
            
            # JSONL: Process outcomes for all signals of this match
            process_match_outcomes_for_jsonl(match_id, client)
            
            # Statistics are now calculated on-demand from match state (no update_daily_stats call)
            
            # Check if all matches for this date are finished and send report if ready
            with state_lock:
                signal_date = state.get("match_signal_info", {}).get(str(match_id), {}).get("signal_date")
            if signal_date:
                check_and_send_daily_stats_if_ready(signal_date)

        if updated:
            with state_lock:
                cur_status = state.get("match_goal_status", {}).get(str(match_id), {})
                state.setdefault("match_goal_status", {})[str(match_id)] = cur_status
                if res:
                    state.setdefault("match_prob_status", {})[str(match_id)] = prob_now
            mark_state_dirty()
        else:
            track_status = status_short if is_finished else "LIVE"
            mark_tracked_match_update(match_id, current_home, current_away, edited=False, status=track_status)

        update_persistent_fixture_tracking(match_id, fixture)

    except Exception:
        logger.exception(f"[MONITOR] Error updating match {match_id}")

# -------------------------
# Message formatting (xG display rule preserved)
# -------------------------
def _is_halftime(minute: int, status_label: str, status_short: str = "", status_elapsed: Optional[int] = None) -> bool:
    """
    Determine if match is at halftime.
    
    Rules:
    - Return True if status is HT/HALF_TIME
    - OR if minute == 45 and second half hasn't started
    """
    if str(status_short or "").upper() == "HT":
        return True

    if status_elapsed is not None:
        try:
            status_elapsed = int(status_elapsed)
        except Exception:
            status_elapsed = None

    # API transitional state: short=1H with elapsed=45 should be treated as halftime.
    if str(status_short or "").upper() == "1H" and (int(minute or 0) == 45 or status_elapsed == 45):
        return True

    if not status_label:
        return minute == 45
    
    s = str(status_label).strip().lower()
    
    # Explicit halftime statuses
    if any(ind in s for ind in ("ht", "half", "перерыв", "halftime", "half-time")):
        return True
    
    # minute == 45 and not in second half
    if minute == 45:
        # If status is 2H (second half), NOT halftime
        if "2h" in s or "second" in s:
            return False
        # Otherwise, treat 45' as halftime
        return True
    
    return False

def _extract_signal_format_fields(collected: Dict[str, Any]) -> Dict[str, Any]:
    fixture = collected.get("fixture", {}) or {}
    home_raw = (fixture.get("team_home_name") or {}).get("value") if isinstance(fixture.get("team_home_name"), dict) else fixture.get("team_home_name")
    away_raw = (fixture.get("team_away_name") or {}).get("value") if isinstance(fixture.get("team_away_name"), dict) else fixture.get("team_away_name")
    home = home_raw or "Home"
    away = away_raw or "Away"
    league_raw = (fixture.get("league_name") or {}).get("value") if isinstance(fixture.get("league_name"), dict) else fixture.get("league_name")
    league = league_raw or ""
    venue = (fixture.get("venue") or {}).get("value") if isinstance(fixture.get("venue"), dict) else fixture.get("venue") or {}
    country_raw = ""
    if isinstance(venue, dict):
        country_raw = venue.get("country") or ""
    if not country_raw:
        league_country = (fixture.get("league_country") or {}).get("value") if isinstance(fixture.get("league_country"), dict) else fixture.get("league_country")
        country_raw = league_country or ""
    country = country_raw or ""
    minute = int((fixture.get("elapsed") or {}).get("value") or 0)
    status_short = ""
    status_elapsed: Optional[int] = None
    status_obj_raw = fixture.get("status")
    status_obj: Dict[str, Any] = {}
    if isinstance(status_obj_raw, dict):
        status_val = status_obj_raw.get("value")
        if isinstance(status_val, dict):
            status_obj = status_val
        else:
            status_obj = status_obj_raw
    if isinstance(status_obj, dict):
        status_short = str(status_obj.get("short") or "").upper()
        try:
            status_elapsed_raw = status_obj.get("elapsed")
            if status_elapsed_raw is not None:
                status_elapsed = int(float(status_elapsed_raw))
        except Exception:
            status_elapsed = None
    if minute <= 0 and status_elapsed is not None:
        minute = int(status_elapsed)
    score_home_now = int((fixture.get("score_home") or {}).get("value") or 0)
    score_away_now = int((fixture.get("score_away") or {}).get("value") or 0)
    minute_label_raw = (fixture.get("elapsed_label") or {}).get("value") or ""
    
    # Get extra/added time from fixture status
    extra_time = 0
    extra_val = fixture.get("extra_time")
    if isinstance(extra_val, dict):
        extra_val = extra_val.get("value")
    if extra_val is not None:
        try:
            extra_time = int(extra_val)
        except Exception:
            extra_time = 0
    
    # Check if match is at halftime
    is_halftime = _is_halftime(minute, minute_label_raw, status_short=status_short, status_elapsed=status_elapsed)

    # xG selection logic (API > fallback)
    api_entry_h = fixture.get("expected_goals_home")
    api_entry_a = fixture.get("expected_goals_away")
    api_provided_h = isinstance(api_entry_h, dict) and ("value" in api_entry_h)
    api_provided_a = isinstance(api_entry_a, dict) and ("value" in api_entry_a)
    api_xg_h = api_entry_h.get("value") if api_provided_h else None
    api_xg_a = api_entry_a.get("value") if api_provided_a else None

    xg_now_h = api_xg_h if api_provided_h else None
    xg_now_a = api_xg_a if api_provided_a else None

    generated_h = False
    generated_a = False

    both_none_or_zero = (xg_now_h in (None, 0)) and (xg_now_a in (None, 0))
    if both_none_or_zero:
        if _has_enough_stats_for_xg(fixture):
            if xg_now_h in (None, 0):
                xg_now_h = estimate_xg_from_metrics_combined(fixture, "home")
                generated_h = True
            if xg_now_a in (None, 0):
                xg_now_a = estimate_xg_from_metrics_combined(fixture, "away")
                generated_a = True
        else:
            xg_now_h = 0.0 if xg_now_h in (None, 0) else xg_now_h
            xg_now_a = 0.0 if xg_now_a in (None, 0) else xg_now_a
    else:
        if xg_now_h in (None, 0):
            xg_now_h = estimate_xg_from_metrics_combined(fixture, "home")
            generated_h = True
        if xg_now_a in (None, 0):
            xg_now_a = estimate_xg_from_metrics_combined(fixture, "away")
            generated_a = True

    try:
        xg_now_h_f = float(xg_now_h)
    except Exception:
        xg_now_h_f = 0.0
    try:
        xg_now_a_f = float(xg_now_a)
    except Exception:
        xg_now_a_f = 0.0

    # Используем одинаковый xG независимо от источника (API или fallback)
    # Новый fallback_compute уже возвращает правдоподобный xG
    display_xg_h = xg_now_h_f
    display_xg_a = xg_now_a_f

    def stat_int(key):
        v = (fixture.get(key) or {}).get("value") if isinstance(fixture.get(key), dict) else fixture.get(key)
        try:
            return int(v)
        except Exception:
            try:
                return int(float(v))
            except Exception:
                return 0

    total_shots_h = stat_int("total_shots_home") or int(get_any_metric(fixture, ["total_shots"], "home") or 0)
    total_shots_a = stat_int("total_shots_away") or int(get_any_metric(fixture, ["total_shots"], "away") or 0)
    shots_on_h = stat_int("shots_on_target_home") or int(get_any_metric(fixture, ["shots_on_target"], "home") or 0)
    shots_on_a = stat_int("shots_on_target_away") or int(get_any_metric(fixture, ["shots_on_target"], "away") or 0)
    saves_h = stat_int("saves_home") or int(get_any_metric(fixture, ["saves"], "home") or 0)
    saves_a = stat_int("saves_away") or int(get_any_metric(fixture, ["saves"], "away") or 0)
    shots_inside_h = stat_int("shots_inside_box_home") or int(get_any_metric(fixture, ["shots_inside_box"], "home") or 0)
    shots_inside_a = stat_int("shots_inside_box_away") or int(get_any_metric(fixture, ["shots_inside_box"], "away") or 0)
    corners_h = stat_int("corner_kicks_home") or int(get_any_metric(fixture, ["corner_kicks","corners"], "home") or 0)
    corners_a = stat_int("corner_kicks_away") or int(get_any_metric(fixture, ["corner_kicks","corners"], "away") or 0)
    possession_h = (fixture.get("ball_possession_home") or {}).get("value") if isinstance(fixture.get("ball_possession_home"), dict) else fixture.get("ball_possession_home") or get_any_metric(fixture, ["ball_possession","passes_%"], "home") or 0
    possession_a = (fixture.get("ball_possession_away") or {}).get("value") if isinstance(fixture.get("ball_possession_away"), dict) else fixture.get("ball_possession_away") or get_any_metric(fixture, ["ball_possession","passes_%"], "away") or 0
    try:
        possession_h_int = int(round(float(possession_h)))
    except Exception:
        possession_h_int = 0
    try:
        possession_a_int = int(round(float(possession_a)))
    except Exception:
        possession_a_int = 0

    try:
        score_home_now = int((fixture.get("score_home") or {}).get("value") or 0)
    except Exception:
        score_home_now = 0
    try:
        score_away_now = int((fixture.get("score_away") or {}).get("value") or 0)
    except Exception:
        score_away_now = 0

    is_finished = False
    if isinstance(minute_label_raw, str) and minute_label_raw.strip().lower() in ("ft", "finished", "full time"):
        is_finished = True
    if minute >= 95:
        is_finished = True

    league_country_line = league
    if country:
        league_country_line = f"{league_country_line}, {country}" if league_country_line else f"{country}"

    return {
        "home": home,
        "away": away,
        "league_country_line": league_country_line,
        "minute": minute,
        "extra_time": extra_time,
        "is_halftime": is_halftime,
        "is_finished": is_finished,
        "score_home_now": score_home_now,
        "score_away_now": score_away_now,
        "display_xg_h": display_xg_h,
        "display_xg_a": display_xg_a,
        "total_shots_h": total_shots_h,
        "total_shots_a": total_shots_a,
        "shots_on_h": shots_on_h,
        "shots_on_a": shots_on_a,
        "saves_h": saves_h,
        "saves_a": saves_a,
        "shots_inside_h": shots_inside_h,
        "shots_inside_a": shots_inside_a,
        "corners_h": corners_h,
        "corners_a": corners_a,
        "possession_h_int": possession_h_int,
        "possession_a_int": possession_a_int,
    }


def _build_signal_snapshot_data(
    collected: Dict[str, Any],
    prob_display: float,
    signal_score: Tuple[int, int],
    signal_minute: int,
    prob_display_90: Optional[float] = None,
    is_admin_approved: bool = False,
) -> Dict[str, Any]:
    fields = _extract_signal_format_fields(collected)
    return {
        "is_admin_approved": bool(is_admin_approved),
        "home": fields["home"],
        "away": fields["away"],
        "league_country_line": fields["league_country_line"],
        "signal_score_home": int(signal_score[0]),
        "signal_score_away": int(signal_score[1]),
        "signal_minute": int(signal_minute or 0),
        "prob_display": float(prob_display or 0.0),
        "prob_display_90": float(prob_display_90) if prob_display_90 is not None else None,
        "display_xg_h": float(fields["display_xg_h"]),
        "display_xg_a": float(fields["display_xg_a"]),
        "total_shots_h": int(fields["total_shots_h"]),
        "total_shots_a": int(fields["total_shots_a"]),
        "shots_on_h": int(fields["shots_on_h"]),
        "shots_on_a": int(fields["shots_on_a"]),
        "saves_h": int(fields["saves_h"]),
        "saves_a": int(fields["saves_a"]),
        "shots_inside_h": int(fields["shots_inside_h"]),
        "shots_inside_a": int(fields["shots_inside_a"]),
        "corners_h": int(fields["corners_h"]),
        "corners_a": int(fields["corners_a"]),
        "possession_h_int": int(fields["possession_h_int"]),
        "possession_a_int": int(fields["possession_a_int"]),
    }


def build_signal_header(snapshot_data: Dict[str, Any]) -> str:
    header_title = "🚨 Сигнал от админа" if snapshot_data.get("is_admin_approved") else "🚨 Сигнал"
    home = snapshot_data.get("home") or "Home"
    away = snapshot_data.get("away") or "Away"
    league_country_line = snapshot_data.get("league_country_line") or ""
    signal_score_home = int(snapshot_data.get("signal_score_home", 0) or 0)
    signal_score_away = int(snapshot_data.get("signal_score_away", 0) or 0)
    signal_minute = int(snapshot_data.get("signal_minute", 0) or 0)
    prob_to75 = float(snapshot_data.get("prob_display", 0.0) or 0.0)
    prob_to90 = snapshot_data.get("prob_display_90")

    lines: List[str] = [
        header_title,
        "",
        f"{home} — {away}",
        "",
        league_country_line,
        "",
        f"Счет: {signal_score_home} - {signal_score_away} {signal_minute} минута",
        "",
        f"Вероятность гола до 75 минуты: {prob_to75:.1f}%",
    ]

    if prob_to90 is not None:
        try:
            lines.append(f"Вероятность гола до 90 минуты: {float(prob_to90):.1f}%")
        except Exception:
            pass

    lines.extend([
        "",
        f"xG до {signal_minute} минуты: {float(snapshot_data.get('display_xg_h', 0.0)):.2f} - {float(snapshot_data.get('display_xg_a', 0.0)):.2f}",
        "",
        f"Удары по воротам: {int(snapshot_data.get('total_shots_h', 0))} - {int(snapshot_data.get('total_shots_a', 0))}",
        f"Удары в створ: {int(snapshot_data.get('shots_on_h', 0))} - {int(snapshot_data.get('shots_on_a', 0))}",
        f"Сейвы: {int(snapshot_data.get('saves_h', 0))} - {int(snapshot_data.get('saves_a', 0))}",
        f"Удары из штрафной: {int(snapshot_data.get('shots_inside_h', 0))} - {int(snapshot_data.get('shots_inside_a', 0))}",
        f"Угловые: {int(snapshot_data.get('corners_h', 0))} - {int(snapshot_data.get('corners_a', 0))}",
        f"Владение: {int(snapshot_data.get('possession_h_int', 0))}% - {int(snapshot_data.get('possession_a_int', 0))}%",
    ])
    return "\n".join(lines)


def _format_current_minute_label(fields: Dict[str, Any]) -> str:
    minute = int(fields.get("minute", 0) or 0)
    extra_time = int(fields.get("extra_time", 0) or 0)
    if minute >= 90 and extra_time > 0:
        return f"90 (+{extra_time}) минута"
    if minute >= 45 and minute < 90 and extra_time > 0:
        return f"45 (+{extra_time}) минута"
    return f"{minute} минута"


def render_live_message(
    current_data: Dict[str, Any],
    signal_score: Tuple[int, int],
    prob_display: float,
    prob_display_90: Optional[float] = None,
    is_admin_approved: bool = False,
) -> str:
    fields = _extract_signal_format_fields(current_data)
    signal_home = int(signal_score[0])
    signal_away = int(signal_score[1])
    minute = int(fields.get("minute", 0) or 0)
    is_halftime = bool(fields.get("is_halftime", False))
    minute_label = _format_current_minute_label(fields)
    minute_label_for_xg = minute_label.replace("минута", "минуты")

    try:
        prob_val = float(prob_display)
    except Exception:
        prob_val = 0.0

    parts: List[str] = []
    title = "🚨 Сигнал от админа" if is_admin_approved else "🚨 Сигнал"
    parts.append(title)
    parts.append("")
    parts.append(f"{fields.get('home', 'Home')} — {fields.get('away', 'Away')}")
    parts.append("")
    parts.append(str(fields.get("league_country_line", "") or ""))
    parts.append("")

    if is_halftime and not bool(fields.get("is_finished")):
        prob90_val = prob_display_90 if prob_display_90 is not None else prob_val
        # Top score line must always remain the score at signal time.
        parts.append(f"Счет: {signal_home} - {signal_away} (Перерыв)")
        parts.append("")
        parts.append(f"Вероятность гола до 75 минуты: {prob_val:.1f}%")
        try:
            parts.append(f"Вероятность гола до 90 минуты: {float(prob90_val):.1f}%")
        except Exception:
            parts.append(f"Вероятность гола до 90 минуты: {prob_val:.1f}%")
        parts.append("")
        parts.append(f"xG до 45 минуты: {float(fields.get('display_xg_h', 0.0)):.2f} - {float(fields.get('display_xg_a', 0.0)):.2f}")
        parts.append("")
        parts.append(f"Удары по воротам: {int(fields.get('total_shots_h', 0))} - {int(fields.get('total_shots_a', 0))}")
        parts.append(f"Удары в створ: {int(fields.get('shots_on_h', 0))} - {int(fields.get('shots_on_a', 0))}")
        parts.append(f"Сейвы: {int(fields.get('saves_h', 0))} - {int(fields.get('saves_a', 0))}")
        parts.append(f"Удары из штрафной: {int(fields.get('shots_inside_h', 0))} - {int(fields.get('shots_inside_a', 0))}")
        parts.append(f"Угловые: {int(fields.get('corners_h', 0))} - {int(fields.get('corners_a', 0))}")
        parts.append(f"Владение: {int(fields.get('possession_h_int', 0))}% - {int(fields.get('possession_a_int', 0))}%")
        return "\n".join(parts)

    parts.append(f"Счет: {signal_home} - {signal_away} {minute_label}")
    parts.append("")

    if not bool(fields.get("is_finished")):
        has_probability = False
        if minute < 75:
            parts.append(f"Вероятность гола до 75 минуты: {prob_val:.1f}%")
            has_probability = True
        if minute < 90 and prob_display_90 is not None:
            try:
                parts.append(f"Вероятность гола до 90 минуты: {float(prob_display_90):.1f}%")
                has_probability = True
            except Exception:
                pass
        if has_probability:
            parts.append("")

    parts.append(f"xG до {minute_label_for_xg}: {float(fields.get('display_xg_h', 0.0)):.2f} - {float(fields.get('display_xg_a', 0.0)):.2f}")
    parts.append("")
    parts.append(f"Удары по воротам: {int(fields.get('total_shots_h', 0))} - {int(fields.get('total_shots_a', 0))}")
    parts.append(f"Удары в створ: {int(fields.get('shots_on_h', 0))} - {int(fields.get('shots_on_a', 0))}")
    parts.append(f"Сейвы: {int(fields.get('saves_h', 0))} - {int(fields.get('saves_a', 0))}")
    parts.append(f"Удары из штрафной: {int(fields.get('shots_inside_h', 0))} - {int(fields.get('shots_inside_a', 0))}")
    parts.append(f"Угловые: {int(fields.get('corners_h', 0))} - {int(fields.get('corners_a', 0))}")
    parts.append(f"Владение: {int(fields.get('possession_h_int', 0))}% - {int(fields.get('possession_a_int', 0))}%")
    return "\n".join(parts)


def render_final_message(match_state: Dict[str, Any], header_text: str) -> str:
    data = match_state.get("data", {}) if isinstance(match_state, dict) else {}
    fields = _extract_signal_format_fields(data)

    signal_home = int(match_state.get("signal_home", 0) or 0)
    signal_away = int(match_state.get("signal_away", 0) or 0)
    final_home = int(match_state.get("final_home", fields.get("score_home_now", 0)) or 0)
    final_away = int(match_state.get("final_away", fields.get("score_away_now", 0)) or 0)

    goal_minutes = match_state.get("goal_minutes") if isinstance(match_state, dict) else None
    if not isinstance(goal_minutes, list):
        goal_minutes = []
    try:
        goal_minutes = sorted({int(m) for m in goal_minutes})
    except Exception:
        goal_minutes = []

    has_goals_after_signal = bool(goal_minutes)
    badge = "✅" if has_goals_after_signal else "❌"
    final_line = f"{badge} Финальный счёт: {final_home} - {final_away}"

    lines: List[str] = [
        str(header_text or "🚨 Сигнал"),
        "",
        f"{fields.get('home', 'Home')} — {fields.get('away', 'Away')}",
        "",
        str(fields.get("league_country_line", "") or ""),
        "",
        f"Счет: {signal_home} - {signal_away} (Завершен)",
        "",
        f"xG: {float(fields.get('display_xg_h', 0.0)):.2f} - {float(fields.get('display_xg_a', 0.0)):.2f}",
        "",
        f"Удары по воротам: {int(fields.get('total_shots_h', 0))} - {int(fields.get('total_shots_a', 0))}",
        f"Удары в створ: {int(fields.get('shots_on_h', 0))} - {int(fields.get('shots_on_a', 0))}",
        f"Сейвы: {int(fields.get('saves_h', 0))} - {int(fields.get('saves_a', 0))}",
        f"Удары из штрафной: {int(fields.get('shots_inside_h', 0))} - {int(fields.get('shots_inside_a', 0))}",
        f"Угловые: {int(fields.get('corners_h', 0))} - {int(fields.get('corners_a', 0))}",
        f"Владение: {int(fields.get('possession_h_int', 0))}% - {int(fields.get('possession_a_int', 0))}%",
    ]

    if goal_minutes:
        goals_str = " ".join([f"{m}′" for m in goal_minutes])
        lines.extend(["", f"⚽️Голы: {goals_str}"])

    lines.extend(["", final_line])
    return "\n".join(lines)


def render_live_main_text(
    current_data: Dict[str, Any],
    signal_score: Tuple[int, int],
    prob_display: float,
    prob_display_90: Optional[float] = None,
    is_admin_approved: bool = False,
) -> str:
    return render_live_message(
        current_data=current_data,
        signal_score=signal_score,
        prob_display=prob_display,
        prob_display_90=prob_display_90,
        is_admin_approved=is_admin_approved,
    )


def build_live_footer(
    current_data: Dict[str, Any],
    scored_goals_after_signal: List[int],
    flash_goal_until_ts: float = 0.0,
    now_ts: Optional[float] = None,
    force_final: bool = False,
) -> str:
    fields = _extract_signal_format_fields(current_data)
    current_home = int(fields.get("score_home_now", 0))
    current_away = int(fields.get("score_away_now", 0))
    goals_sorted = sorted([int(m) for m in (scored_goals_after_signal or [])])
    goals_count = len(goals_sorted)
    now_val = float(now_ts if now_ts is not None else time.time())

    if force_final or fields.get("is_finished"):
        if goals_count > 0:
            return f"✅Финальный счёт: {current_home} - {current_away}"
        return f"❌Финальный счёт: {current_home} - {current_away}"

    if goals_count == 0:
        return ""

    if now_val < float(flash_goal_until_ts or 0.0):
        return f"⚽️ГООООЛ! ({current_home} - {current_away})"

    goals_str = " ".join([f"{m}’" for m in goals_sorted])
    return f"⚽️Голы: {goals_str}\n\n✅Счёт: {current_home} - {current_away}"


def compose_signal_message(header_text: str, live_footer: str) -> str:
    header = (header_text or "").strip()
    footer = (live_footer or "").strip()
    if not header:
        return footer
    if not footer:
        return header
    return f"{header}\n\n{footer}"


def format_signal_report_from_metrics(collected: Dict[str,Any], prob_display: float, initial_score: Tuple[int,int], extra_text: str="", prob_display_90: Optional[float]=None, match_id: Optional[int]=None, is_admin_approved: bool=False) -> str:
    header_text = render_live_message(
        current_data=collected,
        signal_score=initial_score,
        prob_display=prob_display,
        prob_display_90=prob_display_90,
        is_admin_approved=is_admin_approved,
    )
    if extra_text:
        return compose_signal_message(header_text, extra_text)
    return header_text

# -------------------------
# Monitor daemon & main loop (with Timeout handling fix)
# -------------------------
_monitor_thread: Optional[threading.Thread] = None
_monitor_stop = threading.Event()

def monitor_daemon(client: APISportsMetricsClient, interval: int = MONITOR_INTERVAL, max_workers: int = 8):
    logger.info(f"[MONITOR] daemon started, interval={interval}s")
    executor = ThreadPoolExecutor(max_workers=max_workers)
    try:
        while not _monitor_stop.wait(interval):
            monitored = get_tracked_update_candidates()
            if not monitored:
                continue
            futures = {executor.submit(perform_monitor_update, mid, client): mid for mid in monitored}
            fut_list = list(futures.keys())
            try:
                for fut in as_completed(fut_list, timeout=interval):
                    mid = futures.get(fut)
                    try:
                        fut.result()
                    except Exception:
                        logger.exception(f"[MONITOR] exception updating {mid}")
            except TimeoutError:
                logger.debug(f"[MONITOR] as_completed timed out after {interval}s; processing completed futures and continuing.")
                for fut in fut_list:
                    if fut.done():
                        mid = futures.get(fut)
                        try:
                            fut.result()
                        except Exception:
                            logger.exception(f"[MONITOR] exception updating {mid} after timeout")
            
            # Update review tracking (check if any review messages are ready to finalize)
            # DISABLED: No longer monitor/update review messages - send once and never edit
            # try:
            #     update_review_tracking(client)
            # except Exception:
            #     logger.exception("[MONITOR] Error updating review tracking")
    except Exception:
        logger.exception("[MONITOR] daemon unexpected error")
    finally:
        executor.shutdown(wait=False)
        logger.info("[MONITOR] daemon stopped")

def start_monitor_daemon(client: APISportsMetricsClient):
    global _monitor_thread
    if _monitor_thread and _monitor_thread.is_alive():
        return
    _monitor_stop.clear()
    t = threading.Thread(target=monitor_daemon, args=(client, MONITOR_INTERVAL, 8), daemon=True)
    _monitor_thread = t
    t.start()

def stop_monitor_daemon():
    _monitor_stop.set()
    if _monitor_thread:
        _monitor_thread.join(timeout=2)

# -------------------------
# Early strict mode helper (25-30 minute filter)
# -------------------------
def check_early_strict_mode(fixture_metrics: Dict[str, Any], minute: int) -> bool:
    """
    Check if signal should be allowed in MID STRICT MODE (25-30 minute).
    
    Mid strict mode applies ONLY during 25-30 minute window.
    Uses flexible "one allowed failure" rule: AT LEAST 3 out of 4 conditions must be met.
    
    Required conditions (25-30 minute):
    1. pressure_index >= 16.5
    2. total shots on target (both teams) >= 4
    3. total shots inside box (both teams) >= 5
    4. (xg_total >= 1.0) OR (abs(xg_delta) >= 0.35)
    
    Logic:
    - If 4/4 conditions met → signal ALLOWED
    - If 3/4 conditions met → signal ALLOWED (one allowed failure)
    - If <= 2/4 conditions met → signal BLOCKED
    
    Args:
        fixture_metrics: Match fixture metrics
        minute: Current match minute
        
    Returns:
        True if signal is allowed (3+ conditions met), False if blocked (<=2 conditions met)
    """
    # Strict mode applies ONLY in 25-30 minute window
    if minute < 25 or minute > 30:
        # Outside strict mode window - allow signal (no additional filtering)
        return True
    
    # Inside strict mode window (25-30 min) - apply strict filters with flexibility
    
    # Calculate pressure_index
    pressure_index = calculate_pressure_index(fixture_metrics)
    
    # Get shots on target (both teams)
    shots_on_target_home = int(get_any_metric(fixture_metrics, ["shots_on_target"], "home") or 0)
    shots_on_target_away = int(get_any_metric(fixture_metrics, ["shots_on_target"], "away") or 0)
    total_shots_on_target = shots_on_target_home + shots_on_target_away
    
    # Get shots from box (both teams)
    shots_in_box_home = int(get_any_metric(fixture_metrics, ["shots_inside_box"], "home") or 0)
    shots_in_box_away = int(get_any_metric(fixture_metrics, ["shots_inside_box"], "away") or 0)
    total_shots_in_box = shots_in_box_home + shots_in_box_away
    
    # Get xG values and calculate delta and total
    xg_home = float(get_any_metric(fixture_metrics, ["expected_goals"], "home") or 0.0)
    xg_away = float(get_any_metric(fixture_metrics, ["expected_goals"], "away") or 0.0)
    xg_total = xg_home + xg_away
    xg_delta = abs(xg_home - xg_away)
    
    # Check conditions with detailed logging
    pressure_ok = pressure_index >= 16.5
    shots_on_target_ok = total_shots_on_target >= 4
    shots_in_box_ok = total_shots_in_box >= 5
    xg_compound_ok = (xg_total >= 1.0) or (xg_delta >= 0.35)
    
    # Count how many conditions passed
    conditions_passed = sum([pressure_ok, shots_on_target_ok, shots_in_box_ok, xg_compound_ok])
    
    # Log individual condition results with status markers
    status_pressure = "OK" if pressure_ok else "FAIL"
    status_shots_target = "OK" if shots_on_target_ok else "FAIL"
    status_shots_box = "OK" if shots_in_box_ok else "FAIL"
    status_xg_compound = "OK" if xg_compound_ok else "FAIL"
    
    logger.info(f"[MID_STRICT] pressure_index={pressure_index:.2f} (>= 16.5) {status_pressure}")
    logger.info(f"[MID_STRICT] shots_on_target={total_shots_on_target} (>= 4) {status_shots_target}")
    logger.info(f"[MID_STRICT] shots_in_box={total_shots_in_box} (>= 5) {status_shots_box}")
    logger.info(f"[MID_STRICT] (xg_total={xg_total:.2f} >= 1.0) OR (abs(xg_delta)={xg_delta:.2f} >= 0.35) {status_xg_compound}")
    
    # ONE ALLOWED FAILURE: need at least 3/4 conditions to pass
    # This allows match to proceed even if one metric is weak
    if conditions_passed >= 3:
        logger.info(f"[MID_STRICT] Passed {conditions_passed}/4 conditions → SIGNAL ALLOWED")
        return True
    else:
        logger.info(f"[MID_STRICT] Passed {conditions_passed}/4 conditions (need >= 3) → SIGNAL BLOCKED")
        return False

# -------------------------
# CLI helpers & main_loop
# -------------------------
def parse_args():
    p = argparse.ArgumentParser(description="Goal Predictor Bot")
    p.add_argument("--dump-match", type=int, help="Dump full metrics for a fixture_id and exit")
    return p.parse_args()

def dump_match_metrics_cli(fixture_id: int):
    client = APISportsMetricsClient(api_key=API_FOOTBALL_KEY, host=API_FOOTBALL_HOST, cache_ttl=CACHE_TTL)
    data = client.collect_match_all(fixture_id)
    print(json.dumps(data, ensure_ascii=False, indent=2))

def main_loop():
    # Validate Telegram configuration (non-blocking, continues on error)
    if not validate_telegram_target():
        logger.warning("[TG] Telegram validation had warnings. Bot will continue, but messages may fail.")
    else:
        logger.info("[TG] Telegram validation successful.")
    
    # Initialize Google Sheets
    if init_google_sheets():
        logger.info("[GSHEETS] Google Sheets integration enabled")
    else:
        logger.warning("[GSHEETS] Google Sheets integration disabled")
    
    client = APISportsMetricsClient(api_key=API_FOOTBALL_KEY, host=API_FOOTBALL_HOST, cache_ttl=CACHE_TTL)
    start_monitor_daemon(client)
    start_callback_handler_daemon()  # Start callback handler for inline buttons
    start_review_timeout_daemon()  # Start review timeout checker for auto-skip after 10 minutes
    # DISABLED: admin_review_daemon no longer needed - review messages sent once and never edited
    # start_admin_review_daemon(client)  # Start admin review updater for DM messages
    start_daily_stats_checker_daemon()  # Start daily stats checker for 23:59 rule
    start_gsheets_labeler_daemon(client)  # Start Google Sheets labeler daemon for goal_next_15 backfill
    start_daily_cleanup_daemon(client)  # Start maintenance daemon (04:00 MSK smart cleanup)
    refresh_all_cup_leagues_now()  # Queue cup/problem leagues for one-by-one non-blocking refresh
    send_permanent_instruction_message()  # Send permanent instruction message to channel (once)
    logger.info("Starting Goal Predictor Bot main loop...")
    
    # Счетчик циклов для периодической очистки no_stats
    loop_counter = 0
    global _league_stale_scan_date_msk, _league_last_queue_process_ts
    
    try:
        while True:
            try:
                # Периодическая очистка expired no_stats_fixtures (каждые 10 циклов)
                loop_counter += 1
                if loop_counter % 10 == 0:
                    cleanup_expired_no_stats_fixtures()
                    cleanup_expired_no_stats_blocked()
                if loop_counter % 30 == 0:
                    cleanup_persist_matches()

                refreshed_count = weekly_refresh_tick(
                    client,
                    stale_scan_limit=200,
                    process_limit=LEAGUE_REFRESH_MAX_PER_CYCLE,
                )
                if refreshed_count > 0:
                    logger.info("[LEAGUE_QUEUE] processed=%s this cycle", refreshed_count)

                cup_processed = process_cup_league_queue(client, limit=50)
                if cup_processed > 0:
                    logger.info("[LEAGUE_CUP_BATCH] processed_this_cycle=%s", cup_processed)
                
                raw_live = client._get("fixtures", params={"live":"all"}, cache=False) or {}
                fixtures = raw_live.get("response") or []
                logger.info(f"[LOOP] Fetched {len(fixtures)} live fixtures")

                live_active_ids: Set[int] = set()
                for fixture in fixtures:
                    try:
                        fixture_id = get_fixture_id(fixture)
                        if fixture_id is None:
                            continue
                        fixture_id_int = int(fixture_id)
                        if _is_live_fixture_active(fixture):
                            live_active_ids.add(fixture_id_int)
                    except Exception:
                        continue

                with state_lock:
                    ACTIVE_FIXTURES.clear()
                    ACTIVE_FIXTURES.update(live_active_ids)

                for fixture in fixtures:
                    try:
                        fixture_id = get_fixture_id(fixture)
                        if fixture_id is None:
                            block_key = f"unknown:{abs(hash(str(fixture))) % 1000000000}"
                            if not is_no_stats_blocked(block_key):
                                block_no_stats_match_long(block_key, "missing fixture_id")
                            continue
                        fixture_id = int(fixture_id)
                        block_key = str(fixture_id)

                        # ПЕРВАЯ ПРОВЕРКА: длинная блокировка no-stats (быстрый skip)
                        if is_no_stats_blocked(block_key):
                            continue

                        # ПЕРВАЯ ПРОВЕРКА: блокировка no_stats (быстрый skip)
                        if is_no_stats_fixture(fixture_id):
                            continue

                        if is_ignored_match(fixture_id):
                            logger.debug(f"[LOOP] Skipping ignored match {fixture_id}")
                            continue

                        with state_lock:
                            if fixture_id in state.get("monitored_matches", []):
                                continue
                            if fixture_id in state.get("excluded_matches", []):
                                continue

                        minute_i = _fixture_minute_from_raw(fixture)
                        if minute_i < MATCH_MIN_MINUTE:
                            logger.debug(f"[LOOP] Skipping early match {fixture_id} minute={minute_i} (<{MATCH_MIN_MINUTE})")
                            continue

                        # Блокировать новые сигналы после MATCH_MAX_MINUTE, но НЕ для уже отслеживаемых
                        if minute_i > MATCH_MAX_MINUTE:
                            logger.debug(f"[LOOP] Skipping late match {fixture_id} minute={minute_i} (>{MATCH_MAX_MINUTE})")
                            continue

                        data = client.collect_match_all(fixture_id)
                        fixture_metrics = data.get("fixture", {}) or {}

                        # Ensure fixture_id is in metrics for logging
                        if "fixture_id" not in fixture_metrics:
                            fixture_metrics["fixture_id"] = fixture_id

                        # Ensure live statistics are loaded and normalized
                        if not has_stats_data(fixture_metrics):
                            raw_stats = client.fetch_fixture_statistics(fixture_id) or []
                            if raw_stats:
                                data["statistics_raw"] = raw_stats
                                raw_fixture = client.fetch_fixture(fixture_id) or {}
                                stats_norm = client._normalize_statistics(raw_stats, raw_fixture)
                                if stats_norm:
                                    fixture_metrics.update(stats_norm)

                        # Diagnostics: raw keys and stats presence
                        logger.info(f"[RAW_KEYS] fixture_id={fixture_id} keys={list(data.keys())}")
                        has_stats_flag = (
                            ("statistics" in data) or ("stats" in data) or ("statistics_raw" in data)
                            or has_stats_data(fixture_metrics)
                        )
                        logger.info(f"[HAS_STATS] fixture_id={fixture_id} has_statistics={has_stats_flag}")

                        # If still no stats, long-block to avoid log spam
                        if not has_stats_data(fixture_metrics):
                            coverage, _missing = compute_coverage_score(fixture_metrics)
                            if coverage == 0.0:
                                block_no_stats_match_long(block_key, "no statistics in live data")
                                block_no_stats_fixture(fixture_id, minute_i, coverage)
                                continue

                        minute = int((fixture_metrics.get("elapsed") or {}).get("value") or minute_i or 0)
                        # Блокировать новые сигналы после MATCH_MAX_MINUTE
                        if minute > MATCH_MAX_MINUTE:
                            logger.debug(f"[LOOP] Skipping late match {fixture_id} minute={minute} (>{MATCH_MAX_MINUTE})")
                            continue

                        res = compute_lambda_and_probability(fixture_metrics, minute)
                        prob_actual = res.get("prob_goal_either_to75", 0.0)
                        
                        # Determine evaluation mode
                        eval_mode = "MID_STRICT" if 25 <= minute <= 30 else "NORMAL"
                        logger.info(f"[EVAL] mode={eval_mode} minute={minute}")
                        logger.info(f"[EVAL] match={fixture_id} minute={minute} prob={prob_actual}%")
                        
                        # REVIEW MODE: Check if probability is in review range (50-79.999%)
                        if REVIEW_MIN_THRESHOLD <= prob_actual < PROB_SEND_THRESHOLD:
                            logger.info(f"[REVIEW] match={fixture_id} prob={prob_actual:.1f}% -> REVIEW mode (queuing for admin)")
                            # Early strict mode filter still applies to REVIEW
                            if not check_early_strict_mode(fixture_metrics, minute):
                                logger.info(f"[REVIEW] match={fixture_id} BLOCKED by early strict mode at minute {minute}")
                                continue
                            # Send to admin for review
                            send_review_to_admin(fixture_id, data, prob_actual, minute)
                            continue  # Don't auto-send, wait for admin decision
                        
                        if should_send_signal(prob_actual, PROB_SEND_THRESHOLD, fixture_metrics):
                            # Early strict mode filter (15-23 minute)
                            # Check strict conditions before proceeding with signal
                            if not check_early_strict_mode(fixture_metrics, minute):
                                logger.info(f"[EVAL] match={fixture_id} BLOCKED by early strict mode at minute {minute}")
                                continue

                            if not validate_match_context_before_send(data):
                                logger.info(f"[EVAL] match={fixture_id} BLOCKED by pre-send validation at minute {minute}")
                                continue
                            
                            initial_score = (int((fixture_metrics.get("score_home") or {}).get("value") or 0),
                                             int((fixture_metrics.get("score_away") or {}).get("value") or 0))
                            
                            prob_actual_90 = res.get("prob_goal_either_to90", 0.0)
                            snapshot_data = _build_signal_snapshot_data(
                                collected=data,
                                prob_display=prob_actual,
                                signal_score=initial_score,
                                signal_minute=minute,
                                prob_display_90=prob_actual_90,
                                is_admin_approved=False,
                            )
                            header_text = build_signal_header(snapshot_data)
                            msg = render_live_message(
                                current_data=data,
                                signal_score=initial_score,
                                prob_display=prob_actual,
                                prob_display_90=prob_actual_90,
                                is_admin_approved=False,
                            )
                            mid_msg = send_to_telegram(
                                msg,
                                match_id=fixture_id,
                                score_at_signal=initial_score,
                                signal_minute=int(minute),
                            )
                            if mid_msg:
                                save_signal_snapshot_state(
                                    match_id=fixture_id,
                                    header_text=header_text,
                                    signal_score_home=initial_score[0],
                                    signal_score_away=initial_score[1],
                                    signal_minute=minute,
                                    message_id=int(mid_msg),
                                    chat_id=int(TELEGRAM_CHAT_ID),
                                    prob_to75=prob_actual,
                                    prob_to90=prob_actual_90,
                                )
                                # Calculate initial PressureIndex
                                fixture_data = data.get("fixture", {})
                                initial_pressure = calculate_pressure_index(fixture_data)
                                initial_momentum = 0.0  # First signal has 0 momentum
                                
                                # Extract probability values
                                prob_next_15 = res.get("prob_next_15", prob_actual)
                                prob_until_end = res.get("prob_until_end", prob_actual)
                                league_factor_used = res.get("league_factor", 1.0)
                                _score01_final, match_score_value, _score_metrics = compute_match_score_v2(fixture_metrics, league_factor_used)
                                
                                # Create match snapshot for JSONL logging
                                fixture = fixture_metrics
                                league_id = fixture.get("league_id", {}).get("value") if isinstance(fixture.get("league_id"), dict) else fixture.get("league_id")
                                league_name = fixture.get("league_name", {}).get("value") if isinstance(fixture.get("league_name"), dict) else fixture.get("league_name")
                                season = fixture.get("league_season", {}).get("value") if isinstance(fixture.get("league_season"), dict) else fixture.get("league_season")
                                home_team_id = fixture.get("team_home_id", {}).get("value") if isinstance(fixture.get("team_home_id"), dict) else fixture.get("team_home_id")
                                home_team_name = fixture.get("team_home_name", {}).get("value") if isinstance(fixture.get("team_home_name"), dict) else fixture.get("team_home_name")
                                away_team_id = fixture.get("team_away_id", {}).get("value") if isinstance(fixture.get("team_away_id"), dict) else fixture.get("team_away_id")
                                away_team_name = fixture.get("team_away_name", {}).get("value") if isinstance(fixture.get("team_away_name"), dict) else fixture.get("team_away_name")
                                
                                status_obj = fixture.get("status", {}).get("value") if isinstance(fixture.get("status"), dict) else fixture.get("status") or {}
                                if isinstance(status_obj, dict):
                                    status_short = str(status_obj.get("short") or "").upper()
                                else:
                                    status_short = ""
                                
                                xg_home = float(get_any_metric(fixture, ["expected_goals"], "home") or 0.0)
                                xg_away = float(get_any_metric(fixture, ["expected_goals"], "away") or 0.0)
                                shots_home = int(get_any_metric(fixture, ["total_shots"], "home") or 0)
                                shots_away = int(get_any_metric(fixture, ["total_shots"], "away") or 0)
                                shots_on_target_home = int(get_any_metric(fixture, ["shots_on_target"], "home") or 0)
                                shots_on_target_away = int(get_any_metric(fixture, ["shots_on_target"], "away") or 0)
                                saves_home = int(get_any_metric(fixture, ["saves"], "home") or 0)
                                saves_away = int(get_any_metric(fixture, ["saves"], "away") or 0)
                                shots_in_box_home = int(get_any_metric(fixture, ["shots_inside_box"], "home") or 0)
                                shots_in_box_away = int(get_any_metric(fixture, ["shots_inside_box"], "away") or 0)
                                corners_home = int(get_any_metric(fixture, ["corner_kicks"], "home") or 0)
                                corners_away = int(get_any_metric(fixture, ["corner_kicks"], "away") or 0)
                                possession_home = float(get_any_metric(fixture, ["ball_possession"], "home") or 0.0)
                                possession_away = float(get_any_metric(fixture, ["ball_possession"], "away") or 0.0)
                                
                                pressure_index = calculate_pressure_index(fixture)
                                xg_delta = xg_home - xg_away
                                shots_ratio = (shots_on_target_home + 1.0) / (shots_on_target_away + 1.0) if shots_on_target_away + 1.0 > 0 else 1.0
                                
                                total_saves = saves_home + saves_away
                                total_goals = initial_score[0] + initial_score[1]
                                total_sot = shots_on_target_home + shots_on_target_away
                                save_stress = calculate_save_stress(
                                    saves_home=saves_home,
                                    saves_away=saves_away,
                                    current_home_score=initial_score[0],
                                    current_away_score=initial_score[1],
                                    shots_on_target_home=shots_on_target_home,
                                    shots_on_target_away=shots_on_target_away
                                )
                                
                                possession_diff = abs(possession_home - possession_away)
                                if possession_diff > 1.0:
                                    possession_pressure = possession_diff / 100.0
                                else:
                                    possession_pressure = possession_diff
                                
                                league_factor = float(res.get("league_factor", 1.0) or 1.0)
                                team_mix_factor = float(res.get("team_mix_factor", 1.0) or 1.0)
                                combined_M = float(res.get("combined_m", 1.0) or 1.0)
                                
                                snapshot = {
                                    "match_id": fixture_id,
                                    "league_id": league_id,
                                    "league_name": league_name,
                                    "season": season,
                                    "home_team_id": home_team_id,
                                    "home_team_name": home_team_name,
                                    "away_team_id": away_team_id,
                                    "away_team_name": away_team_name,
                                    "minute": minute,
                                    "status_short": status_short,
                                    "signal_ts_utc": datetime.utcnow().isoformat(),
                                    "score_home": initial_score[0],
                                    "score_away": initial_score[1],
                                    "xg_home": xg_home,
                                    "xg_away": xg_away,
                                    "shots_home": shots_home,
                                    "shots_away": shots_away,
                                    "shots_on_target_home": shots_on_target_home,
                                    "shots_on_target_away": shots_on_target_away,
                                    "saves_home": saves_home,
                                    "saves_away": saves_away,
                                    "shots_in_box_home": shots_in_box_home,
                                    "shots_in_box_away": shots_in_box_away,
                                    "corners_home": corners_home,
                                    "corners_away": corners_away,
                                    "possession_home": possession_home,
                                    "possession_away": possession_away,
                                    "pressure_index": pressure_index,
                                    "xg_delta": xg_delta,
                                    "shots_ratio": shots_ratio,
                                    "save_stress": save_stress,
                                    "possession_pressure": possession_pressure,
                                    "prob_goal_75": prob_actual,
                                    "prob_goal_90": prob_actual_90,
                                    "league_factor": league_factor,
                                    "team_mix_factor": team_mix_factor,
                                    "combined_M": combined_M,
                                    "match_score": match_score_value,
                                    "signal_source": "bot",
                                    "schema_version": "1.0",
                                    "prob_model_version": "v2",
                                    "match_score_version": "v2.5"
                                }
                                
                                save_snapshot(snapshot)
                                
                                # Save match data to Google Sheets (initial signal)
                                save_match_to_sheet(
                                    fixture_id,
                                    data,
                                    minute,
                                    prob_next_15,
                                    prob_until_end,
                                    signal_sent=True,
                                    pressure_index=initial_pressure,
                                    momentum=initial_momentum,
                                    signal_source="bot",
                                    league_factor=league_factor_used,
                                    match_score=match_score_value,
                                )
                                
                                with state_lock:
                                    state.setdefault("match_initial_score", {})[str(fixture_id)] = initial_score
                                    state.setdefault("match_initial_minute", {})[str(fixture_id)] = minute
                                    state.setdefault("match_goal_status", {})[str(fixture_id)] = {
                                        "valid_goals_after_send": False,
                                        "last_goal_time": None,
                                        "persistent_goal_line": None,
                                        "final_line": None,
                                        "cancellations": [],
                                        "score": initial_score,
                                        "processed_ids": [],
                                        "finalized": False,
                                        "last_reported_minute": minute,
                                        "last_pressure_index": initial_pressure
                                    }
                                    state.setdefault("match_processed_event_ids", {})[str(fixture_id)] = []
                                    state.setdefault("match_sent_at", {})[str(fixture_id)] = time.time()
                                    state.setdefault("match_prob_status", {})[str(fixture_id)] = prob_actual
                                    # Save signal date (Moscow time) and set is_finished flag
                                    # Date is determined ONLY by signal send time, regardless of match start time
                                    signal_date = get_msk_date()
                                    msk_time = get_msk_datetime().strftime("%H:%M:%S")
                                    state.setdefault("match_signal_info", {})[str(fixture_id)] = {
                                        "signal_date": signal_date,
                                        "is_finished": False
                                    }
                                    logger.info(f"[STATS] Match {fixture_id}: signal sent at {msk_time} MSK, assigned to date {signal_date}")
                                    if fixture_id not in state.setdefault("monitored_matches", []):
                                        state["monitored_matches"].append(fixture_id)
                                mark_state_dirty()
                                update_persistent_fixture_tracking(fixture_id, fixture_metrics, sent_text=msg)
                    except Exception:
                        logger.exception("Error processing fixture inner")
                time.sleep(CHECK_INTERVAL)
            except Exception:
                logger.exception("Main loop iteration failed")
                time.sleep(CHECK_INTERVAL)
    finally:
        stop_monitor_daemon()

# -------------------------
# Entrypoint
# -------------------------
if __name__ == "__main__":
    # Setup logging first
    logger = setup_logging()
    logger.info("Logging initialized")
    
    args = parse_args()
    if args.dump_match:
        dump_match_metrics_cli(args.dump_match)
        raise SystemExit(0)

    missing = []
    if not TELEGRAM_TOKEN:
        missing.append("TELEGRAM_TOKEN")
    # Support both numeric chat_id (int) and string username (deprecated)
    if TELEGRAM_CHAT_ID is None or (isinstance(TELEGRAM_CHAT_ID, str) and not TELEGRAM_CHAT_ID.strip()):
        missing.append("TELEGRAM_CHAT_ID")
    if not API_FOOTBALL_KEY:
        logger.warning("API_FOOTBALL_KEY not set; live xG/stat fetches may fail.")
    if missing:
        logger.error("Missing required configuration: %s", ", ".join(missing))
        raise SystemExit(1)
    
    # Log configuration
    logger.info(f"Configuration: TELEGRAM_CHAT_ID={TELEGRAM_CHAT_ID} (type: {type(TELEGRAM_CHAT_ID).__name__})")

    logger.info("Starting Goal Predictor Bot (API-Football powered)...")
    try:
        main_loop()
    except KeyboardInterrupt:
        logger.info("Interrupted by user, exiting.")
    except Exception:
        logger.exception("Top-level exception")