"""Shadow-only live-market benchmarking primitives.

The package has no dependency on the Telegram publication path and never
changes a production probability or signal decision.
"""

from .normalization import (
    DEFAULT_LIVE_GOAL_MARKETS,
    PROVIDER,
    REJECTION_RECORD_TYPE,
    SCHEMA_VERSION,
    SNAPSHOT_RECORD_TYPE,
    SOURCE_ENDPOINT,
    LiveGoalMarketSpec,
    NormalizationResult,
    live_goal_market_preference_key,
    normalize_live_goal_markets,
    remove_vig_proportional,
)
from .storage import (
    AppendOnlyMarketJournal,
    iter_market_records,
    market_journal_paths,
)

__all__ = [
    "AppendOnlyMarketJournal",
    "DEFAULT_LIVE_GOAL_MARKETS",
    "LiveGoalMarketSpec",
    "NormalizationResult",
    "PROVIDER",
    "REJECTION_RECORD_TYPE",
    "SCHEMA_VERSION",
    "SNAPSHOT_RECORD_TYPE",
    "SOURCE_ENDPOINT",
    "iter_market_records",
    "live_goal_market_preference_key",
    "market_journal_paths",
    "normalize_live_goal_markets",
    "remove_vig_proportional",
]
