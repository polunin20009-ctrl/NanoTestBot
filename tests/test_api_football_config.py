from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import NanoTest as bot


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_api_football_key_is_not_hardcoded_in_nanotest_source() -> None:
    source = (REPO_ROOT / "NanoTest.py").read_text(encoding="utf-8")
    assert 'API_FOOTBALL_KEY = "' not in source
    assert "API_FOOTBALL_KEY = _parse_env_str" in source


def test_api_football_client_uses_configured_key_in_headers() -> None:
    client = bot.APISportsMetricsClient(
        api_key="configured-test-key",
        host=bot.API_FOOTBALL_HOST,
    )
    assert client.api_key == "configured-test-key"
    assert client.headers.get("x-apisports-key") == "configured-test-key"


def test_api_football_key_loaded_from_environment_on_fresh_import() -> None:
    env = os.environ.copy()
    env["API_FOOTBALL_KEY"] = "fresh-import-env-key"
    env["PYTEST_CURRENT_TEST"] = "tests/test_api_football_config.py::isolated"
    script = (
        "import NanoTest as nanotest; "
        "assert nanotest.API_FOOTBALL_KEY == 'fresh-import-env-key', "
        "repr(nanotest.API_FOOTBALL_KEY)"
    )
    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=str(REPO_ROOT),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr or completed.stdout
