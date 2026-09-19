from __future__ import annotations

import os


# Test imports must never start the bot state-saver or run persistent-state
# migrations against the live project directory.
os.environ["GOALBOT_LIBRARY_MODE"] = "1"
