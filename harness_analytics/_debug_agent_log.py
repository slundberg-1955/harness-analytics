"""Folded debug NDJSON logger (session d261ff). Do not log secrets."""

from __future__ import annotations

import json
import sys
import time

_LOG_PATH = "/Users/stevelundberg/.cursor/debug-logs/debug-d261ff.log"
_SESSION_ID = "d261ff"


def agent_log(location: str, message: str, *, data: dict | None = None, hypothesis_id: str = "") -> None:
    # #region agent log
    try:
        line = json.dumps(
            {
                "sessionId": _SESSION_ID,
                "location": location,
                "message": message,
                "data": data or {},
                "hypothesisId": hypothesis_id,
                "timestamp": int(time.time() * 1000),
            },
            ensure_ascii=False,
        )
        with open(_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except OSError:
        pass
    try:
        print("AGENT_DEBUG " + line, file=sys.stderr, flush=True)
    except OSError:
        pass
    # #endregion
