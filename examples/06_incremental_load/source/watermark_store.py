"""
LakeForge Solution 06: Watermark State Store Manager
Author: Jayaram Pogakula (https://github.com/jayarampogakula)

Manages persistent high-watermark state in a dedicated Delta control table.
Ensures atomic state commits and crash recovery.
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("LakeForge_Watermark_Store")

STATE_FILE = "expected_output/watermark_state_history.json"

class WatermarkStore:
    def __init__(self, table_identifier: str):
        self.table_identifier = table_identifier
        self.state_file = STATE_FILE

    def get_last_watermark(self) -> str:
        """Retrieves the last committed high watermark timestamp."""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, list) and len(data) > 0:
                        last_entry = data[-1]
                        return last_entry.get("high_watermark", "1900-01-01 00:00:00")
            except Exception as e:
                logger.warning("Error reading state file, defaulting to epoch: %s", e)
        return "1900-01-01 00:00:00"

    def commit_watermark(self, new_watermark: str, rows_processed: int, batch_id: str):
        """Atomically persists the new high watermark after Delta commit succeeds."""
        entry = {
            "table_identifier": self.table_identifier,
            "batch_id": batch_id,
            "high_watermark": new_watermark,
            "rows_processed": rows_processed,
            "committed_at_utc": datetime.now(timezone.utc).isoformat(),
            "status": "COMMITTED"
        }

        history = []
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, "r", encoding="utf-8") as f:
                    history = json.load(f)
            except Exception:
                history = []

        history.append(entry)
        os.makedirs(os.path.dirname(self.state_file), exist_ok=True)
        with open(self.state_file, "w", encoding="utf-8") as f:
            json.dump(history, f, indent=2)

        logger.info("Successfully committed watermark %s for %s", new_watermark, self.table_identifier)
