from __future__ import annotations

import json
import logging
import sys
import time
from pathlib import Path
from typing import Any, Dict

from pydantic import Field

from .workflow import Task

logger = logging.getLogger(__name__)


class GateTask(Task):
    """
    A Human-in-the-loop task that waits for manual approval or rejection.

    Behavior:
    1. Writes 'gate_info.json' with instructions/context.
    2. Waits for 'approved.txt' (success) or 'rejected.txt' (failure).
    """

    command: str = Field(default="")  # Override Task.command to be optional, calculated in post_init
    message: str = "Please approve or reject this step."
    approve_file: str = "approved.txt"
    reject_file: str = "rejected.txt"
    info_file: str = "gate_info.json"
    poll_interval: float = 2.0

    def model_post_init(self, __context: Any) -> None:
        # Configuration for the wrapper
        config = {
            "message": self.message,
            "approve_file": self.approve_file,
            "reject_file": self.reject_file,
            "info_file": self.info_file,
            "poll_interval": self.poll_interval,
            "timeout_minutes": self.time_limit_minutes if self.time_limit_minutes is not None else 60,
        }

        config_json = json.dumps(config)

        # We use 'python3 -m matterstack.core.gate' as the entry point
        cmd = f"python3 -m matterstack.core.gate '{config_json}'"
        self.command = cmd
        super().model_post_init(__context)


class GateTaskWrapper:
    """
    Helper class executed INSIDE the compute job to coordinate the gate.
    """

    def __init__(self, config: Dict[str, Any]):
        self.message = config.get("message", "")
        self.approve_path = Path(config["approve_file"])
        self.reject_path = Path(config["reject_file"])
        self.info_path = Path(config["info_file"])
        self.poll_interval = config.get("poll_interval", 2.0)
        timeout = config.get("timeout_minutes")
        self.timeout_minutes = timeout if timeout is not None else 60

    def run(self):
        logging.basicConfig(level=logging.INFO)
        logger.info(f"Starting Gate Task: {self.message}")

        # 1. Write Info
        info_data = {
            "message": self.message,
            "instructions": f"Create '{self.approve_path.name}' to approve, or '{self.reject_path.name}' to reject.",
        }
        with open(self.info_path, "w") as f:
            json.dump(info_data, f, indent=2)

        logger.info(f"Waiting for approval ({self.approve_path}) or rejection ({self.reject_path})...")

        # 2. Poll
        start_time = time.time()
        timeout_seconds = self.timeout_minutes * 60

        while True:
            if self.approve_path.exists():
                logger.info("Approval file found. Gate passed.")
                return  # Success

            if self.reject_path.exists():
                logger.error("Rejection file found. Gate failed.")
                sys.exit(1)  # Failure

            if time.time() - start_time > timeout_seconds:
                logger.error("Timed out waiting for gate decision.")
                sys.exit(1)

            time.sleep(self.poll_interval)


def main():
    """Entry point for the wrapper script."""
    if len(sys.argv) < 2:
        print("Usage: python -m matterstack.core.gate <config_json>")
        sys.exit(1)

    config_json = sys.argv[1]
    try:
        config = json.loads(config_json)
    except json.JSONDecodeError as e:
        print(f"Invalid config JSON: {e}")
        sys.exit(1)

    wrapper = GateTaskWrapper(config)
    wrapper.run()


if __name__ == "__main__":
    main()
