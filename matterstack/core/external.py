from __future__ import annotations
import sys
import time
import json
import logging
import argparse
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, Any, Union

from pydantic import Field
from .workflow import Task

logger = logging.getLogger(__name__)

class ExternalTask(Task):
    """
    A specialized Task that coordinates with an external process via file system.
    
    Instead of running a direct command, this task runs a python wrapper that:
    1. Writes a request file (JSON) to `request_path`.
    2. Polls for a response file (JSON) at `response_path`.
    3. Exits with success/failure based on the response content.
    
    The external agent (robot, human, service) is responsible for watching
    `request_path`, performing the work, and writing to `response_path`.
    """
    request_path: str = "request.json"
    response_path: str = "response.json"
    request_data: Dict[str, Any] = Field(default_factory=dict)
    poll_interval: float = 5.0
    
    def model_post_init(self, __context: Any) -> None:
        # We override the command to run our internal poller wrapper
        # The wrapper code needs to be available in the environment.
        # We assume 'matterstack' is installed or PYTHONPATH is set.
        
        # We serialize the configuration for the wrapper
        config = {
            "request_path": self.request_path,
            "response_path": self.response_path,
            "request_data": self.request_data,
            "poll_interval": self.poll_interval,
            "timeout_minutes": self.time_limit_minutes
        }
        
        # We pass the config as a JSON string argument
        config_json = json.dumps(config)
        
        # Construct the command
        # We use 'python3 -m matterstack.core.external' as the entry point
        cmd = f"python3 -m matterstack.core.external '{config_json}'"
        
        self.command = cmd
        
        # We ensure the image has python installed.
        # If the user provided an image, we trust it.
        # If not, the backend default applies (which usually has python).
        super().model_post_init(__context)


class ExternalTaskWrapper:
    """
    Helper class executed INSIDE the compute job to coordinate the external task.
    """
    def __init__(self, config: Dict[str, Any]):
        self.request_path = Path(config["request_path"])
        self.response_path = Path(config["response_path"])
        self.request_data = config.get("request_data", {})
        self.poll_interval = config.get("poll_interval", 5.0)
        self.timeout_minutes = config.get("timeout_minutes", 60)
        
    def run(self):
        logging.basicConfig(level=logging.INFO)
        logger.info(f"Starting External Task Wrapper")
        logger.info(f"Request Path: {self.request_path.absolute()}")
        logger.info(f"Response Path: {self.response_path.absolute()}")
        
        # 1. Write Request
        logger.info("Writing request file...")
        with open(self.request_path, "w") as f:
            json.dump(self.request_data, f, indent=2)
            
        # 2. Poll for Response
        start_time = time.time()
        timeout_seconds = self.timeout_minutes * 60
        
        logger.info("Waiting for response...")
        while True:
            if self.response_path.exists():
                logger.info("Response file found!")
                try:
                    self._handle_response()
                    return # Success
                except Exception as e:
                    logger.error(f"Error handling response: {e}")
                    sys.exit(1)
            
            if time.time() - start_time > timeout_seconds:
                logger.error("Timed out waiting for response file.")
                sys.exit(1)
                
            time.sleep(self.poll_interval)
            
    def _handle_response(self):
        """Read and validate response."""
        content = self.response_path.read_text()
        try:
            data = json.loads(content)
        except json.JSONDecodeError:
            raise ValueError("Response file contains invalid JSON")
            
        logger.info(f"Response content: {data}")
        
        # Check for success signal in response
        # We assume standard fields: "status" ("success", "failed")
        status = data.get("status", "success").lower()
        
        if status == "failed":
            reason = data.get("reason", "Unknown error")
            raise RuntimeError(f"External task reported failure: {reason}")
            
        logger.info("External task completed successfully.")


def main():
    """Entry point for the wrapper script."""
    if len(sys.argv) < 2:
        print("Usage: python -m matterstack.core.external <config_json>")
        sys.exit(1)
        
    config_json = sys.argv[1]
    try:
        config = json.loads(config_json)
    except json.JSONDecodeError as e:
        print(f"Invalid config JSON: {e}")
        sys.exit(1)
        
    wrapper = ExternalTaskWrapper(config)
    wrapper.run()

if __name__ == "__main__":
    main()