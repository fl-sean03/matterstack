import json
import sys
import logging
import os
from matterstack.core.external import ExternalTaskWrapper

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("handoff")

def main():
    """
    Bridges the Simulation Task and the External Robot Task.
    1. Reads sim_results.json
    2. Uses ExternalTaskWrapper to send request and wait for response.
    """
    logger.info("Starting Handoff Process...")
    
    # 1. Read Simulation Results
    try:
        with open("sim_results.json", 'r') as f:
            sim_data = json.load(f)
        logger.info(f"Loaded simulation data for candidate: {sim_data.get('candidate_id')}")
    except FileNotFoundError:
        logger.error("sim_results.json not found!")
        sys.exit(1)
        
    # 2. Configure ExternalTaskWrapper
    # We pass the sim data as the request body
    config = {
        "request_path": "experiment_request.json",
        "response_path": "experiment_results.json",
        "request_data": sim_data,
        "poll_interval": 2.0,
        "timeout_minutes": 5
    }
    
    # 3. Execute Wrapper
    wrapper = ExternalTaskWrapper(config)
    wrapper.run()
    
    logger.info("Handoff complete. Robot finished.")

if __name__ == "__main__":
    main()