import time
import json
import os
import random
import sys
from pathlib import Path

def run_robot_daemon(watch_dir: str):
    """
    Watches for experiment_request.json in the specified directory (and subdirectories).
    If found, processes it and writes experiment_results.json.
    """
    print(f"Robot Daemon started. Watching {watch_dir}...")
    
    # We'll run for a fixed duration or until interrupted
    # In a real scenario, this would be a persistent service.
    # For this demo, we'll loop until we find a request, process it, and maybe stay alive if needed.
    # To keep it simple for the demo, we will scan recursively.
    
    # We will look for 'experiment_request.json' that DOES NOT have a corresponding 'experiment_results.json'
    
    max_duration = 300 # 5 minutes max
    start_time = time.time()
    
    while time.time() - start_time < max_duration:
        found_work = False
        
        # Walk the directory to find requests
        for root, dirs, files in os.walk(watch_dir):
            if "experiment_request.json" in files:
                request_path = Path(root) / "experiment_request.json"
                result_path = Path(root) / "experiment_results.json"
                
                if not result_path.exists():
                    # Found a pending request
                    print(f"Processing request at {request_path}")
                    found_work = True
                    
                    try:
                        with open(request_path, 'r') as f:
                            req_data = json.load(f)
                            
                        # Simulate processing time
                        time.sleep(2)
                        
                        # Calculate results with sim-to-real gap
                        # req_data usually has sim results we can perturb
                        
                        conductivity = req_data.get("conductivity_sim", 50.0)
                        stability = req_data.get("stability_sim", 0.5)
                        
                        # Add noise/drift
                        real_conductivity = conductivity * random.uniform(0.8, 1.1)
                        real_stability = stability * random.uniform(0.7, 0.95) # Real world is harsher
                        
                        result_data = {
                            "conductivity_exp": real_conductivity,
                            "stability_exp": real_stability,
                            "status": "success",
                            "robot_id": "Bot-42"
                        }
                        
                        with open(result_path, 'w') as f:
                            json.dump(result_data, f, indent=2)
                            
                        print(f"Completed request at {result_path}")
                        
                    except Exception as e:
                        print(f"Error processing {request_path}: {e}")
                        # Write failure
                        with open(result_path, 'w') as f:
                            json.dump({"status": "failed", "reason": str(e)}, f)
                            
        time.sleep(1)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        watch_dir = sys.argv[1]
    else:
        watch_dir = "."
        
    run_robot_daemon(watch_dir)