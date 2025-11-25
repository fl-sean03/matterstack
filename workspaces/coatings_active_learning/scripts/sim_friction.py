import sys
import json
import random

def main():
    if len(sys.argv) < 2:
        print("Usage: python sim_friction.py <input_json_file>")
        sys.exit(1)

    input_file = sys.argv[1]
    
    try:
        with open(input_file, 'r') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"Error: File {input_file} not found")
        sys.exit(1)
        
    candidate_id = data.get('candidate_id', 'unknown')
    # Use candidate_id to seed random for deterministic "simulation"
    random.seed(str(candidate_id) + "_friction")
    
    # Simulate friction coefficient between 0.01 and 0.5
    friction = random.uniform(0.01, 0.5)
    
    result = {
        "candidate_id": candidate_id,
        "friction_coefficient": friction,
        "status": "success"
    }
    
    # Write to standard output or a specific file if needed.
    # For ExternalTask, we might write to a response file, but here we just return JSON to stdout
    # or write to a known output file.
    # The Task definition will likely capture stdout or expect a specific file.
    # Let's assume the wrapper expects a response file "response.json" in the current directory.
    
    with open("response.json", "w") as f:
        json.dump(result, f, indent=2)

if __name__ == "__main__":
    main()