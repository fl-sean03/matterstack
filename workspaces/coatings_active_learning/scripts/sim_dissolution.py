import sys
import json
import random

def main():
    if len(sys.argv) < 2:
        print("Usage: python sim_dissolution.py <input_json_file>")
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
    random.seed(str(candidate_id) + "_dissolution")
    
    # Simulate dissolution rate (nm/h) between 0.1 and 10.0
    dissolution = random.uniform(0.1, 10.0)
    
    result = {
        "candidate_id": candidate_id,
        "dissolution_rate": dissolution,
        "status": "success"
    }
    
    # Write to response.json as expected by our convention for this mock
    with open("response.json", "w") as f:
        json.dump(result, f, indent=2)

if __name__ == "__main__":
    main()