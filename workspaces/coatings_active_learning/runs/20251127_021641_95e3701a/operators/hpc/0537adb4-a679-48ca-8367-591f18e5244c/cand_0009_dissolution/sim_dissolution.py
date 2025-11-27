import argparse
import json
import random
import sys

def main():
    parser = argparse.ArgumentParser(description="Simulate dissolution rate.")
    parser.add_argument("input_file", help="Path to input JSON file")
    parser.add_argument("--output", "-o", default="response.json", help="Path to output JSON file")
    args = parser.parse_args()

    try:
        with open(args.input_file, 'r') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"Error: File {args.input_file} not found")
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
    
    with open(args.output, "w") as f:
        json.dump(result, f, indent=2)

if __name__ == "__main__":
    main()