import argparse
import json
import random
import sys


def main():
    parser = argparse.ArgumentParser(description="Simulate friction.")
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
    random.seed(str(candidate_id) + "_friction")

    # Simulate friction coefficient between 0.01 and 0.5
    friction = random.uniform(0.01, 0.5)

    result = {
        "candidate_id": candidate_id,
        "friction_coefficient": friction,
        "status": "success"
    }

    with open(args.output, "w") as f:
        json.dump(result, f, indent=2)

if __name__ == "__main__":
    main()
