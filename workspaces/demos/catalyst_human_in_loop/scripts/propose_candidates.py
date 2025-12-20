import csv
import sys
import random
import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="candidates.csv", help="Output path for candidates CSV")
    args = parser.parse_args()

    print("Reading intent.md...")
    # Mock reading intent
    
    candidates = [
        {"id": "cat_001", "composition": "Pt(111)"},
        {"id": "cat_002", "composition": "Pd(100)"},
        {"id": "cat_003", "composition": "Au(111)"},
        {"id": "cat_004", "composition": "Ag(111)"},
        {"id": "cat_005", "composition": "Cu(100)"}
    ]
    
    with open(args.output, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "composition"])
        writer.writeheader()
        writer.writerows(candidates)
        
    print(f"Proposed {len(candidates)} candidates to {args.output}")

if __name__ == "__main__":
    main()