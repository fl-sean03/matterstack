import argparse
import json
import random
import sys
import time


def calculate_properties(candidate_id, doping_level, dopant):
    # Simulate computation time
    time.sleep(0.01)

    # Deterministic RNG: makes the demo reproducible and avoids flakey "all candidates failed" runs.
    rng = random.Random(f"{candidate_id}:{doping_level}:{dopant}")

    # Introduce deterministic failure (~10%)
    if rng.random() < 0.1:
        print(f"Error: Simulation failed for candidate {candidate_id} due to numerical instability.")
        sys.exit(1)

    # Mock physics calculation
    base_energy = -5.0
    # Doping affects energy
    e_form = base_energy + (doping_level * 0.5) + rng.uniform(-0.1, 0.1)

    # Mock voltage calculation
    base_voltage = 3.5
    voltage = base_voltage - (doping_level * 0.2) + rng.uniform(-0.05, 0.05)

    return {
        "candidate_id": candidate_id,
        "doping_level": doping_level,
        "dopant": dopant,
        "E_form": round(e_form, 4),
        "voltage": round(voltage, 4),
        "status": "success"
    }

def main():
    parser = argparse.ArgumentParser(description="Calculate battery material properties")
    parser.add_argument("--candidate_id", type=str, required=True)
    parser.add_argument("--doping_level", type=float, required=True)
    parser.add_argument("--dopant", type=str, required=True)
    parser.add_argument("--output", type=str, default="results.json")

    args = parser.parse_args()

    results = calculate_properties(args.candidate_id, args.doping_level, args.dopant)

    # Write results
    with open(args.output, "w") as f:
        json.dump(results, f, indent=2)

    print(f"Successfully calculated properties for {args.candidate_id}")

if __name__ == "__main__":
    main()
