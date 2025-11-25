import json
import random
import sys

def main():
    """
    Generates synthetic simulation predictions for a thin-film material.
    Output: sim_results.json
    """
    print("Starting Simulation Prediction...")
    
    # Generate synthetic properties
    # Target: High conductivity, high stability
    
    conductivity = random.uniform(10.0, 100.0)
    stability = random.uniform(0.0, 1.0)
    
    # Random composition
    composition = {
        "A": round(random.uniform(0.1, 0.9), 2),
        "B": round(random.uniform(0.1, 0.9), 2)
    }
    
    results = {
        "conductivity_sim": conductivity,
        "stability_sim": stability,
        "composition": composition,
        "candidate_id": f"cand_{random.randint(1000, 9999)}"
    }
    
    print(f"Generated predictions: {results}")
    
    with open("sim_results.json", "w") as f:
        json.dump(results, f, indent=2)

if __name__ == "__main__":
    main()