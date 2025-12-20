import json
import random
import sys

def main():
    # Mock input parsing (not actually used, just simulating args)
    if len(sys.argv) > 1:
        print(f"Calculating for input: {sys.argv[1]}")
    
    # Simulate work
    adsorption_energy = -1.5 + random.random() * 2.0 # -1.5 to 0.5
    
    result = {
        "adsorption_energy_ev": adsorption_energy,
        "converged": True
    }
    
    with open("energy.json", "w") as f:
        json.dump(result, f, indent=2)
        
    print(f"Calculated adsorption energy: {adsorption_energy:.3f} eV")

if __name__ == "__main__":
    main()