import argparse
import json
import os
from pathlib import Path
import statistics

def train_model(input_files):
    results = []
    failed_count = 0
    
    print(f"Checking {len(input_files)} potential input files...")

    for fpath in input_files:
        path = Path(fpath)
        
        if path.exists():
            try:
                with open(path, "r") as f:
                    data = json.load(f)
                    results.append(data)
            except Exception as e:
                print(f"Error reading {path}: {e}")
                failed_count += 1
        else:
            # This is expected for failed upstream tasks
            print(f"Missing {path}")
            failed_count += 1
            
    success_count = len(results)
    print(f"Found {success_count} successful results.")
    print(f"Encountered {failed_count} missing/failed inputs.")
    
    if success_count == 0:
        print("No valid data found to train model!")
        return
        
    # Mock Training
    print("Training model on aggregated data...")
    e_forms = [r["E_form"] for r in results]
    voltages = [r["voltage"] for r in results]
    
    avg_e_form = statistics.mean(e_forms)
    avg_voltage = statistics.mean(voltages)
    
    # Generate Model Card
    model_card = f"""
# Model Card: Battery Material Predictor

## Training Data
- Total Candidates: {len(input_files)}
- Successful Samples: {success_count}
- Failed/Missing Samples: {failed_count}
- Failure Rate: {failed_count / len(input_files) * 100:.1f}%

## Model Statistics
- Average Formation Energy: {avg_e_form:.4f} eV
- Average Voltage: {avg_voltage:.4f} V

## Conclusion
The model has been trained on the available data. The workflow demonstrated robustness by continuing despite {failed_count} upstream failures.
"""
    
    with open("model_card.md", "w") as f:
        f.write(model_card)
        
    print("Model card generated: model_card.md")

def main():
    parser = argparse.ArgumentParser(description="Aggregate results and train model")
    # We expect a list of files passed as arguments
    parser.add_argument("input_files", nargs="+", help="List of input files to check")
    
    args = parser.parse_args()
    
    train_model(args.input_files)

if __name__ == "__main__":
    main()