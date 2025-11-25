import argparse
import json
import os
from pathlib import Path
import statistics

def train_model(input_dirs):
    results = []
    failed_count = 0
    
    print(f"Checking {len(input_dirs)} potential input directories...")

    for d in input_dirs:
        dir_path = Path(d)
        result_file = dir_path / "results.json"
        
        if result_file.exists():
            try:
                with open(result_file, "r") as f:
                    data = json.load(f)
                    results.append(data)
            except Exception as e:
                print(f"Error reading {result_file}: {e}")
                failed_count += 1
        else:
            # This is expected for failed upstream tasks
            # print(f"Missing {result_file}")
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
- Total Candidates: {len(input_dirs)}
- Successful Samples: {success_count}
- Failed/Missing Samples: {failed_count}
- Failure Rate: {failed_count / len(input_dirs) * 100:.1f}%

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
    # We expect a list of directories passed as arguments
    parser.add_argument("input_dirs", nargs="+", help="List of input directories to check")
    
    args = parser.parse_args()
    
    train_model(args.input_dirs)

if __name__ == "__main__":
    main()