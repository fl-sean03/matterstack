import json
import sys

def main():
    """
    Reads sim_results.json and experiment_results.json, reconciles them.
    Output: final_report.json
    """
    try:
        with open("sim_results.json", 'r') as f:
            sim_data = json.load(f)
            
        with open("experiment_results.json", 'r') as f:
            exp_data = json.load(f)
            
        # Calculate Error
        sim_cond = sim_data.get("conductivity_sim")
        exp_cond = exp_data.get("conductivity_exp")
        
        sim_stab = sim_data.get("stability_sim")
        exp_stab = exp_data.get("stability_exp")
        
        cond_error = abs(sim_cond - exp_cond) / exp_cond if exp_cond else 0
        stab_error = abs(sim_stab - exp_stab) / exp_stab if exp_stab else 0
        
        report = {
            "candidate_id": sim_data.get("candidate_id"),
            "sim_data": sim_data,
            "exp_data": exp_data,
            "metrics": {
                "conductivity_error_rel": cond_error,
                "stability_error_rel": stab_error,
                "overall_drift": (cond_error + stab_error) / 2
            }
        }
        
        print(f"Reconciliation Complete. Drift: {report['metrics']['overall_drift']:.2%}")
        
        with open("final_report.json", "w") as f:
            json.dump(report, f, indent=2)
            
    except Exception as e:
        print(f"Error reconciling data: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()