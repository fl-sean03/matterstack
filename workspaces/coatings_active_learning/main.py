import logging
import json
import random
from pathlib import Path
from typing import List, Dict, Any, Optional

from matterstack.campaign.engine import Campaign, CampaignState
from matterstack.core.workflow import Workflow, Task
from matterstack.orchestration.results import WorkflowResult, JobState
from matterstack.ai.surrogate import RandomSurrogate
from matterstack.runtime.backends.local import LocalBackend

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CoatingsCampaign(Campaign):
    def __init__(self):
        super().__init__()
        self.candidates = []
        self.results = {}  # candidate_id -> {friction: float, dissolution: float}
        self.surrogate = RandomSurrogate(seed=42)
        self.top_candidates = []
        
        # Initial candidates
        self._generate_candidates(10)
        
    def _generate_candidates(self, n: int):
        start_id = len(self.candidates)
        for i in range(n):
            cid = f"cand_{start_id + i:04d}"
            # Mock params: composition ratios, temperature, etc.
            params = [random.random() for _ in range(5)]
            self.candidates.append({"id": cid, "params": params})
            
    def plan(self) -> Optional[Workflow]:
        # Cycle 0: Evaluate initial 10 candidates
        # Cycle 1: Generate new candidates and evaluate (but instruction says "Run tasks for new candidates", implying we generate them based on surrogate)
        # But for mock acquisition, we just select top 5 from predictions?
        # Instructions:
        # Cycle 0:
        #   - Create tasks for friction and dissolution for each candidate.
        #   - Run workflow.
        #   - Train RandomSurrogate on results.
        #   - Select top 5 candidates based on surrogate prediction (mock acquisition).
        # Cycle 1:
        #   - Run tasks for new candidates.
        #   - Output final ranking.
        
        iteration = self.state.iteration
        
        if iteration == 0:
            # Evaluate all initial candidates
            to_evaluate = self.candidates
        elif iteration == 1:
            # Generate new candidates based on "acquisition" (mocked by just generating 5 new ones)
            # The "Select top 5" part in instructions likely implies we select 5 *new* ones or select 5 best to refine?
            # "Select top 5 candidates based on surrogate prediction (mock acquisition)" usually means acquisition function selects next batch.
            # I will generate 10 random potential candidates, predict with surrogate, and pick top 5 to evaluate.
            
            potential_candidates = []
            start_id = len(self.candidates)
            for i in range(10):
                cid = f"cand_{start_id + i:04d}"
                params = [random.random() for _ in range(5)]
                potential_candidates.append({"id": cid, "params": params})
            
            # Predict
            X = [c["params"] for c in potential_candidates]
            # Surrogate predict returns random values in range [0, 1]
            scores = self.surrogate.predict(X)
            
            # Combine and sort (lower score is better? "random friction score (lower is better)")
            # Let's assume lower is better for the aggregate score too.
            scored = list(zip(potential_candidates, scores))
            scored.sort(key=lambda x: x[1])
            
            # Select top 5
            to_evaluate = [x[0] for x in scored[:5]]
            
            # Add to our main list
            self.candidates.extend(to_evaluate)
            
        else:
            return None
            
        wf = Workflow()
        
        script_dir = Path(__file__).parent / "scripts"
        friction_script = (script_dir / "sim_friction.py").read_text()
        dissolution_script = (script_dir / "sim_dissolution.py").read_text()
        
        for cand in to_evaluate:
            cid = cand["id"]
            
            # Task 1: Friction
            t1 = Task(
                image="python:3.9", # Standard python image
                command="python3 sim_friction.py input.json",
                task_id=f"{cid}_friction",
                files={
                    "sim_friction.py": friction_script,
                    "input.json": json.dumps({"candidate_id": cid, "params": cand["params"]})
                }
            )
            wf.add_task(t1)
            
            # Task 2: Dissolution
            t2 = Task(
                image="python:3.9",
                command="python3 sim_dissolution.py input.json",
                task_id=f"{cid}_dissolution",
                files={
                    "sim_dissolution.py": dissolution_script,
                    "input.json": json.dumps({"candidate_id": cid, "params": cand["params"]})
                }
            )
            wf.add_task(t2)
            
        return wf

    def analyze(self, result: WorkflowResult) -> None:
        # Parse results
        new_data_X = []
        new_data_y = []
        
        for task_id, task_result in result.tasks.items():
            if task_result.status.state != JobState.COMPLETED:
                logger.warning(f"Task {task_id} failed or incomplete.")
                continue
                
            # Parse candidate ID from task ID
            # format: {cid}_friction or {cid}_dissolution
            parts = task_id.rsplit('_', 1)
            cid = parts[0]
            metric = parts[1]
            
            # Read output
            output_file = task_result.workspace_path / "response.json"
            if output_file.exists():
                try:
                    with open(output_file, 'r') as f:
                        data = json.load(f)
                    
                    if cid not in self.results:
                        self.results[cid] = {}
                        
                    if metric == "friction":
                        self.results[cid]["friction"] = data.get("friction_coefficient")
                    elif metric == "dissolution":
                        self.results[cid]["dissolution"] = data.get("dissolution_rate")
                        
                except Exception as e:
                    logger.error(f"Failed to read result for {task_id}: {e}")
            else:
                logger.error(f"Output file not found for {task_id} at {output_file}")

        # Check for complete candidates (both metrics)
        complete_cands = []
        for cid, metrics in self.results.items():
            if "friction" in metrics and "dissolution" in metrics:
                # Find params
                cand = next((c for c in self.candidates if c["id"] == cid), None)
                if cand:
                    complete_cands.append(cand)
                    # Simple objective: minimize friction + dissolution/10 (normalization roughly)
                    score = metrics["friction"] + metrics["dissolution"] / 10.0
                    new_data_X.append(cand["params"])
                    new_data_y.append(score)

        # Train surrogate
        if new_data_X:
            self.surrogate.fit(new_data_X, new_data_y)
            logger.info(f"Trained surrogate on {len(new_data_X)} samples.")
            
        # Update state
        if self.state.iteration >= 1: # After Cycle 1 analysis
            self.state.stopped = True
            
            # Final Ranking
            ranked = []
            for cid, metrics in self.results.items():
                if "friction" in metrics and "dissolution" in metrics:
                     score = metrics["friction"] + metrics["dissolution"] / 10.0
                     ranked.append((cid, score, metrics))
            
            ranked.sort(key=lambda x: x[1])
            logger.info("Final Ranking (Top 5):")
            for i, (cid, score, metrics) in enumerate(ranked[:5]):
                logger.info(f"{i+1}. {cid}: Score={score:.4f} (F={metrics['friction']:.4f}, D={metrics['dissolution']:.4f})")


if __name__ == "__main__":
    print("\n--- SIMULATION MODE: All data is synthetic and for demonstration purposes only ---\n")
    campaign = CoatingsCampaign()
    project_root = Path(__file__).resolve().parent.parent.parent
    backend = LocalBackend(workspace_root=Path(__file__).parent / "results")
    campaign.run(max_iterations=2, backend=backend)