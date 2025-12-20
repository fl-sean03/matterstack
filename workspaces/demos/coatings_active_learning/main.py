import logging
import json
import random
from pathlib import Path
from typing import List, Dict, Any, Optional, Union
from pydantic import BaseModel, Field

from matterstack import Campaign, Task, Workflow, initialize_run, run_until_completion
from matterstack.ai.surrogate import RandomSurrogate

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- State Definition ---
class Candidate(BaseModel):
    id: str
    params: List[float]

class CoatingsState(BaseModel):
    iteration: int = 0
    candidates: List[Candidate] = []
    results: Dict[str, Dict[str, float]] = Field(default_factory=dict) # candidate_id -> {friction: float, dissolution: float}
    completed_candidates: List[str] = [] # List of IDs

# --- Campaign Logic ---
class CoatingsCampaign(Campaign):
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        # We re-instantiate surrogate each time since we retrain it on full history
        self.surrogate = RandomSurrogate(seed=42) 

    def plan(self, state: Optional[Union[CoatingsState, Dict]]) -> Optional[Workflow]:
        # Handle dict state (from deserialization)
        if isinstance(state, dict):
            state = CoatingsState(**state)

        # Initial State
        if state is None:
            state = CoatingsState()
            self._generate_initial_candidates(state, 10)
            
        iteration = state.iteration
        
        # Decide what to evaluate
        to_evaluate = []
        
        if iteration == 0:
            # Evaluate all initial candidates
            to_evaluate = state.candidates
        elif iteration == 1:
            # Generate new candidates based on "acquisition"
            # 1. Train surrogate on existing results (re-build training set from state)
            self._train_surrogate(state)
            
            # 2. Generate pool
            potential_candidates = []
            start_id = len(state.candidates)
            for i in range(10):
                cid = f"cand_{start_id + i:04d}"
                params = [random.random() for _ in range(5)]
                potential_candidates.append(Candidate(id=cid, params=params))
            
            # 3. Predict
            X = [c.params for c in potential_candidates]
            scores = self.surrogate.predict(X)
            
            # 4. Select top 5
            scored = list(zip(potential_candidates, scores))
            scored.sort(key=lambda x: x[1])
            
            to_evaluate = [x[0] for x in scored[:5]]
            
            # Add to state (this state update is transient until returned by analyze, 
            # but here we are in PLAN. Wait. Plan doesn't return state. 
            # Plan uses state to generate workflow.
            # The candidates generated here need to be part of the 'state' passed to next analyze?
            # Or we just generate tasks. 
            # Actually, if we generate new candidates here, we should probably persist them.
            # But 'plan' only returns a Workflow. 
            # The architecture assumes state is updated in 'analyze'. 
            # So, technically, the generation logic should happen in 'analyze' of the PREVIOUS step?
            # Or we can encode the candidate details in the TASK payload, and extract them in analyze.
            # Let's do that. We won't update state.candidates here directly.
            # Wait, that's messy.
            # Alternative: 'analyze' is responsible for transitioning state. 
            # So, analyze(prev_results) -> updates state (increment iteration, generate new candidates to 'pending' list)
            # plan(state) -> takes 'pending' candidates and makes tasks.
            
            # Let's adjust the logic to follow that pattern.
            # However, for this migration, we have a chicken-and-egg for the *first* plan.
            # If state is None, we return a workflow. But we also need to return the initial state?
            # The 'initialize_run' calls plan(None). It expects a workflow. 
            # It DOES NOT expect a state return.
            # So the initial state must be implicit or we need a way to set it.
            # Actually, `initialize_run` creates the Run but doesn't persist a custom CampaignState yet?
            # The architecture says `analyze` returns State. 
            # `plan` just reads it.
            # If `plan` needs to modify state (e.g. "I have submitted these candidates"), it can't.
            # This suggests `analyze` should prepare the "work queue" in the state.
            
            # Workaround for v0.2 migration of this specific workspace:
            # We will use the `state` passed to `plan`. If it's missing data (like iter 0 candidates),
            # we assume they are implied by the workflow we return, and `analyze` will add them 
            # to the state when it sees the tasks?
            # No, `analyze` sees results.
            
            # Better approach for Cycle 1:
            # In `analyze` of Cycle 0, we will generate the candidates and put them in `state.candidates`.
            # Then `plan` for Cycle 1 just picks them up.
            pass
            
        else:
            return None # Stop after Cycle 1

        wf = Workflow()
        script_dir = Path(__file__).parent / "scripts"
        friction_script = (script_dir / "sim_friction.py").read_text()
        dissolution_script = (script_dir / "sim_dissolution.py").read_text()
        
        for cand in to_evaluate:
            # For Cycle 0, they are in state.candidates.
            # For Cycle 1, they are in state.candidates (added by analyze of Cycle 0).
            
            # Check if already completed
            if cand.id in state.completed_candidates:
                continue
                
            cid = cand.id
            
            # Task 1: Friction
            t1 = Task(
                image="python:3.9",
                command="python3 sim_friction.py input.json -o friction_results.json",
                task_id=f"{cid}_friction",
                files={
                    "sim_friction.py": friction_script,
                    "input.json": json.dumps({"candidate_id": cid, "params": cand.params})
                }
            )
            wf.add_task(t1)
            
            # Task 2: Dissolution
            t2 = Task(
                image="python:3.9",
                command="python3 sim_dissolution.py input.json -o dissolution_results.json",
                task_id=f"{cid}_dissolution",
                files={
                    "sim_dissolution.py": dissolution_script,
                    "input.json": json.dumps({"candidate_id": cid, "params": cand.params})
                }
            )
            wf.add_task(t2)
            
        return wf

    def analyze(self, state: Optional[Union[CoatingsState, Dict]], results: Dict[str, Any]) -> CoatingsState:
        # Handle dict state
        if isinstance(state, dict):
            state = CoatingsState(**state)

        if state is None:
            # Should not happen if we initialized correctly, but handle it
            state = CoatingsState()
            self._generate_initial_candidates(state, 10)

        # Parse results
        new_results = False
        
        for task_id, task_result in results.items():
            if task_result.get("status") != "COMPLETED":
                continue
                
            # Parse candidate ID from task ID
            # format: {cid}_friction or {cid}_dissolution
            parts = task_id.rsplit('_', 1)
            cid = parts[0]
            metric = parts[1]
            
            # Try to read real data
            data = task_result.get("data")
            
            # If data not in manifest, try to read from file
            if not data:
                files = task_result.get("files", {})
                response_path = None
                
                # Check for expected output file in returned files
                expected_file = "friction_results.json" if metric == "friction" else "dissolution_results.json"
                
                for fname, fpath in files.items():
                    if expected_file in str(fname):
                        response_path = fpath
                        break
                
                if response_path and Path(response_path).exists():
                    try:
                        with open(response_path, 'r') as f:
                            data = json.load(f)
                    except Exception as e:
                        logger.error(f"Failed to read result file {response_path}: {e}")
            
            if not data:
                logger.warning(f"No data found for task {task_id}. Skipping.")
                continue
            
            if cid not in state.results:
                state.results[cid] = {}
            
            if metric == "friction":
                state.results[cid]["friction"] = data.get("friction_coefficient", 0.5)
            elif metric == "dissolution":
                state.results[cid]["dissolution"] = data.get("dissolution_rate", 0.5)
                
            new_results = True

        # Update completed candidates
        for cid, metrics in state.results.items():
            if "friction" in metrics and "dissolution" in metrics:
                if cid not in state.completed_candidates:
                    state.completed_candidates.append(cid)

        # Transition Logic
        if state.iteration == 0:
            # Check if all initial candidates are done
            # We know we start with 10.
            if len(state.completed_candidates) >= 10:
                logger.info("Cycle 0 complete. Analyzing and generating Cycle 1 candidates.")
                
                # Train and Select for Cycle 1
                self._train_surrogate(state)
                
                # Generate new candidates
                potential_candidates = []
                start_id = 100 # Jump to 100 for new batch
                for i in range(10):
                    cid = f"cand_{start_id + i:04d}"
                    params = [random.random() for _ in range(5)]
                    potential_candidates.append(Candidate(id=cid, params=params))
                
                # Predict
                X = [c.params for c in potential_candidates]
                scores = self.surrogate.predict(X)
                
                # Select top 5
                scored = list(zip(potential_candidates, scores))
                scored.sort(key=lambda x: x[1])
                top_5 = [x[0] for x in scored[:5]]
                
                state.candidates.extend(top_5)
                state.iteration = 1
                
        elif state.iteration == 1:
            # Check if Cycle 1 is done
            # We added 5 more, so total completed should be 15
            if len(state.completed_candidates) >= 15:
                logger.info("Cycle 1 complete. Campaign Finished.")
                self._print_final_ranking(state)
                state.iteration = 2 # Terminal state
                
        return state

    def _generate_initial_candidates(self, state: CoatingsState, n: int):
        if state.candidates:
            return
        for i in range(n):
            cid = f"cand_{i:04d}"
            params = [random.random() for _ in range(5)]
            state.candidates.append(Candidate(id=cid, params=params))

    def _train_surrogate(self, state: CoatingsState):
        X = []
        y = []
        for cid in state.completed_candidates:
            metrics = state.results[cid]
            cand = next((c for c in state.candidates if c.id == cid), None)
            if cand:
                score = metrics["friction"] + metrics["dissolution"] / 10.0
                X.append(cand.params)
                y.append(score)
        
        if X:
            self.surrogate.fit(X, y)
            logger.info(f"Trained surrogate on {len(X)} samples.")

    def _print_final_ranking(self, state: CoatingsState):
        ranked = []
        for cid in state.completed_candidates:
            metrics = state.results[cid]
            score = metrics["friction"] + metrics["dissolution"] / 10.0
            ranked.append((cid, score, metrics))
        
        ranked.sort(key=lambda x: x[1])
        logger.info("Final Ranking (Top 5):")
        for i, (cid, score, metrics) in enumerate(ranked[:5]):
            logger.info(f"{i+1}. {cid}: Score={score:.4f} (F={metrics['friction']:.4f}, D={metrics['dissolution']:.4f})")

# --- Entry Points ---

def get_campaign(config: Dict[str, Any] = None) -> Campaign:
    return CoatingsCampaign(config)

def get_operators(config: Dict[str, Any] = None) -> Dict[str, Any]:
    # Return default operators or configured ones
    # For now, we rely on the orchestrator's default handling
    return {}

if __name__ == "__main__":
    # Run the campaign
    try:
        # We need to instantiate the campaign first
        campaign = get_campaign()
        handle = initialize_run("coatings_active_learning", campaign)

        # Create config.json to force HPC mode execution locally
        # Must be in the run directory so the orchestrator finds it
        config_path = handle.root_path / "config.json"
        with open(config_path, "w") as f:
            json.dump({"execution_mode": "HPC"}, f)
        
        run_until_completion(handle, campaign)
    finally:
        pass