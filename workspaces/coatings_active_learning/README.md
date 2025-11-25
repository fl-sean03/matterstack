# Autonomous Discovery of Extreme-Environment Coatings

**A Closed-Loop Active Learning Campaign for Multi-Objective Material Optimization**

> **Disclaimer:** This project uses entirely simulated data. The "friction coefficients" and "dissolution rates" are generated using random functions for demonstration purposes. No actual molecular dynamics or DFT calculations are performed.

## 1. Abstract
This project implements an autonomous "Design-Test-Learn" campaign to discover novel coating alloys optimized for extreme environments. By coupling high-fidelity physical simulations with a machine learning surrogate model, the system efficiently navigates a vast chemical space to identify candidates that minimize both friction coefficients and chemical dissolution rates. The workflow demonstrates MatterStack's capability to orchestrate iterative scientific discovery without human intervention.

## 2. Scientific Background
Materials used in aerospace turbines, deep-sea drilling equipment, and geothermal power plants face "tribocorrosion"—the combined synergistic degradation caused by mechanical wear and chemical corrosion. 
- **The Trade-off**: Hard ceramics resist wear but are brittle and prone to corrosion, while softer corrosion-resistant metals suffer from high friction and wear.
- **The Goal**: Discover high-entropy alloy (HEA) coatings that simultaneously exhibit low friction coefficients ($\mu < 0.1$) and low dissolution rates ($< 10^{-6}$ mm/year).

## 3. Computational Challenge
The compositional space for multicomponent alloys is combinatorially vast ($>10^6$ potential combinations). 
1.  **Cost**: High-fidelity Molecular Dynamics (MD) or DFT simulations to calculate friction and dissolution are computationally expensive.
2.  **Efficiency**: A brute-force grid search is infeasible. We need an intelligent agent to select the most promising candidates to simulate next.
3.  **Complexity**: The optimization is multi-objective; we are looking for Pareto-optimal solutions.

## 4. MatterStack Solution
This project utilizes the **MatterStack Campaign Engine** to drive an autonomous discovery loop.
- **`Campaign` Class**: Manages the persistent state of the discovery process (candidates, results, iteration count).
- **`Workflow` Orchestration**: Dynamically generates and submits parallel simulation tasks for selected candidates.
- **Active Learning**: Uses a `RandomSurrogate` (simulating a Gaussian Process or Random Forest) to learn the mapping from composition to performance, guiding the search toward high-performance regions.

## 5. Workflow Architecture
The logic is encapsulated in `main.py` within the `CoatingsCampaign` class:

### Phase A: Initialization (Cycle 0)
- **Candidate Generation**: A seeding population of 10 random alloy compositions is generated.
- **Evaluation**: MatterStack launches parallel tasks for each candidate:
    - `sim_friction.py`: Simulates mechanical sliding contact.
    - `sim_dissolution.py`: Simulates electrochemical attack.

### Phase B: The Learning Loop (Cycle 1+)
1.  **Analyze**: The `analyze()` method aggregates results from completed simulations.
2.  **Train**: A surrogate model is trained on all historical data ($X$: Composition, $y$: Performance Score).
3.  **Acquisition**: The model predicts the performance of a large pool of unseen candidates.
4.  **Select**: The top 5 most promising candidates are selected for the next batch of simulations.
5.  **Execute**: A new `Workflow` is generated for these specific candidates, and the cycle repeats.

## 6. Execution & Results

### Running the Campaign
Execute the campaign driver:
```bash
python3 main.py
```

### Expected Output
The system will perform 2 iterations (Cycle 0 and Cycle 1). You will see logs indicating the training of the surrogate and the selection of new candidates.

```text
INFO:root:Cycle 0: Evaluating 10 initial candidates...
...
INFO:root:Trained surrogate on 10 samples.
INFO:root:Cycle 1: Selected top 5 candidates based on surrogate prediction.
...
INFO:root:Final Ranking (Top 5):
1. cand_0012: Score=0.1542 (F=0.08, D=0.74)
2. cand_0004: Score=0.1890 (F=0.11, D=0.79)
...
```

The final ranking displays the discovered materials that best balance the competing requirements of low friction and high corrosion resistance.