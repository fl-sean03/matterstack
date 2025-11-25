from dataclasses import dataclass, field
from typing import Any, Dict, Tuple, Optional

ScalarConstraint = Tuple[str, float]  # e.g. ("<", 50.0) or (">", 0.8)

@dataclass
class Constraints:
    """
    Defines hard limits on candidates.
    Candidates violating these are invalid.
    """
    scalar: Dict[str, ScalarConstraint] = field(default_factory=dict)
    categorical: Dict[str, Any] = field(default_factory=dict)

@dataclass
class Objective:
    """
    Defines what to optimize and what to constrain.
    """
    primary: str                      # e.g. "time_to_failure"
    secondary: Optional[str] = None      # e.g. "cost_per_m2"
    constraints: Constraints = field(default_factory=Constraints)