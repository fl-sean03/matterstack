from dataclasses import dataclass, field
from typing import Any, Dict, List
import itertools

@dataclass
class Candidate:
    """
    A single point in the design space.
    Represents a concrete material system configuration.
    """
    id: str
    params: Dict[str, Any]
    files: Dict[str, str] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class DesignSpace:
    """
    A class to define valid parameters and enumerate candidates.
    """
    dimensions: Dict[str, List[Any]] = field(default_factory=dict)

    def add_dimension(self, name: str, values: List[Any]) -> None:
        """Add a new dimension to the design space."""
        self.dimensions[name] = values

    def enumerate_candidates(self) -> List[Candidate]:
        """
        Returns the cartesian product of all dimensions as a list of Candidates.
        """
        if not self.dimensions:
            return []

        keys = list(self.dimensions.keys())
        values_list = list(self.dimensions.values())
        
        candidates = []
        # Calculate Cartesian product
        for i, combination in enumerate(itertools.product(*values_list)):
            # Create params dictionary for this combination
            params = dict(zip(keys, combination))
            
            # Generate a simple ID (can be customized)
            cand_id = f"cand_{i:04d}"
            
            candidates.append(Candidate(
                id=cand_id,
                params=params
            ))
            
        return candidates
