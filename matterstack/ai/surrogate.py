from __future__ import annotations

import random
from abc import ABC, abstractmethod
from typing import List

# Type aliases for broader compatibility (using basic types if numpy not available in env)
Matrix = List[List[float]]
Vector = List[float]


class Surrogate(ABC):
    """
    Abstract interface for AI/ML surrogate models.
    """

    @abstractmethod
    def fit(self, X: Matrix, y: Vector) -> None:
        """
        Train the surrogate model on the provided data.

        Args:
            X: Matrix of input features (n_samples, n_features)
            y: Vector of target values (n_samples,)
        """
        pass

    @abstractmethod
    def predict(self, X: Matrix) -> Vector:
        """
        Predict target values for new inputs.

        Args:
            X: Matrix of input features (n_samples, n_features)

        Returns:
            Vector of predicted values (n_samples,)
        """
        pass


class RandomSurrogate(Surrogate):
    """
    A dummy surrogate that returns random predictions. Useful for testing harnesses.
    """

    def __init__(self, seed: int = 42):
        self.seed = seed
        self.rng = random.Random(seed)
        self.fitted = False

    def fit(self, X: Matrix, y: Vector) -> None:
        # No-op, just mark as fitted
        self.fitted = True

    def predict(self, X: Matrix) -> Vector:
        if not self.fitted:
            raise RuntimeError("Model must be fitted before prediction.")

        # Return random float between 0 and 1 for each sample
        return [self.rng.random() for _ in range(len(X))]
