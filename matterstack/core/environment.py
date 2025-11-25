from dataclasses import dataclass

@dataclass
class Environment:
    """
    A dataclass for global conditions.
    """
    name: str
    temperature: float
    pressure: float
    media: str
