from typing import Any, Dict, List

from matterstack.core.domain import Candidate
from matterstack.core.operators import get_operator

from .context import RuntimeContext


def execute_batch(
    operator_name: str,
    candidates: List[Candidate],
    env: Any,
    ctx: RuntimeContext,
) -> Dict[str, Dict[str, Any]]:
    op = get_operator(operator_name)
    results: Dict[str, Dict[str, Any]] = {}
    for c in candidates:
        results[c.id] = op.fn(c, env, ctx)
    return results
