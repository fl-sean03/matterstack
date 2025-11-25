import json
from pathlib import Path
from typing import List
from matterstack.core.evidence import EvidenceBundle
from matterstack.core.domain import Candidate

def export_evidence_json(bundle: EvidenceBundle, out_path: str) -> None:
    path = Path(out_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        json.dump(bundle.to_dict(), f, indent=2)

def export_markdown_report(
    bundle: EvidenceBundle,
    candidates: List[Candidate],
    primary_metric: str,
    out_path: str,
) -> None:
    path = Path(out_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = []
    lines.append(f"# Campaign Report – {bundle.campaign_name}\n")
    lines.append("## Top Candidates\n")
    for c in candidates:
        lines.append(f"- **{c.id}**: {c.params} | {primary_metric}={c.metrics.get(primary_metric)}")
    path.write_text("\n".join(lines))