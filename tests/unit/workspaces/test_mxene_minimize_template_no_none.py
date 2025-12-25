from __future__ import annotations

import re
from pathlib import Path

from jinja2 import Template


def _render_minimize_template(*, minimize_maxiter, minimize_maxeval) -> str:
    template_path = Path("workspaces/mxene_shear_demo_V2/templates/minimize.inp.j2")
    template = Template(template_path.read_text())
    return template.render(
        data_file="structure.data",
        minimize_maxiter=minimize_maxiter,
        minimize_maxeval=minimize_maxeval,
    )


def test_minimize_template_does_not_emit_none_when_overrides_are_missing() -> None:
    rendered = _render_minimize_template(minimize_maxiter=None, minimize_maxeval=None)

    minimize_lines = [
        line.strip()
        for line in rendered.splitlines()
        if line.strip().startswith("minimize")
    ]
    assert minimize_lines, "Expected a 'minimize ...' line in rendered template"
    assert len(minimize_lines) == 1, f"Expected exactly one minimize line, got: {minimize_lines!r}"

    line = minimize_lines[0]
    assert "None" not in line, f"Template rendered a Python None into LAMMPS input: {line!r}"

    # Ensure the numeric integers are present (no blanks, no 'None', etc.).
    assert re.match(r"^minimize\s+1\.0e-4\s+1\.0e-4\s+\d+\s+\d+\s*$", line)

    # Be explicit about defaults expected for MXene equilibration minimization.
    assert line.endswith("5000 10000")
