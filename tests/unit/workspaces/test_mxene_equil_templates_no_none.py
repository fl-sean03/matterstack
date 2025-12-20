from __future__ import annotations

import re
from pathlib import Path

from jinja2 import Template


def _render_template(template_path: Path, *, restart_file: str, **context) -> str:
    template = Template(template_path.read_text())
    return template.render(restart_file=restart_file, **context)


def _non_comment_lines(rendered: str) -> list[str]:
    """
    Return only the non-empty, non-comment lines from a rendered LAMMPS input.

    We intentionally ignore comments because templates may document Python-level concepts
    (like the string "None") without affecting the actual LAMMPS commands being executed.
    """
    out: list[str] = []
    for raw in rendered.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        out.append(line)
    return out


def _extract_run_lines(rendered: str) -> list[str]:
    return [line for line in _non_comment_lines(rendered) if line.startswith("run")]


def _assert_no_none_tokens_in_commands(rendered: str) -> None:
    # Ensure we never emit the failure mode that triggered the bug.
    assert "run             None" not in rendered
    assert "run None" not in rendered

    # Ensure no *command* line contains the literal token "None".
    for line in _non_comment_lines(rendered):
        assert "None" not in line, f"Template rendered a Python None into LAMMPS command: {line!r}"


def test_equil_npt_template_does_not_emit_none_when_overrides_are_missing() -> None:
    rendered = _render_template(
        Path("workspaces/mxene_shear_demo_V2/templates/equil_npt.inp.j2"),
        restart_file="minimize.restart",
        npt_steps=None,
    )

    _assert_no_none_tokens_in_commands(rendered)

    run_lines = _extract_run_lines(rendered)
    assert run_lines, "Expected a 'run ...' line in rendered template"
    assert len(run_lines) == 1, f"Expected exactly one run line, got: {run_lines!r}"

    line = run_lines[0]
    assert re.match(r"^run\s+\d+\s*$", line), f"Expected 'run <int>' line, got: {line!r}"
    assert line.endswith("100000"), f"Expected default run steps to be 100000, got: {line!r}"


def test_equil_nvt_template_does_not_emit_none_when_overrides_are_missing() -> None:
    rendered = _render_template(
        Path("workspaces/mxene_shear_demo_V2/templates/equil_nvt.inp.j2"),
        restart_file="equil_npt.restart",
        nvt_steps=None,
    )

    _assert_no_none_tokens_in_commands(rendered)

    run_lines = _extract_run_lines(rendered)
    assert run_lines, "Expected a 'run ...' line in rendered template"
    assert len(run_lines) == 1, f"Expected exactly one run line, got: {run_lines!r}"

    line = run_lines[0]
    assert re.match(r"^run\s+\d+\s*$", line), f"Expected 'run <int>' line, got: {line!r}"
    assert line.endswith("100000"), f"Expected default run steps to be 100000, got: {line!r}"