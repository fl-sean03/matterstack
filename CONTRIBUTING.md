# Contributing to MatterStack

Thank you for your interest in contributing to MatterStack! We aim to build a robust, professional-grade platform for autonomous scientific discovery.

## Development Setup

We use `uv` for extremely fast dependency management and virtual environments.

1.  **Install uv**:
    ```bash
    curl -LsSf https://astral.sh/uv/install.sh | sh
    ```

2.  **Clone the repository**:
    ```bash
    git clone https://github.com/fl-sean03/matterstack.git
    cd matterstack
    ```

3.  **Sync dependencies**:
    This will create a virtual environment and install all dependencies (including dev tools) from `uv.lock`.
    ```bash
    uv sync
    ```

4.  **Activate the environment**:
    ```bash
    source .venv/bin/activate
    ```

## Running Tests

We use `pytest` for our test suite. All contributions must pass existing tests and include new tests for added functionality.

```bash
# Run all tests
pytest

# Run tests with output
pytest -v

# Run a specific test file
pytest tests/test_core_basic.py
```

## Quality Standards

### Code Style

- Python 3.11+
- Ruff for linting and formatting
- 500 LOC limit per file (enforced by `scripts/check_max_lines.sh`)
- **Type Hinting**: All new code must be fully type-hinted.
- **Formatting**: We generally follow PEP 8.
- **Docstrings**: Use Google-style docstrings for all public classes and functions.

### Testing

- Run `uv run pytest tests/` before submitting PRs
- Add characterization tests before refactoring
- Target >80% test coverage for new code

### Pre-commit

Install hooks: `uv run pre-commit install`

Hooks run automatically on commit:
- Ruff lint + format
- LOC limit check

## Pull Request Process

1.  Create a new branch for your feature or fix (`git checkout -b feature/my-new-feature`).
2.  Write code and corresponding tests.
3.  Ensure all tests pass.
4.  Submit a Pull Request to the `main` branch.
5.  Provide a clear description of the problem solved and the solution implemented.

## Reporting Issues

Please use the GitHub Issue tracker to report bugs or request features. Include a minimal reproduction example for bugs.