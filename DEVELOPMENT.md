# Development Guide

## Project Structure

```
truenas_pynvmeof_client/
├── src/
│   └── nvmeof_client/          # Main package code
│       ├── __init__.py          # Public API exports
│       ├── client.py            # NVMeoFClient class
│       ├── models.py            # Data structures
│       ├── exceptions.py        # Exception classes
│       ├── parsers/             # Protocol parsers
│       │   ├── __init__.py
│       │   ├── base.py
│       │   ├── response.py
│       │   ├── controller.py
│       │   ├── namespace.py
│       │   ├── discovery.py
│       │   ├── ana.py
│       │   └── async_event.py
│       └── protocol/            # Protocol structures
│           ├── __init__.py
│           ├── pdu.py
│           ├── admin_commands.py
│           ├── fabric_commands.py
│           ├── io_commands.py
│           └── utils.py
├── tests/                       # Test suite
│   ├── unit/                    # Unit tests
│   ├── integration/             # Integration tests
│   └── fixtures/                # Test fixtures
├── pyproject.toml              # Package configuration
├── README.md                   # User documentation
├── CHANGELOG.md                # Version history
├── LICENSE                     # License (LGPL v3)
├── MANIFEST.in                 # Distribution files
└── .gitignore                  # Git ignore rules
```

## Quick Start

### Installation for Development

```bash
# Clone the repository
git clone https://github.com/truenas/truenas_pynvmeof_client.git
cd truenas_pynvmeof_client

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install in editable mode
pip install -e .

# Or with development dependencies
pip install -e ".[dev]"
```

### Installation from Git (for CI)

In your `requirements.txt`:

```
nvmeof-client @ git+https://github.com/truenas/truenas_pynvmeof_client.git
```

Or for a specific branch/tag:

```
nvmeof-client @ git+https://github.com/truenas/truenas_pynvmeof_client.git@main
nvmeof-client @ git+https://github.com/truenas/truenas_pynvmeof_client.git@v1.0.0
```

Or install directly:

```bash
pip install git+https://github.com/truenas/truenas_pynvmeof_client.git
```

### Running Tests

Tests can be run directly from the git checkout without installing the package first (pytest is configured to find the source in `src/`):

```bash
# Run all tests
pytest

# Run only unit tests
pytest tests/unit/

# Run specific test file
pytest tests/unit/test_exceptions.py

# Run with verbose output
pytest -v

# Run with coverage
pytest --cov=nvmeof_client tests/

# Run in parallel (requires pytest-xdist)
pytest -n auto
```

**Note:** The `pythonpath = ["src"]` setting in `pyproject.toml` allows pytest to find the package without installation. However, for development it's still recommended to install in editable mode (`pip install -e .`) as this ensures imports work consistently across all tools.

### Code Quality

```bash
# Format code
black src/ tests/

# Lint code
ruff check src/ tests/

# Type check
mypy src/
```

## Package Layout Benefits

The `src/` layout provides several advantages:

1. **Import Safety**: Can't accidentally import from repository instead of installed package
2. **Clean Separation**: Code vs. infrastructure files clearly separated
3. **Tool Compatibility**: All modern Python tools expect this structure
4. **Testing Isolation**: Tests import from installed package, not source

## Making Changes

1. Make your changes in `src/nvmeof_client/`
2. Add tests in `tests/unit/` or `tests/integration/`
3. Run tests: `pytest`
4. Format code: `black src/ tests/`
5. Check linting: `ruff check src/ tests/`
6. Update CHANGELOG.md
7. Commit and push

## Versioning

Version is defined in:
- `pyproject.toml` - `version = "1.0.0"`
- `src/nvmeof_client/__init__.py` - `__version__ = "1.0.0"`

Follow [Semantic Versioning](https://semver.org/):
- MAJOR.MINOR.PATCH
- 1.0.0 → 1.0.1 (bug fixes)
- 1.0.0 → 1.1.0 (new features, backwards compatible)
- 1.0.0 → 2.0.0 (breaking changes)

## Building Distribution

```bash
# Install build tools
pip install build

# Build distribution
python -m build

# Creates:
# dist/nvmeof_client-1.0.0-py3-none-any.whl
# dist/nvmeof-client-1.0.0.tar.gz
```

## Common Tasks

### Adding a New Module

```bash
# Add to src/nvmeof_client/
touch src/nvmeof_client/new_module.py

# Import in __init__.py if part of public API
# Add tests in tests/unit/test_new_module.py
```

### Adding Dependencies

Edit `pyproject.toml`:

```toml
[project]
dependencies = [
    "some-package>=1.0.0",
]
```

### Updating Documentation

Edit `README.md` for user-facing documentation.
Edit `DEVELOPMENT.md` (this file) for developer documentation.

## CI Integration

Your CI can install the package like this:

```yaml
# GitHub Actions example
steps:
  - name: Install package
    run: |
      pip install git+https://github.com/truenas/truenas_pynvmeof_client.git

  - name: Run tests from CI
    run: |
      pytest tests/
```

Or with a requirements.txt:

```yaml
steps:
  - name: Install dependencies
    run: |
      pip install -r requirements.txt

  - name: Run tests
    run: |
      pytest tests/
```

## Troubleshooting

### Import Errors

If you get import errors, make sure you've installed the package:

```bash
pip install -e .
```

### Tests Not Found

Make sure pytest can find the tests:

```bash
# From repository root
pytest tests/

# Or specify path
pytest tests/unit/test_client.py
```

### Changes Not Reflected

If using editable install (`-e`), changes should be reflected immediately.
If not, try reinstalling:

```bash
pip install -e . --force-reinstall --no-deps
```
