# Contributing to LocalArchive

Thanks for your interest! This project builds free, local-first tools that help real people.

## Getting Started

```bash
git clone https://github.com/YOUR_USERNAME/LocalArchive.git
cd LocalArchive
python -m venv venv
source venv/bin/activate
pip install -e ".[dev]"
pytest
```

## Guidelines

1. **Keep it local-first** - No features requiring network calls for basic use.
2. **No paywalls** - Every feature is free. Period.
3. **Privacy by default** - Never collect, transmit, or log user data.
4. **Test your changes** - Add tests for new features. Run `pytest` before submitting.
5. **Keep dependencies minimal** - Prefer stdlib when possible.

## Code Style

- Python 3.10+ features welcome
- Format with `ruff format`
- Lint with `ruff check`

## Pull Request Process

1. Create a feature branch: `git checkout -b feature/your-feature`
2. Make changes with tests
3. Run `pytest` and `ruff check`
4. Submit PR with clear description

## Areas That Need Help

- OCR accuracy testing with diverse document types
- More extraction patterns for common document fields
- Web UI polish
- Documentation and usage guides
- Platform-specific installers
