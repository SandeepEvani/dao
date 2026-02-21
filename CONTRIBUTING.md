# Contributing to DAO

Thank you for your interest in contributing to DAO! This guide will help you get started.

## Branching Model

```
main            ← active development (v1.x), default branch
legacy/v0.x     ← archived v0 code (read-only, critical fixes only)
feature/*       ← new features (branch from main)
fix/*           ← bug fixes (branch from main)
```

### Where to submit PRs

| Change type             | Target branch   |
|-------------------------|-----------------|
| New feature for v1.x    | `main`          |
| Bug fix for v1.x        | `main`          |
| Critical fix for v0.x   | `legacy/v0.x`   |

## Getting Started

### 1. Fork & clone

```bash
git clone https://github.com/<your-fork>/DAO.git
cd DAO
```

### 2. Set up the development environment

```bash
pip install -e ".[dev]"
```

### 3. Create a feature branch

```bash
git checkout -b feature/my-feature main
```

### 4. Make your changes

- Write clear, well-documented code
- Add or update tests for any new functionality
- Run the test suite before submitting

### 5. Run checks locally

```bash
# Lint
black --check src/ tests/
isort --check src/ tests/

# Test
pytest
```

### 6. Commit with a descriptive message

We follow [Conventional Commits](https://www.conventionalcommits.org/):

```
feat: add support for Iceberg interface
fix: resolve routing ambiguity with **kwargs-only methods
docs: update quickstart example
chore: update CI matrix to Python 3.13
```

### 7. Push and open a PR

```bash
git push origin feature/my-feature
```

Then open a Pull Request against `main` on GitHub.

## Pull Request Checklist

- [ ] Branch is based on `main` (or `legacy/v0.x` for v0 patches)
- [ ] Code passes `black` and `isort` formatting checks
- [ ] Tests pass (`pytest`)
- [ ] New functionality includes tests
- [ ] Documentation is updated if applicable

## Code Style

- **Formatter:** [Black](https://black.readthedocs.io/) with `line-length = 120`
- **Import sorting:** [isort](https://pycqa.github.io/isort/) with `profile = "black"`

## Reporting Issues

- Use the [bug report template](.github/ISSUE_TEMPLATE/bug_report.md) for bugs
- Use the [feature request template](.github/ISSUE_TEMPLATE/feature_request.md) for ideas

## Code of Conduct

Please read and follow our [Code of Conduct](CODE_OF_CONDUCT.md).

