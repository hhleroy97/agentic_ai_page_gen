# Testing & Code Quality Guide

This document outlines the comprehensive testing strategy and code quality tools for the Agentic Local SEO Content Factory.

## 🧪 Testing Framework Overview

### **Python Backend Testing**

| Tool | Purpose | Configuration |
|------|---------|---------------|
| **pytest** | Core testing framework | `pyproject.toml` |
| **pytest-cov** | Coverage reporting | HTML + XML reports |
| **pytest-mock** | Mocking utilities | Built-in fixtures |
| **moto** | AWS service mocking | S3, Lambda, Step Functions |
| **hypothesis** | Property-based testing | Generate test cases |

### **Frontend Testing**

| Tool | Purpose | Configuration |
|------|---------|---------------|
| **HTMLHint** | HTML validation | `.htmlhintrc` |
| **Stylelint** | CSS linting | `.stylelintrc.json` |
| **ESLint** | JavaScript linting | `.eslintrc.json` |
| **Prettier** | Code formatting | `.prettierrc` |
| **axe-core** | Accessibility testing | npm package |
| **Lighthouse** | Performance auditing | CLI integration |

### **Code Quality Tools**

| Tool | Purpose | Configuration |
|------|---------|---------------|
| **Black** | Python code formatting | `pyproject.toml` |
| **isort** | Import sorting | `pyproject.toml` |
| **flake8** | Style guide enforcement | `pyproject.toml` |
| **mypy** | Static type checking | `pyproject.toml` |
| **bandit** | Security vulnerability scanning | `pyproject.toml` |
| **pylint** | Advanced code analysis | `pyproject.toml` |

## 🚀 Quick Start

### **Install Testing Dependencies**

```bash
# Python testing tools
pip install -r requirements.txt

# Frontend testing tools
npm install

# Optional: Install development tools globally
pip install black isort flake8 mypy bandit pytest
```

### **Run All Tests**

```bash
# Comprehensive test suite
make test

# Individual test categories
make test-python        # Python unit tests only
make test-frontend      # Frontend linting only
make test-fast         # Quick tests (excludes slow tests)
```

### **Code Quality Commands**

```bash
# Format code automatically
make format

# Run linters
make lint

# Security scanning
make security
```

## 📋 Test Categories

### **Unit Tests** (`tests/`)

**Scope**: Individual functions and classes
**Coverage Target**: >90%
**Speed**: Fast (<1 second per test)

```bash
# Run unit tests with coverage
pytest tests/ -v --cov=lambdas --cov-report=html

# Run specific test file
pytest tests/test_schemas.py -v

# Run tests with specific markers
pytest tests/ -m "not slow" -v
```

**Example Test Structure:**
```python
def test_business_validation(sample_business):
    """Test business data validation."""
    assert sample_business.business_id == "test_001"
    assert sample_business.rating <= 5.0
```

### **Integration Tests**

**Scope**: Component interactions
**Coverage**: Lambda functions, AWS services
**Mocking**: AWS services via moto

```python
@pytest.mark.integration
def test_s3_upload_integration(mock_s3_client, s3_buckets):
    """Test S3 upload functionality."""
    # Test actual S3 operations with mocked AWS
```

### **Property-Based Tests**

**Scope**: Business logic validation
**Tool**: Hypothesis
**Purpose**: Generate edge cases automatically

```python
from hypothesis import given, strategies as st

@given(st.text(min_size=10, max_size=70))
def test_title_length_validation(title):
    """Test title validation with generated data."""
    # Test with various title inputs
```

## 🎯 Coverage Requirements

### **Minimum Coverage Targets**

| Component | Target | Critical |
|-----------|---------|----------|
| **Schemas** | 95% | ✅ Data validation |
| **SEO Rules** | 90% | ✅ Content quality |
| **S3 Utils** | 85% | ⚠️ Infrastructure |
| **Bedrock Client** | 80% | ⚠️ External API |
| **Lambda Functions** | 75% | ⚠️ Integration |

### **Coverage Reports**

```bash
# Generate HTML coverage report
pytest --cov=lambdas --cov-report=html
open htmlcov/index.html

# Terminal coverage summary
pytest --cov=lambdas --cov-report=term-missing

# XML for CI/CD
pytest --cov=lambdas --cov-report=xml
```

## 🔍 Frontend Quality Checks

### **HTML Validation**

```bash
# Lint HTML templates
npm run lint:html

# Configuration: .htmlhintrc
{
  "doctype-first": true,
  "alt-require": true,
  "id-unique": true
}
```

### **CSS Linting**

```bash
# Lint CSS files
npm run lint:css

# Auto-fix CSS issues
npx stylelint site_templates/**/*.css --fix
```

### **Accessibility Testing**

```bash
# Check accessibility compliance
npm run test:a11y

# Manual accessibility audit
npx axe-core site_templates/
```

### **Performance Testing**

```bash
# Lighthouse performance audit
npm run test:lighthouse

# Core Web Vitals testing
npm run test:web-vitals
```

## 🔒 Security Testing

### **Python Security Scanning**

```bash
# Scan for security vulnerabilities
bandit -r lambdas/ -f json -o reports/security.json

# High confidence issues only
bandit -r lambdas/ -ll

# Exclude test files
bandit -r lambdas/ --exclude tests/
```

**Common Security Checks:**
- SQL injection vulnerabilities
- Hardcoded secrets
- Insecure random number generation
- Path traversal issues
- Subprocess shell injection

### **Dependency Vulnerability Scanning**

```bash
# Check for known vulnerabilities
pip-audit

# Generate security report
safety check --json --output reports/safety.json
```

## 📊 Quality Metrics

### **Code Quality Scoring**

| Metric | Tool | Target | Weight |
|--------|------|--------|---------|
| **Test Coverage** | pytest-cov | >85% | 30% |
| **Type Coverage** | mypy | >80% | 20% |
| **Linting Score** | flake8 | 0 issues | 25% |
| **Security Score** | bandit | 0 high issues | 25% |

### **Frontend Quality Scoring**

| Metric | Tool | Target |
|--------|------|--------|
| **HTML Validation** | HTMLHint | 0 errors |
| **CSS Quality** | Stylelint | 0 errors |
| **Accessibility** | axe-core | WCAG AA |
| **Performance** | Lighthouse | >90 score |

## 🤖 Continuous Integration

### **GitHub Actions Workflow**

**Triggers**: Push to main/develop, Pull requests
**Jobs**: test, build, security-scan, docs

```yaml
# .github/workflows/ci.yml
- name: Run Python tests
  run: pytest tests/ -v --cov=lambdas

- name: Run frontend linting
  run: npm run lint:all

- name: Security scan
  run: bandit -r lambdas/ -ll
```

### **Pre-commit Hooks**

```bash
# Install pre-commit hooks
pip install pre-commit
pre-commit install

# Manual pre-commit run
pre-commit run --all-files
```

**Hook Configuration:**
```yaml
# .pre-commit-config.yaml
repos:
  - repo: https://github.com/psf/black
    hooks:
      - id: black
  - repo: https://github.com/pycqa/isort
    hooks:
      - id: isort
```

## 📈 Test Strategy

### **Test Pyramid**

```
    /\     E2E Tests (5%)
   /  \    ├── Full pipeline
  /____\   └── User workflows
 /      \
/________\  Integration Tests (20%)
\        /  ├── Lambda functions
 \______/   ├── AWS services
  \    /    └── External APIs
   \  /
    \/      Unit Tests (75%)
            ├── Business logic
            ├── Data validation
            └── Utility functions
```

### **Testing Best Practices**

1. **Arrange, Act, Assert**: Structure tests clearly
2. **Single Responsibility**: One assertion per test
3. **Descriptive Names**: Test names explain behavior
4. **Fast Execution**: Unit tests complete in milliseconds
5. **Deterministic**: Tests produce consistent results
6. **Independent**: Tests don't depend on each other

### **Mock Strategy**

- **External APIs**: Mock Bedrock, S3, Step Functions
- **File System**: Use temporary directories
- **Time**: Mock datetime for consistent timestamps
- **Environment**: Mock environment variables

## 🐛 Debugging Tests

### **Common Test Failures**

| Issue | Cause | Solution |
|-------|-------|----------|
| **Import Errors** | Path issues | Check `sys.path` in `conftest.py` |
| **AWS Errors** | Missing mocks | Use `@mock_s3` decorators |
| **Validation Errors** | Schema changes | Update test data |
| **Coverage Drops** | Uncovered code | Add missing test cases |

### **Debug Commands**

```bash
# Run single test with verbose output
pytest tests/test_schemas.py::test_business_validation -v -s

# Debug test with pdb
pytest tests/test_schemas.py --pdb

# Show test duration
pytest tests/ --durations=10
```

## 🔧 Configuration Files

### **Key Configuration Files**

- `pyproject.toml` - Python tool configuration
- `package.json` - Frontend tool scripts
- `.htmlhintrc` - HTML validation rules
- `.stylelintrc.json` - CSS linting rules
- `.eslintrc.json` - JavaScript linting rules
- `.prettierrc` - Code formatting rules
- `conftest.py` - Pytest fixtures and configuration

### **Environment-Specific Settings**

```bash
# Local development
export TESTING=true
export AWS_REGION=us-east-1

# CI/CD environment
export CI=true
export CODECOV_TOKEN=<token>
```

---

## 📚 Additional Resources

- [pytest Documentation](https://docs.pytest.org/)
- [moto AWS Mocking](https://docs.getmoto.org/)
- [Hypothesis Property Testing](https://hypothesis.readthedocs.io/)
- [HTMLHint Rules](https://htmlhint.com/docs/user-guide/list-rules)
- [Stylelint Rules](https://stylelint.io/user-guide/rules/list)
- [WCAG Accessibility Guidelines](https://www.w3.org/WAI/WCAG21/quickref/)

**Ready to ensure code quality? Run `make test` to get started!** 🚀