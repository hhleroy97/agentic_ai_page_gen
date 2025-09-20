#!/bin/bash

# Agentic Local SEO Content Factory - Test Runner Script
# Runs comprehensive testing suite with reporting

set -e  # Exit on any error

echo "🧪 Agentic Local SEO Content Factory - Test Suite"
echo "================================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Create reports directory
mkdir -p reports

echo -e "${BLUE}📋 Running Python Tests...${NC}"

# Run Python tests with coverage
echo "Running unit tests..."
python -m pytest tests/ -v --cov=lambdas --cov-report=html:reports/coverage-html --cov-report=xml:reports/coverage.xml --cov-report=term-missing

# Run type checking
echo -e "${BLUE}🔍 Running Type Checking...${NC}"
python -m mypy lambdas/common/ --ignore-missing-imports || echo -e "${YELLOW}⚠️  MyPy issues found (non-blocking)${NC}"

# Run security scanning
echo -e "${BLUE}🔒 Running Security Scan...${NC}"
python -m bandit -r lambdas/ -f json -o reports/bandit-security.json || echo -e "${YELLOW}⚠️  Security issues found${NC}"

# Run linting
echo -e "${BLUE}📝 Running Code Quality Checks...${NC}"
python -m flake8 lambdas/ --output-file=reports/flake8-issues.txt || echo -e "${YELLOW}⚠️  Linting issues found${NC}"

# Check code formatting
echo -e "${BLUE}🎨 Checking Code Formatting...${NC}"
python -m black --check lambdas/ || echo -e "${YELLOW}⚠️  Code formatting issues found. Run 'black lambdas/' to fix.${NC}"

# Check import sorting
echo -e "${BLUE}📦 Checking Import Sorting...${NC}"
python -m isort --check-only lambdas/ || echo -e "${YELLOW}⚠️  Import sorting issues found. Run 'isort lambdas/' to fix.${NC}"

# Frontend linting (if npm is available)
if command -v npm &> /dev/null; then
    echo -e "${BLUE}🎨 Running Frontend Tests...${NC}"

    # HTML linting
    echo "Checking HTML..."
    npm run lint:html || echo -e "${YELLOW}⚠️  HTML issues found${NC}"

    # CSS linting
    echo "Checking CSS..."
    npm run lint:css || echo -e "${YELLOW}⚠️  CSS issues found${NC}"

    # Format check
    echo "Checking formatting..."
    npx prettier --check site_templates/ || echo -e "${YELLOW}⚠️  Formatting issues found${NC}"
else
    echo -e "${YELLOW}⚠️  npm not found, skipping frontend tests${NC}"
fi

# AWS CloudFormation validation (if AWS CLI is available)
if command -v aws &> /dev/null; then
    echo -e "${BLUE}☁️  Validating CloudFormation Template...${NC}"
    aws cloudformation validate-template --template-body file://template.yaml > reports/cf-validation.json || echo -e "${YELLOW}⚠️  CloudFormation validation issues${NC}"
else
    echo -e "${YELLOW}⚠️  AWS CLI not found, skipping CloudFormation validation${NC}"
fi

# Generate test summary
echo -e "${BLUE}📊 Generating Test Summary...${NC}"

cat > reports/test-summary.md << EOF
# Test Summary Report

Generated: $(date)

## Python Tests
- Unit tests: $(grep -c "PASSED\|FAILED" reports/pytest.log 2>/dev/null || echo "See pytest output")
- Coverage: See reports/coverage-html/index.html

## Code Quality
- Type checking: MyPy results
- Security: See reports/bandit-security.json
- Linting: See reports/flake8-issues.txt

## Frontend
- HTML validation: HTMLHint results
- CSS validation: Stylelint results
- Code formatting: Prettier results

## Infrastructure
- CloudFormation: See reports/cf-validation.json

## Next Steps
1. Review coverage report: open reports/coverage-html/index.html
2. Fix any linting issues found
3. Address security findings if any
4. Ensure all tests pass before deployment
EOF

echo -e "${GREEN}✅ Test suite completed!${NC}"
echo -e "${BLUE}📁 Reports generated in: reports/${NC}"
echo -e "${BLUE}📊 View coverage: open reports/coverage-html/index.html${NC}"

# Exit with error code if any critical tests failed
if [ -f "reports/pytest-failed" ]; then
    echo -e "${RED}❌ Some tests failed - check output above${NC}"
    exit 1
else
    echo -e "${GREEN}🎉 All tests passed!${NC}"
fi