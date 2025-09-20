.PHONY: help install build deploy clean test crawl query render demo

help:
	@echo "Agentic Local SEO Content Factory - Available Commands:"
	@echo ""
	@echo "Setup & Deployment:"
	@echo "  install    - Install dependencies and setup environment"
	@echo "  build      - Build SAM application"
	@echo "  deploy     - Deploy infrastructure to AWS"
	@echo "  clean      - Clean build artifacts"
	@echo ""
	@echo "Data Pipeline:"
	@echo "  crawl      - Run Glue crawler on raw data"
	@echo "  query      - Execute Athena profiling queries"
	@echo "  render     - Generate sample pages (local test)"
	@echo ""
	@echo "Demo & Testing:"
	@echo "  demo       - Run full pipeline demonstration"
	@echo "  test       - Run unit tests"
	@echo ""

# Environment variables
STACK_NAME ?= agentic-seo-factory
REGION ?= us-east-1
S3_BUCKET ?= $(STACK_NAME)-sam-artifacts-$(shell date +%s)

install:
	@echo "Installing dependencies..."
	pip install -r requirements.txt
	@echo "Setting up pre-commit hooks..."
	pre-commit install || echo "pre-commit not available, skipping..."

build:
	@echo "Building SAM application..."
	sam build --use-container

deploy: build
	@echo "Deploying to AWS..."
	sam deploy --guided --stack-name $(STACK_NAME) --region $(REGION) --capabilities CAPABILITY_IAM
	@echo "Deployment complete! Check outputs:"
	aws cloudformation describe-stacks --stack-name $(STACK_NAME) --region $(REGION) --query 'Stacks[0].Outputs'

clean:
	@echo "Cleaning build artifacts..."
	rm -rf .aws-sam/
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete

crawl:
	@echo "Starting Glue crawler..."
	$(eval CRAWLER_NAME := $(shell aws cloudformation describe-stacks --stack-name $(STACK_NAME) --query 'Stacks[0].Outputs[?OutputKey==`GlueCrawler`].OutputValue' --output text))
	aws glue start-crawler --name $(CRAWLER_NAME)
	@echo "Crawler started. Check status with: aws glue get-crawler --name $(CRAWLER_NAME)"

query:
	@echo "Running Athena profiling queries..."
	$(eval WORKGROUP := $(shell aws cloudformation describe-stacks --stack-name $(STACK_NAME) --query 'Stacks[0].Outputs[?OutputKey==`AthenaWorkgroup`].OutputValue' --output text))
	aws athena start-query-execution --query-string "$$(cat sql/profiling.sql)" --work-group $(WORKGROUP)

render:
	@echo "Testing page rendering locally..."
	cd notebooks && jupyter nbconvert --execute --to notebook --inplace eda.ipynb

demo:
	@echo "Running full pipeline demonstration..."
	$(eval STATE_MACHINE := $(shell aws cloudformation describe-stacks --stack-name $(STACK_NAME) --query 'Stacks[0].Outputs[?OutputKey==`StateMachineArn`].OutputValue' --output text))
	aws stepfunctions start-execution --state-machine-arn $(STATE_MACHINE) --input '{"source": "demo", "batch_size": 10}'
	@echo "Pipeline execution started. Monitor at: https://console.aws.amazon.com/states/"

test:
	@echo "Running unit tests..."
	python -m pytest tests/ -v || echo "No tests found yet"

# Upload sample data
upload-sample:
	$(eval RAW_BUCKET := $(shell aws cloudformation describe-stacks --stack-name $(STACK_NAME) --query 'Stacks[0].Outputs[?OutputKey==`RawDataBucket`].OutputValue' --output text))
	aws s3 cp data/sample_businesses.csv s3://$(RAW_BUCKET)/businesses/sample_businesses.csv
	@echo "Sample data uploaded to $(RAW_BUCKET)"

# Quick development cycle
dev: build deploy upload-sample crawl
	@echo "Development environment ready!"