# Agentic Local SEO Content Factory

A production-ready serverless system that automatically generates SEO-optimized local business pages using AI agents, AWS infrastructure, and modern web technologies.

## 🎯 Overview

This system ingests messy business data, cleans and transforms it using SQL + Python, generates high-quality SEO content via an agent→checker loop (Amazon Bedrock), and publishes static HTML websites with structured data markup.

### Key Features

- **🤖 Agentic Content Generation**: Claude-powered content creation with quality control loops
- **🔍 SEO-First Design**: Structured data, meta optimization, and local SEO best practices
- **⚡ Serverless Architecture**: AWS Lambda + Step Functions for scalable, cost-effective operations
- **📊 Data-Driven**: SQL-first analytics with Athena/Glue for transparency and insights
- **🎨 Modern Templates**: Responsive HTML/CSS with accessibility and performance optimization
- **🚀 Fast Deployment**: Complete infrastructure as code with AWS SAM

## 🏗️ Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Raw CSV Data │───▶│  Data Pipeline   │───▶│  Content Gen    │
│   (S3 Bucket)  │    │  (Lambda + SQL)  │    │  (Bedrock AI)   │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                │                        │
                                ▼                        ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│  Static Website │◀───│  HTML Rendering  │◀───│  Quality Check  │
│   (S3 + CDN)   │    │  (Jinja2 + CSS)  │    │  (AI + Rules)   │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

### Technology Stack

- **Infrastructure**: AWS SAM, CloudFormation, S3, Lambda, Step Functions
- **Data Layer**: AWS Glue, Athena, Pandas, SQL
- **AI/ML**: Amazon Bedrock (Claude), Pydantic validation
- **Frontend**: Jinja2, HTML5, CSS3, responsive design
- **Languages**: Python 3.12, SQL, JavaScript

## 📁 Project Structure

```
agentic-ai-page-gen/
├── README.md                    # This file
├── template.yaml               # AWS SAM infrastructure template
├── Makefile                    # Build and deployment commands
├── requirements.txt            # Python dependencies
├── .gitignore                 # Git ignore patterns
├── CLAUDE.md                  # Project guidelines and instructions
│
├── statemachine/
│   └── definition.asl.json    # Step Functions workflow definition
│
├── lambdas/
│   ├── common/                # Shared utilities and schemas
│   │   ├── schemas.py         # Pydantic data models
│   │   ├── prompts.py         # AI prompt templates
│   │   ├── s3_utils.py        # S3 operations utilities
│   │   ├── seo_rules.py       # SEO validation rules
│   │   └── bedrock_client.py  # Amazon Bedrock client
│   │
│   ├── ingest_raw/            # CSV data ingestion
│   │   └── app.py
│   ├── clean_transform/       # Data cleaning and transformation
│   │   └── app.py
│   ├── agent_generate/        # AI content generation
│   │   └── app.py
│   ├── agent_qc/             # Quality control and validation
│   │   └── app.py
│   ├── render_html/          # HTML page rendering
│   │   └── app.py
│   └── publish_site/         # Website publishing and deployment
│       └── app.py
│
├── site_templates/            # Jinja2 HTML templates and assets
│   ├── base.html             # Base template with SEO optimization
│   ├── listing.html          # Business listing page template
│   ├── styles.css            # Professional CSS styling
│   ├── sitemap.xml.j2        # XML sitemap template
│   └── robots.txt            # Search engine directives
│
├── sql/                      # Analytics and data queries
│   ├── profiling.sql         # Data quality analysis queries
│   └── publish_list.sql      # Publication readiness queries
│
├── data/                     # Sample and test data
│   └── sample_businesses.csv # Sample business data for testing
│
└── notebooks/                # Analysis and exploration
    └── eda.ipynb            # Exploratory data analysis notebook
```

## 🚀 Quick Start

### Prerequisites

- AWS CLI configured with appropriate permissions
- AWS SAM CLI installed
- Python 3.12+ and pip
- Make (optional, for convenience commands)

### 1. Deploy Infrastructure

```bash
# Clone the repository
git clone <repository-url>
cd agentic-ai-page-gen

# Install dependencies
make install

# Build and deploy to AWS
make build
make deploy
```

### 2. Upload Sample Data

```bash
# Upload sample business data
make upload-sample

# Run Glue crawler to catalog data
make crawl
```

### 3. Generate Content

```bash
# Run the full content generation pipeline
make demo

# Monitor execution in AWS Step Functions console
```

### 4. View Results

After successful execution, your static website will be available at the S3 website URL displayed in the deployment outputs.

## 📊 Phase-Based Development Plan

### Day 1 – Infrastructure & Data Layer
- [x] Deploy SAM stack (S3, IAM, Lambdas, Step Functions, Glue DB, Athena)
- [x] Load sample CSV to `raw/`, run Glue Crawler, confirm Athena tables
- [x] Implement `clean_transform` (Pandas + data validation)

### Day 2 – Agentic Generation & QC
- [x] Implement Pydantic schema + generator prompt + QC/repair loop
- [x] Store raw and repaired `PageSpec` JSON; write execution traces
- [x] Render Jinja templates → HTML; build sitemap/robots.txt

### Day 3 – Polish & Demo
- [x] Publish 25+ sample pages; add index page listing
- [x] Add profiling queries and data analysis notebook
- [x] Create comprehensive documentation and setup guide

## 🔧 Configuration

### Environment Variables

The system uses the following environment variables (automatically set by SAM):

- `BEDROCK_REGION`: AWS region for Bedrock service (default: us-east-1)
- `RAW_BUCKET`: S3 bucket for raw business data
- `PROCESSED_BUCKET`: S3 bucket for processed data and generated content
- `WEBSITE_BUCKET`: S3 bucket for published static website
- `GLUE_DATABASE`: Glue database name for data catalog
- `ATHENA_WORKGROUP`: Athena workgroup for running queries

### Customization

1. **Content Templates**: Modify files in `site_templates/` to customize page design
2. **AI Prompts**: Edit `lambdas/common/prompts.py` to adjust content generation
3. **SEO Rules**: Update `lambdas/common/seo_rules.py` for validation criteria
4. **Data Schema**: Modify `lambdas/common/schemas.py` for different business data

## 📈 Data Pipeline

### 1. Data Ingestion (`ingest_raw`)
- Validates CSV format and required fields
- Cleans phone numbers, addresses, and contact information
- Generates data quality reports

### 2. Data Transformation (`clean_transform`)
- Standardizes business categories and locations
- Calculates content generation priority scores
- Runs SQL-based data quality checks

### 3. Content Generation (`agent_generate`)
- Uses Amazon Bedrock (Claude) to generate SEO content
- Creates structured PageSpec objects with metadata
- Implements category-specific content strategies

### 4. Quality Control (`agent_qc`)
- AI-powered content review and scoring
- Automated SEO compliance checking
- Iterative improvement with feedback loops

### 5. HTML Rendering (`render_html`)
- Jinja2 template rendering with structured data
- Responsive design with accessibility features
- Sitemap and robots.txt generation

### 6. Site Publishing (`publish_site`)
- S3 static website configuration
- Performance optimization and analytics setup
- Deployment reporting and monitoring

## 🎯 SEO Optimization

### Technical SEO
- ✅ Valid HTML5 with semantic markup
- ✅ Schema.org LocalBusiness structured data
- ✅ Meta titles (10-70 characters) and descriptions (50-160 characters)
- ✅ Responsive design with mobile optimization
- ✅ XML sitemap with proper priority and frequency
- ✅ Robots.txt with crawl directives

### Content SEO
- ✅ Minimum 800-word content per page
- ✅ Local keyword optimization
- ✅ Internal linking between related businesses
- ✅ Geographic relevance and local citations
- ✅ Unique, high-quality AI-generated content

### Performance
- ✅ Lightweight CSS with minimal JavaScript
- ✅ Optimized images and assets
- ✅ Fast loading times on S3/CloudFront
- ✅ Accessibility compliance (WCAG guidelines)

## 📊 Analytics and Monitoring

### Data Quality Metrics
- Business data completeness percentages
- Content generation success rates
- SEO compliance scoring
- Geographic and category distribution

### Performance Tracking
- Page generation speed and costs
- Content quality scores and improvements
- User engagement metrics (when analytics enabled)
- Search engine indexing status

### Query Examples

```sql
-- Get content generation candidates
SELECT * FROM businesses
WHERE priority_score >= 70
ORDER BY priority_score DESC;

-- Analyze market opportunities
SELECT city, category, COUNT(*) as competition
FROM businesses
GROUP BY city, category
HAVING COUNT(*) <= 3;
```

## 🛠️ Development

### Local Testing

```bash
# Install development dependencies
pip install -r requirements.txt
pip install pytest jupyter pandas matplotlib

# Run data analysis
jupyter notebook notebooks/eda.ipynb

# Test individual components
python -m pytest tests/ -v

# Validate data quality
make query
```

### Adding New Features

1. **New Business Categories**: Update `prompts.py` and `seo_rules.py`
2. **Additional Data Sources**: Modify `ingest_raw/app.py` and schemas
3. **Custom Templates**: Add new templates to `site_templates/`
4. **Enhanced Analytics**: Extend SQL queries in `sql/` directory

## 🔐 Security & Compliance

### AWS Security
- IAM roles with minimum required permissions
- S3 buckets with appropriate access controls
- VPC endpoints for Lambda (when configured)
- CloudTrail logging for audit compliance

### Data Privacy
- No PII stored in logs or traces
- Configurable data retention policies
- GDPR/CCPA compliance considerations
- Secure API key management

## 💰 Cost Optimization

### Estimated Costs (per 1000 businesses)
- **Lambda execution**: ~$2-5
- **Bedrock API calls**: ~$10-20 (varies by model)
- **S3 storage**: ~$1-3
- **Athena queries**: ~$1-2
- **Total**: ~$15-30 per 1000 generated pages

### Cost Controls
- Efficient batching and retry logic
- Optimal Lambda memory/timeout settings
- S3 lifecycle policies for old data
- Reserved capacity for high-volume usage

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Follow the coding standards in `CLAUDE.md`
4. Add tests for new functionality
5. Submit a pull request with detailed description

### Code Standards
- Type hints and docstrings for all functions
- Pydantic models for data validation
- Error handling with structured logging
- Security-first development practices

## 📚 Additional Resources

- [AWS SAM Documentation](https://docs.aws.amazon.com/serverless-application-model/)
- [Amazon Bedrock Guide](https://docs.aws.amazon.com/bedrock/)
- [Local SEO Best Practices](https://developers.google.com/search/docs/advanced/guidelines/local)
- [Schema.org LocalBusiness](https://schema.org/LocalBusiness)

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

For questions, issues, or feature requests:

1. Check the [CLAUDE.md](CLAUDE.md) file for project guidelines
2. Review existing issues in the repository
3. Create a new issue with detailed information
4. Follow the contribution guidelines for pull requests

---

**🚀 Ready to generate thousands of SEO-optimized pages automatically?**

Start with `make deploy` and watch the Agentic Local SEO Content Factory create professional business pages at scale!
