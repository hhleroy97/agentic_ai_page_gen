"""
HTML Rendering Lambda function.
Converts PageSpec JSON into fully-rendered HTML pages using templates.
"""

import json
import os
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
from jinja2 import Environment, BaseLoader, select_autoescape

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add common modules to path
from schemas import Business, PageSpec, PipelineStatus
from s3_utils import S3Manager


class StringTemplateLoader(BaseLoader):
    """Jinja2 loader for string templates"""

    def __init__(self, template_string: str):
        self.template_string = template_string

    def get_source(self, environment, template):
        return self.template_string, None, lambda: True


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for HTML rendering.

    Args:
        event: Lambda event (Step Functions input)
        context: Lambda context

    Returns:
        Dictionary with rendering results
    """
    logger.info(f"Starting HTML rendering with event: {json.dumps(event, default=str)}")

    try:
        # Initialize S3 manager
        s3_manager = S3Manager(region_name=os.environ.get('AWS_REGION', 'us-east-1'))

        # Get bucket names from environment
        processed_bucket = os.environ['PROCESSED_BUCKET']
        website_bucket = os.environ['WEBSITE_BUCKET']

        # Extract parameters from event
        execution_id = event.get('execution_id', context.aws_request_id)
        input_file = event.get('output_file')  # From agent_qc step

        if not input_file:
            raise Exception("No input file specified from previous step")

        # Initialize pipeline status
        pipeline_status = PipelineStatus(
            execution_id=execution_id,
            stage='render_html',
            total_businesses=0
        )

        # Download QC results data
        logger.info(f"Downloading QC results: s3://{processed_bucket}/{input_file}")
        qc_data = s3_manager.download_json(processed_bucket, input_file)

        if not qc_data:
            error_msg = f"Failed to download QC results from {input_file}"
            logger.error(error_msg)
            pipeline_status.errors.append(error_msg)
            pipeline_status.stage = 'failed'
            s3_manager.save_pipeline_status(processed_bucket, pipeline_status)
            raise Exception(error_msg)

        generated_pages = qc_data.get('generated_pages', [])
        qc_results = qc_data.get('qc_results', [])
        pipeline_status.total_businesses = len(generated_pages)

        logger.info(f"Rendering {len(generated_pages)} pages to HTML")

        # Create mapping of business_id to QC results
        qc_lookup = {result['business_id']: result for result in qc_results}

        # Load HTML template
        template_html = get_page_template()
        jinja_env = Environment(
            loader=StringTemplateLoader(template_html),
            autoescape=select_autoescape(['html', 'xml'])
        )
        template = jinja_env.get_template('')

        # Render each page
        rendered_pages = []
        successful_renders = 0
        failed_renders = 0

        for idx, page_data in enumerate(generated_pages):
            try:
                business_id = page_data['business_id']
                generation_successful = page_data.get('generation_successful', False)

                if not generation_successful:
                    logger.info(f"Skipping render for {business_id} - generation failed")
                    rendered_pages.append({
                        'business_id': business_id,
                        'render_successful': False,
                        'reason': 'generation_failed',
                        'html_file': None
                    })
                    failed_renders += 1
                    continue

                page_spec_dict = page_data.get('page_spec')
                if not page_spec_dict:
                    error_msg = f"No page spec found for {business_id}"
                    logger.error(error_msg)
                    rendered_pages.append({
                        'business_id': business_id,
                        'render_successful': False,
                        'reason': 'no_page_spec',
                        'html_file': None
                    })
                    failed_renders += 1
                    continue

                # Create PageSpec object
                page_spec = PageSpec(**page_spec_dict)
                logger.info(f"Rendering HTML for: {page_spec.business.name}")

                # Get QC information
                qc_info = qc_lookup.get(business_id, {})
                quality_score = None
                if qc_info.get('quality_feedback'):
                    quality_score = qc_info['quality_feedback'].get('quality_score', 0.0)

                # Render HTML
                html_content = render_page_html(template, page_spec, quality_score)

                # Save HTML to website bucket
                html_filename = f"{page_spec.seo.slug}.html"
                html_key = f"pages/{html_filename}"

                if s3_manager.upload_text(website_bucket, html_key, html_content):
                    rendered_pages.append({
                        'business_id': business_id,
                        'render_successful': True,
                        'html_file': html_key,
                        'slug': page_spec.seo.slug,
                        'title': page_spec.seo.title,
                        'quality_score': quality_score
                    })
                    successful_renders += 1
                    logger.info(f"Successfully rendered: {page_spec.business.name} -> {html_filename}")
                else:
                    error_msg = f"Failed to upload HTML for {business_id}"
                    logger.error(error_msg)
                    rendered_pages.append({
                        'business_id': business_id,
                        'render_successful': False,
                        'reason': 'upload_failed',
                        'html_file': None
                    })
                    failed_renders += 1

            except Exception as e:
                error_msg = f"Error rendering page {idx + 1}: {str(e)}"
                logger.error(error_msg)
                pipeline_status.errors.append(error_msg)
                failed_renders += 1

        # Update pipeline status
        pipeline_status.processed_businesses = successful_renders

        # Generate sitemap
        sitemap_content = generate_sitemap(rendered_pages)
        s3_manager.upload_text(website_bucket, "sitemap.xml", sitemap_content)

        # Generate robots.txt
        robots_content = generate_robots_txt()
        s3_manager.upload_text(website_bucket, "robots.txt", robots_content)

        # Generate index page
        index_content = generate_index_page(rendered_pages, template)
        s3_manager.upload_text(website_bucket, "index.html", index_content)

        # Save rendering results
        output_key = f"content/rendered_{execution_id}.json"
        output_data = {
            'execution_id': execution_id,
            'source_file': input_file,
            'total_pages': len(generated_pages),
            'successful_renders': successful_renders,
            'failed_renders': failed_renders,
            'rendered_pages': rendered_pages
        }

        if not s3_manager.upload_json(processed_bucket, output_key, output_data):
            error_msg = "Failed to save rendering results"
            logger.error(error_msg)
            pipeline_status.errors.append(error_msg)
            pipeline_status.stage = 'failed'
            s3_manager.save_pipeline_status(processed_bucket, pipeline_status)
            raise Exception(error_msg)

        # Save pipeline status
        pipeline_status.stage = 'completed'
        try:
            s3_manager.save_pipeline_status(processed_bucket, pipeline_status)
            logger.info("Pipeline status saved successfully")
        except Exception as e:
            logger.warning(f"Failed to save pipeline status: {str(e)} - continuing anyway")

        # Prepare response
        response = {
            'statusCode': 200,
            'execution_id': execution_id,
            'total_pages': len(generated_pages),
            'successful_renders': successful_renders,
            'failed_renders': failed_renders,
            'output_file': output_key
        }

        logger.info(f"HTML rendering completed: {successful_renders}/{len(generated_pages)} successful")
        return response

    except Exception as e:
        error_msg = f"HTML rendering failed: {str(e)}"
        logger.error(error_msg)

        # Try to update pipeline status
        try:
            if 'pipeline_status' in locals():
                pipeline_status.stage = 'failed'
                pipeline_status.errors.append(error_msg)
                s3_manager.save_pipeline_status(processed_bucket, pipeline_status)
        except Exception as status_error:
            logger.error(f"Failed to save pipeline status: {str(status_error)}")

        return {
            'statusCode': 500,
            'error': error_msg,
            'execution_id': event.get('execution_id', context.aws_request_id)
        }


def get_page_template() -> str:
    """
    Return the HTML template for pages.

    Returns:
        HTML template string
    """
    return """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{{ page.seo.title }}</title>
    <meta name="description" content="{{ page.seo.meta_description }}">
    <meta name="keywords" content="{{ page.seo.keywords | join(', ') }}">

    <!-- Open Graph -->
    <meta property="og:title" content="{{ page.seo.title }}">
    <meta property="og:description" content="{{ page.seo.meta_description }}">
    <meta property="og:type" content="website">
    <meta property="og:url" content="https://example.com/{{ page.seo.slug }}">

    <!-- Schema.org JSON-LD -->
    <script type="application/ld+json">
    {{ page.schema_org | tojson }}
    </script>

    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
        }
        .header {
            border-bottom: 3px solid #007cba;
            padding-bottom: 20px;
            margin-bottom: 30px;
        }
        .business-info {
            background: #f8f9fa;
            padding: 20px;
            border-radius: 8px;
            margin: 20px 0;
        }
        .contact-info {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 15px;
            margin: 15px 0;
        }
        .internal-links {
            background: #e3f2fd;
            padding: 15px;
            border-radius: 5px;
            margin: 20px 0;
        }
        .internal-links a {
            color: #1976d2;
            text-decoration: none;
            margin-right: 15px;
        }
        .internal-links a:hover {
            text-decoration: underline;
        }
        .footer {
            border-top: 1px solid #ddd;
            margin-top: 40px;
            padding-top: 20px;
            text-align: center;
            color: #666;
        }
        {% if quality_score %}
        .quality-badge {
            background: {% if quality_score >= 0.8 %}#4caf50{% elif quality_score >= 0.6 %}#ff9800{% else %}#f44336{% endif %};
            color: white;
            padding: 5px 10px;
            border-radius: 20px;
            font-size: 0.8em;
            float: right;
        }
        {% endif %}
    </style>
</head>
<body>
    <div class="header">
        <h1>{{ page.seo.h1 }}</h1>
        {% if quality_score %}
        <div class="quality-badge">Quality: {{ "%.1f" | format(quality_score * 100) }}%</div>
        {% endif %}
    </div>

    <div class="business-info">
        <h2>{{ page.business.name }}</h2>
        <div class="contact-info">
            <div><strong>Address:</strong> {{ page.business.address }}, {{ page.business.city }}, {{ page.business.state }} {{ page.business.zip_code }}</div>
            {% if page.business.phone %}
            <div><strong>Phone:</strong> <a href="tel:{{ page.business.phone }}">{{ page.business.phone }}</a></div>
            {% endif %}
            {% if page.business.website %}
            <div><strong>Website:</strong> <a href="{{ page.business.website }}" target="_blank">{{ page.business.website }}</a></div>
            {% endif %}
            {% if page.business.email %}
            <div><strong>Email:</strong> <a href="mailto:{{ page.business.email }}">{{ page.business.email }}</a></div>
            {% endif %}
        </div>
        {% if page.business.rating %}
        <div><strong>Rating:</strong> {{ page.business.rating }}/5.0
        {% if page.business.review_count %}({{ page.business.review_count }} reviews){% endif %}</div>
        {% endif %}
    </div>

    {% if page.content.introduction %}
    <div class="introduction">
        <p><strong>{{ page.content.introduction }}</strong></p>
    </div>
    {% endif %}

    <div class="main-content">
        {{ page.content.main_content | replace('\n', '</p><p>') | replace('<p></p>', '') | safe }}
    </div>

    {% if page.content.internal_links %}
    <div class="internal-links">
        <h3>Related Local Businesses</h3>
        {% for link in page.content.internal_links %}
        <a href="{{ link.url }}">{{ link.text }}</a>
        {% endfor %}
    </div>
    {% endif %}

    {% if page.content.conclusion %}
    <div class="conclusion">
        <p><em>{{ page.content.conclusion }}</em></p>
    </div>
    {% endif %}

    <div class="footer">
        <p>Generated on {{ current_date.strftime('%B %d, %Y') }}</p>
        <p>© {{ current_date.year }} Local Business Directory</p>
    </div>
</body>
</html>"""


def render_page_html(template, page_spec: PageSpec, quality_score: Optional[float] = None) -> str:
    """
    Render a PageSpec to HTML using the template.

    Args:
        template: Jinja2 template object
        page_spec: Page specification to render
        quality_score: Optional quality score from QC

    Returns:
        Rendered HTML string
    """
    return template.render(
        page=page_spec,
        quality_score=quality_score,
        current_date=datetime.now()
    )


def generate_sitemap(rendered_pages: List[Dict[str, Any]]) -> str:
    """
    Generate XML sitemap for rendered pages.

    Args:
        rendered_pages: List of rendered page information

    Returns:
        XML sitemap content
    """
    sitemap_entries = []
    base_url = "https://example.com"  # Replace with actual domain

    # Add index page
    sitemap_entries.append(f"""
    <url>
        <loc>{base_url}/</loc>
        <lastmod>{datetime.now().strftime('%Y-%m-%d')}</lastmod>
        <priority>1.0</priority>
    </url>""")

    # Add business pages
    for page in rendered_pages:
        if page.get('render_successful') and page.get('slug'):
            sitemap_entries.append(f"""
    <url>
        <loc>{base_url}/{page['slug']}</loc>
        <lastmod>{datetime.now().strftime('%Y-%m-%d')}</lastmod>
        <priority>0.8</priority>
    </url>""")

    sitemap_xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
{''.join(sitemap_entries)}
</urlset>"""

    return sitemap_xml


def generate_robots_txt() -> str:
    """
    Generate robots.txt file.

    Returns:
        robots.txt content
    """
    return """User-agent: *
Allow: /

Sitemap: https://example.com/sitemap.xml
"""


def generate_index_page(rendered_pages: List[Dict[str, Any]], template) -> str:
    """
    Generate index page listing all businesses.

    Args:
        rendered_pages: List of rendered page information
        template: Jinja2 template object

    Returns:
        HTML content for index page
    """
    successful_pages = [p for p in rendered_pages if p.get('render_successful')]

    index_template = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Local Business Directory</title>
    <meta name="description" content="Comprehensive directory of local businesses in your area">
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
        }
        .header {
            text-align: center;
            margin-bottom: 40px;
            border-bottom: 3px solid #007cba;
            padding-bottom: 20px;
        }
        .business-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
            gap: 20px;
            margin: 30px 0;
        }
        .business-card {
            border: 1px solid #ddd;
            border-radius: 8px;
            padding: 20px;
            background: #fff;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            transition: transform 0.2s;
        }
        .business-card:hover {
            transform: translateY(-2px);
            box-shadow: 0 4px 8px rgba(0,0,0,0.15);
        }
        .business-card h3 {
            margin: 0 0 10px 0;
            color: #007cba;
        }
        .business-card a {
            text-decoration: none;
            color: inherit;
        }
        .quality-score {
            float: right;
            background: #4caf50;
            color: white;
            padding: 2px 8px;
            border-radius: 10px;
            font-size: 0.8em;
        }
        .stats {
            background: #f8f9fa;
            padding: 20px;
            border-radius: 8px;
            text-align: center;
            margin: 20px 0;
        }
        .footer {
            text-align: center;
            margin-top: 40px;
            padding-top: 20px;
            border-top: 1px solid #ddd;
            color: #666;
        }
    </style>
</head>
<body>
    <div class="header">
        <h1>Local Business Directory</h1>
        <p>Discover amazing local businesses in your community</p>
    </div>

    <div class="stats">
        <h2>{{ successful_pages | length }} Local Businesses</h2>
        <p>Generated on {{ current_date.strftime('%B %d, %Y') }}</p>
    </div>

    <div class="business-grid">
        {% for page in successful_pages %}
        <div class="business-card">
            <a href="{{ page.slug }}.html">
                <h3>{{ page.title }}
                {% if page.quality_score %}
                <span class="quality-score">{{ "%.0f" | format(page.quality_score * 100) }}%</span>
                {% endif %}
                </h3>
            </a>
        </div>
        {% endfor %}
    </div>

    <div class="footer">
        <p>© {{ current_date.year }} Local Business Directory</p>
        <p>Generated by Agentic AI Content Factory</p>
    </div>
</body>
</html>"""

    jinja_env = Environment(
        loader=StringTemplateLoader(index_template),
        autoescape=select_autoescape(['html', 'xml'])
    )
    index_template_obj = jinja_env.get_template('')

    return index_template_obj.render(
        successful_pages=successful_pages,
        current_date=datetime.now()
    )