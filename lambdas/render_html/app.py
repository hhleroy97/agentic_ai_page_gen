"""
HTML rendering Lambda function.
Converts PageSpec objects to static HTML files using Jinja2 templates.
"""

import json
import os
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
from jinja2 import Environment, FileSystemLoader, Template
import xml.etree.ElementTree as ET

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add common modules to path
import sys
sys.path.append('/opt/python')
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'common'))

from schemas import PageSpec, Business
from s3_utils import S3Manager


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for HTML rendering.

    Args:
        event: Lambda event containing processed businesses
        context: Lambda context

    Returns:
        Dictionary with rendering results
    """
    logger.info(f"Starting HTML rendering")

    try:
        # Initialize services
        s3_manager = S3Manager(region_name=os.environ.get('AWS_REGION', 'us-east-1'))

        # Get environment variables
        processed_bucket = os.environ['PROCESSED_BUCKET']
        website_bucket = os.environ['WEBSITE_BUCKET']

        # Extract data from previous steps
        processed_businesses = event.get('processed_businesses', [])
        execution_id = event.get('execution_id', context.aws_request_id)

        if not processed_businesses:
            raise ValueError("No processed businesses found in event")

        logger.info(f"Rendering HTML for {len(processed_businesses)} businesses")

        # Load site templates
        template_env = setup_jinja_environment()

        # Process each business and render HTML
        rendered_pages = []
        rendering_errors = []

        for business_result in processed_businesses:
            try:
                # Extract PageSpec from business result
                page_spec_data = business_result.get('qc_result', {}).get('Payload', {}).get('page_spec')
                if not page_spec_data:
                    # Try alternative path
                    page_spec_data = business_result.get('generate_result', {}).get('Payload', {}).get('page_spec')

                if not page_spec_data:
                    logger.warning(f"No page spec found for business result")
                    continue

                page_spec = PageSpec(**page_spec_data)

                # Render HTML page
                html_content = render_business_page(template_env, page_spec)

                if html_content:
                    # Save HTML to website bucket
                    html_key = f"pages/{page_spec.seo.slug}.html"
                    if s3_manager.upload_text(website_bucket, html_key, html_content, 'text/html'):
                        rendered_pages.append({
                            'business_id': page_spec.business.business_id,
                            'business_name': page_spec.business.name,
                            'slug': page_spec.seo.slug,
                            'html_key': html_key,
                            'quality_score': page_spec.quality_score
                        })
                        logger.info(f"Rendered page for {page_spec.business.name}")
                    else:
                        raise Exception(f"Failed to upload HTML for {page_spec.business.name}")

            except Exception as e:
                error_msg = f"Failed to render business page: {str(e)}"
                rendering_errors.append(error_msg)
                logger.warning(error_msg)

        # Generate and upload sitemap
        try:
            sitemap_content = generate_sitemap(rendered_pages, website_bucket)
            s3_manager.upload_text(website_bucket, 'sitemap.xml', sitemap_content, 'application/xml')
            logger.info("Generated and uploaded sitemap.xml")
        except Exception as e:
            logger.warning(f"Failed to generate sitemap: {str(e)}")

        # Generate and upload robots.txt
        try:
            robots_content = generate_robots_txt(website_bucket)
            s3_manager.upload_text(website_bucket, 'robots.txt', robots_content, 'text/plain')
            logger.info("Generated and uploaded robots.txt")
        except Exception as e:
            logger.warning(f"Failed to generate robots.txt: {str(e)}")

        # Generate index page listing all businesses
        try:
            index_content = generate_index_page(template_env, rendered_pages)
            s3_manager.upload_text(website_bucket, 'index.html', index_content, 'text/html')
            logger.info("Generated and uploaded index.html")
        except Exception as e:
            logger.warning(f"Failed to generate index page: {str(e)}")

        # Copy CSS and other static assets
        try:
            copy_static_assets(s3_manager, website_bucket)
            logger.info("Copied static assets")
        except Exception as e:
            logger.warning(f"Failed to copy static assets: {str(e)}")

        # Prepare response
        response = {
            'statusCode': 200,
            'execution_id': execution_id,
            'total_businesses': len(processed_businesses),
            'rendered_pages': len(rendered_pages),
            'rendering_errors': len(rendering_errors),
            'pages': rendered_pages,
            'errors': rendering_errors[:10]  # Limit error list
        }

        logger.info(f"HTML rendering completed: {len(rendered_pages)} pages rendered")
        return response

    except Exception as e:
        error_msg = f"HTML rendering failed: {str(e)}"
        logger.error(error_msg)

        return {
            'statusCode': 500,
            'error': error_msg,
            'execution_id': event.get('execution_id', context.aws_request_id)
        }


def setup_jinja_environment() -> Environment:
    """
    Set up Jinja2 template environment.

    Returns:
        Configured Jinja2 Environment
    """
    # In a real deployment, templates would be in /opt/templates or loaded from S3
    # For this demo, we'll use inline templates

    env = Environment(
        loader=FileSystemLoader(['/tmp', '.']),  # Fallback paths
        autoescape=True,
        trim_blocks=True,
        lstrip_blocks=True
    )

    # Add custom filters
    env.filters['format_phone'] = format_phone_number
    env.filters['json_ld'] = json_ld_filter

    return env


def render_business_page(template_env: Environment, page_spec: PageSpec) -> Optional[str]:
    """
    Render HTML page for a business using templates.

    Args:
        template_env: Jinja2 environment
        page_spec: Page specification to render

    Returns:
        Rendered HTML content or None if error
    """
    try:
        # Create template content (in production, this would be loaded from files)
        template_content = get_business_page_template()

        template = template_env.from_string(template_content)

        # Prepare template context
        context = {
            'page': page_spec,
            'business': page_spec.business,
            'seo': page_spec.seo,
            'content': page_spec.content,
            'jsonld': page_spec.jsonld,
            'internal_links': page_spec.internal_links,
            'generated_at': datetime.utcnow(),
            'site_name': 'Local Business Directory'
        }

        # Render template
        rendered_html = template.render(**context)

        return rendered_html

    except Exception as e:
        logger.error(f"Failed to render business page: {str(e)}")
        return None


def generate_sitemap(pages: List[Dict[str, Any]], website_bucket: str) -> str:
    """
    Generate XML sitemap for all rendered pages.

    Args:
        pages: List of rendered page information
        website_bucket: Website S3 bucket name

    Returns:
        XML sitemap content
    """
    # Get the website URL (in production, this would be from CloudFront or custom domain)
    base_url = f"https://{website_bucket}.s3-website-us-east-1.amazonaws.com"

    # Create sitemap XML
    urlset = ET.Element('urlset')
    urlset.set('xmlns', 'http://www.sitemaps.org/schemas/sitemap/0.9')

    # Add index page
    url_elem = ET.SubElement(urlset, 'url')
    ET.SubElement(url_elem, 'loc').text = base_url
    ET.SubElement(url_elem, 'lastmod').text = datetime.utcnow().strftime('%Y-%m-%d')
    ET.SubElement(url_elem, 'changefreq').text = 'weekly'
    ET.SubElement(url_elem, 'priority').text = '1.0'

    # Add business pages
    for page in pages:
        url_elem = ET.SubElement(urlset, 'url')
        ET.SubElement(url_elem, 'loc').text = f"{base_url}/pages/{page['slug']}.html"
        ET.SubElement(url_elem, 'lastmod').text = datetime.utcnow().strftime('%Y-%m-%d')
        ET.SubElement(url_elem, 'changefreq').text = 'monthly'
        ET.SubElement(url_elem, 'priority').text = '0.8'

    # Convert to string
    xml_str = ET.tostring(urlset, encoding='unicode', method='xml')
    return f'<?xml version="1.0" encoding="UTF-8"?>\n{xml_str}'


def generate_robots_txt(website_bucket: str) -> str:
    """
    Generate robots.txt file.

    Args:
        website_bucket: Website S3 bucket name

    Returns:
        robots.txt content
    """
    base_url = f"https://{website_bucket}.s3-website-us-east-1.amazonaws.com"

    return f"""User-agent: *
Allow: /

Sitemap: {base_url}/sitemap.xml

# Generated by Agentic Local SEO Content Factory
# {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC
"""


def generate_index_page(template_env: Environment, pages: List[Dict[str, Any]]) -> str:
    """
    Generate index page listing all businesses.

    Args:
        template_env: Jinja2 environment
        pages: List of rendered page information

    Returns:
        Rendered index page HTML
    """
    template_content = get_index_page_template()
    template = template_env.from_string(template_content)

    # Sort pages by business name
    sorted_pages = sorted(pages, key=lambda x: x['business_name'])

    context = {
        'pages': sorted_pages,
        'total_pages': len(pages),
        'generated_at': datetime.utcnow(),
        'site_name': 'Local Business Directory'
    }

    return template.render(**context)


def copy_static_assets(s3_manager: S3Manager, website_bucket: str):
    """
    Copy static assets (CSS, JS) to website bucket.

    Args:
        s3_manager: S3 manager instance
        website_bucket: Website S3 bucket name
    """
    # Create CSS content (in production, this would be copied from files)
    css_content = get_site_css()

    # Upload CSS
    s3_manager.upload_text(website_bucket, 'styles.css', css_content, 'text/css')


# Template content functions (in production, these would be loaded from files)

def get_business_page_template() -> str:
    """Get the business page HTML template"""
    return """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{{ seo.title }}</title>
    <meta name="description" content="{{ seo.meta_description }}">
    <meta name="keywords" content="{{ seo.keywords | join(', ') }}">
    {% if seo.canonical_url %}
    <link rel="canonical" href="{{ seo.canonical_url }}">
    {% endif %}
    <link rel="stylesheet" href="../styles.css">

    <!-- JSON-LD Structured Data -->
    <script type="application/ld+json">
    {{ jsonld | json_ld }}
    </script>
</head>
<body>
    <header>
        <nav>
            <a href="../index.html">← Back to Directory</a>
        </nav>
    </header>

    <main>
        <article>
            <header>
                <h1>{{ seo.h1 }}</h1>
                <div class="business-info">
                    <p><strong>Category:</strong> {{ business.category }}</p>
                    <p><strong>Location:</strong> {{ business.city }}, {{ business.state }}</p>
                    {% if business.rating %}
                    <p><strong>Rating:</strong> {{ business.rating }}/5.0 ({{ business.review_count }} reviews)</p>
                    {% endif %}
                </div>
            </header>

            <section class="introduction">
                {{ content.introduction }}
            </section>

            <section class="main-content">
                {{ content.main_content | replace('\n', '<br>') | safe }}
            </section>

            {% if content.services_section %}
            <section class="services">
                <h2>Our Services</h2>
                {{ content.services_section | replace('\n', '<br>') | safe }}
            </section>
            {% endif %}

            {% if content.location_section %}
            <section class="location">
                <h2>Location & Service Area</h2>
                {{ content.location_section | replace('\n', '<br>') | safe }}
            </section>
            {% endif %}

            <section class="contact">
                <h2>Contact Information</h2>
                <div class="contact-details">
                    <p><strong>Address:</strong> {{ business.address }}, {{ business.city }}, {{ business.state }} {{ business.zip_code }}</p>
                    {% if business.phone %}
                    <p><strong>Phone:</strong> <a href="tel:{{ business.phone | replace(' ', '') | replace('(', '') | replace(')', '') | replace('-', '') }}">{{ business.phone | format_phone }}</a></p>
                    {% endif %}
                    {% if business.website %}
                    <p><strong>Website:</strong> <a href="{{ business.website }}" target="_blank">{{ business.website }}</a></p>
                    {% endif %}
                    {% if business.email %}
                    <p><strong>Email:</strong> <a href="mailto:{{ business.email }}">{{ business.email }}</a></p>
                    {% endif %}
                </div>
            </section>

            {% if internal_links %}
            <section class="related-businesses">
                <h2>Related Local Businesses</h2>
                <ul>
                {% for link in internal_links %}
                    <li><a href="{{ link.url }}">{{ link.anchor_text }}</a></li>
                {% endfor %}
                </ul>
            </section>
            {% endif %}

            <section class="conclusion">
                {{ content.conclusion }}
            </section>
        </article>
    </main>

    <footer>
        <p>&copy; {{ generated_at.year }} {{ site_name }}. Generated on {{ generated_at.strftime('%B %d, %Y') }}.</p>
    </footer>
</body>
</html>"""


def get_index_page_template() -> str:
    """Get the index page HTML template"""
    return """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Local Business Directory</title>
    <meta name="description" content="Discover local businesses in your area. Professional services, restaurants, automotive, and more.">
    <link rel="stylesheet" href="styles.css">
</head>
<body>
    <header>
        <h1>Local Business Directory</h1>
        <p>Discover {{ total_pages }} local businesses in your area</p>
    </header>

    <main>
        <section class="business-grid">
            {% for page in pages %}
            <div class="business-card">
                <h2><a href="pages/{{ page.slug }}.html">{{ page.business_name }}</a></h2>
                <p class="quality-score">Quality Score: {{ page.quality_score }}/1.0</p>
                <a href="pages/{{ page.slug }}.html" class="read-more">View Details →</a>
            </div>
            {% endfor %}
        </section>
    </main>

    <footer>
        <p>&copy; {{ generated_at.year }} {{ site_name }}. Generated on {{ generated_at.strftime('%B %d, %Y') }}.</p>
        <p>{{ total_pages }} businesses • Powered by Agentic Local SEO Content Factory</p>
    </footer>
</body>
</html>"""


def get_site_css() -> str:
    """Get the site CSS content"""
    return """/* Local Business Directory Styles */

* {
    margin: 0;
    padding: 0;
    box-sizing: border-box;
}

body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    line-height: 1.6;
    color: #333;
    background-color: #f8f9fa;
}

header {
    background: #fff;
    padding: 1rem 0;
    border-bottom: 1px solid #dee2e6;
    margin-bottom: 2rem;
}

header h1 {
    text-align: center;
    color: #2c3e50;
    margin-bottom: 0.5rem;
}

header p {
    text-align: center;
    color: #6c757d;
}

nav a {
    color: #007bff;
    text-decoration: none;
    margin-left: 1rem;
}

nav a:hover {
    text-decoration: underline;
}

main {
    max-width: 1200px;
    margin: 0 auto;
    padding: 0 1rem;
}

article {
    background: #fff;
    padding: 2rem;
    border-radius: 8px;
    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    margin-bottom: 2rem;
}

.business-info {
    background: #e9ecef;
    padding: 1rem;
    border-radius: 4px;
    margin: 1rem 0;
}

.business-info p {
    margin: 0.25rem 0;
}

section {
    margin: 2rem 0;
}

section h2 {
    color: #2c3e50;
    margin-bottom: 1rem;
    border-bottom: 2px solid #007bff;
    padding-bottom: 0.5rem;
}

.contact-details {
    background: #f8f9fa;
    padding: 1rem;
    border-radius: 4px;
    border-left: 4px solid #007bff;
}

.related-businesses ul {
    list-style: none;
    padding: 0;
}

.related-businesses li {
    margin: 0.5rem 0;
}

.related-businesses a {
    color: #007bff;
    text-decoration: none;
    padding: 0.25rem 0;
    display: inline-block;
}

.related-businesses a:hover {
    text-decoration: underline;
}

/* Index page styles */
.business-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
    gap: 1.5rem;
    margin: 2rem 0;
}

.business-card {
    background: #fff;
    padding: 1.5rem;
    border-radius: 8px;
    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    transition: transform 0.2s ease;
}

.business-card:hover {
    transform: translateY(-2px);
}

.business-card h2 {
    margin-bottom: 0.5rem;
}

.business-card h2 a {
    color: #2c3e50;
    text-decoration: none;
}

.business-card h2 a:hover {
    color: #007bff;
}

.quality-score {
    color: #6c757d;
    font-size: 0.9rem;
    margin: 0.5rem 0;
}

.read-more {
    color: #007bff;
    text-decoration: none;
    font-weight: 500;
}

.read-more:hover {
    text-decoration: underline;
}

footer {
    text-align: center;
    padding: 2rem 0;
    margin-top: 3rem;
    border-top: 1px solid #dee2e6;
    color: #6c757d;
    font-size: 0.9rem;
}

/* Responsive design */
@media (max-width: 768px) {
    main {
        padding: 0 0.5rem;
    }

    article {
        padding: 1rem;
    }

    .business-grid {
        grid-template-columns: 1fr;
    }
}"""


# Custom Jinja2 filters

def format_phone_number(phone: str) -> str:
    """Format phone number for display"""
    if not phone:
        return phone
    return phone  # Already formatted in data processing


def json_ld_filter(jsonld_data: Dict[str, Any]) -> str:
    """Convert JSON-LD data to formatted JSON string"""
    return json.dumps(jsonld_data, indent=2)