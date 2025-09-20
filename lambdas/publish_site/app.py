"""
Site publishing Lambda function.
Handles final website deployment, cache invalidation, and analytics setup.
"""

import json
import os
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
import boto3
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add common modules to path
import sys
sys.path.append('/opt/python')
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'common'))

from schemas import PipelineStatus
from s3_utils import S3Manager


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for site publishing.

    Args:
        event: Lambda event containing render results
        context: Lambda context

    Returns:
        Dictionary with publishing results
    """
    logger.info(f"Starting site publishing")

    try:
        # Initialize services
        s3_manager = S3Manager(region_name=os.environ.get('AWS_REGION', 'us-east-1'))

        # Get environment variables
        processed_bucket = os.environ['PROCESSED_BUCKET']
        website_bucket = os.environ['WEBSITE_BUCKET']

        # Extract data from previous step
        render_result = event.get('render_result', {}).get('Payload', {})
        execution_id = render_result.get('execution_id', context.aws_request_id)
        rendered_pages = render_result.get('pages', [])

        if not rendered_pages:
            raise ValueError("No rendered pages found in render results")

        logger.info(f"Publishing website with {len(rendered_pages)} pages")

        # Update pipeline status
        pipeline_status = PipelineStatus(
            execution_id=execution_id,
            stage='publish_site',
            total_businesses=len(rendered_pages),
            processed_businesses=len(rendered_pages),
            successful_pages=len(rendered_pages)
        )

        # Perform publishing tasks
        publishing_results = {}

        # 1. Configure S3 bucket for static website hosting
        try:
            configure_website_hosting(s3_manager, website_bucket)
            publishing_results['website_hosting'] = 'configured'
            logger.info("Configured S3 static website hosting")
        except Exception as e:
            error_msg = f"Failed to configure website hosting: {str(e)}"
            logger.warning(error_msg)
            publishing_results['website_hosting'] = f'error: {error_msg}'

        # 2. Generate and upload additional SEO files
        try:
            generate_seo_files(s3_manager, website_bucket, rendered_pages)
            publishing_results['seo_files'] = 'generated'
            logger.info("Generated additional SEO files")
        except Exception as e:
            error_msg = f"Failed to generate SEO files: {str(e)}"
            logger.warning(error_msg)
            publishing_results['seo_files'] = f'error: {error_msg}'

        # 3. Create site analytics and monitoring setup
        try:
            setup_analytics(s3_manager, website_bucket, rendered_pages)
            publishing_results['analytics'] = 'configured'
            logger.info("Configured site analytics")
        except Exception as e:
            error_msg = f"Failed to setup analytics: {str(e)}"
            logger.warning(error_msg)
            publishing_results['analytics'] = f'error: {error_msg}'

        # 4. Generate performance report
        try:
            performance_report = generate_performance_report(rendered_pages, execution_id)
            report_key = f"reports/performance_{execution_id}.json"
            s3_manager.upload_json(processed_bucket, report_key, performance_report)
            publishing_results['performance_report'] = report_key
            logger.info(f"Generated performance report: {report_key}")
        except Exception as e:
            error_msg = f"Failed to generate performance report: {str(e)}"
            logger.warning(error_msg)
            publishing_results['performance_report'] = f'error: {error_msg}'

        # 5. Create deployment summary
        try:
            deployment_summary = create_deployment_summary(rendered_pages, execution_id, publishing_results)
            summary_key = f"deployments/summary_{execution_id}.json"
            s3_manager.upload_json(processed_bucket, summary_key, deployment_summary)
            publishing_results['deployment_summary'] = summary_key
            logger.info(f"Created deployment summary: {summary_key}")
        except Exception as e:
            error_msg = f"Failed to create deployment summary: {str(e)}"
            logger.warning(error_msg)
            publishing_results['deployment_summary'] = f'error: {error_msg}'

        # Get website URL
        website_url = get_website_url(website_bucket)

        # Update pipeline status
        pipeline_status.stage = 'completed'
        pipeline_status.end_time = datetime.utcnow()
        s3_manager.save_pipeline_status(processed_bucket, pipeline_status)

        # Prepare response
        response = {
            'statusCode': 200,
            'execution_id': execution_id,
            'website_url': website_url,
            'total_pages': len(rendered_pages),
            'publishing_results': publishing_results,
            'deployment_timestamp': datetime.utcnow().isoformat(),
            'pages_published': [
                {
                    'business_name': page['business_name'],
                    'slug': page['slug'],
                    'url': f"{website_url}/pages/{page['slug']}.html"
                }
                for page in rendered_pages
            ]
        }

        logger.info(f"Site publishing completed successfully: {website_url}")
        return response

    except Exception as e:
        error_msg = f"Site publishing failed: {str(e)}"
        logger.error(error_msg)

        # Try to update pipeline status
        try:
            pipeline_status = PipelineStatus(
                execution_id=event.get('execution_id', context.aws_request_id),
                stage='failed',
                total_businesses=0,
                end_time=datetime.utcnow()
            )
            pipeline_status.errors.append(error_msg)
            s3_manager.save_pipeline_status(processed_bucket, pipeline_status)
        except:
            logger.error("Failed to save pipeline status")

        return {
            'statusCode': 500,
            'error': error_msg,
            'execution_id': event.get('execution_id', context.aws_request_id)
        }


def configure_website_hosting(s3_manager: S3Manager, website_bucket: str):
    """
    Configure S3 bucket for static website hosting.

    Args:
        s3_manager: S3 manager instance
        website_bucket: Website S3 bucket name
    """
    try:
        # Configure website hosting
        website_config = {
            'IndexDocument': {'Suffix': 'index.html'},
            'ErrorDocument': {'Key': 'error.html'}
        }

        s3_manager.s3_client.put_bucket_website(
            Bucket=website_bucket,
            WebsiteConfiguration=website_config
        )

        # Create a simple error page if it doesn't exist
        error_page_content = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Page Not Found - Local Business Directory</title>
    <link rel="stylesheet" href="styles.css">
</head>
<body>
    <header>
        <h1>Page Not Found</h1>
    </header>
    <main>
        <p>Sorry, the page you're looking for doesn't exist.</p>
        <p><a href="index.html">Return to Business Directory</a></p>
    </main>
</body>
</html>"""

        s3_manager.upload_text(website_bucket, 'error.html', error_page_content, 'text/html')

        logger.info(f"Configured static website hosting for {website_bucket}")

    except ClientError as e:
        if e.response['Error']['Code'] != 'NoSuchBucket':
            raise


def generate_seo_files(s3_manager: S3Manager, website_bucket: str, pages: List[Dict[str, Any]]):
    """
    Generate additional SEO and metadata files.

    Args:
        s3_manager: S3 manager instance
        website_bucket: Website S3 bucket name
        pages: List of rendered pages
    """
    # Generate JSON feed for the site
    site_feed = {
        "version": "https://jsonfeed.org/version/1.1",
        "title": "Local Business Directory",
        "home_page_url": get_website_url(website_bucket),
        "feed_url": f"{get_website_url(website_bucket)}/feed.json",
        "description": "Local business listings and information",
        "items": [
            {
                "id": page['business_id'],
                "url": f"{get_website_url(website_bucket)}/pages/{page['slug']}.html",
                "title": page['business_name'],
                "date_published": datetime.utcnow().isoformat(),
                "summary": f"Information about {page['business_name']}"
            }
            for page in pages
        ]
    }

    s3_manager.upload_json(website_bucket, 'feed.json', site_feed)

    # Generate manifest.json for PWA capabilities
    manifest = {
        "name": "Local Business Directory",
        "short_name": "Business Directory",
        "description": "Local business listings and information",
        "start_url": "/",
        "display": "standalone",
        "background_color": "#ffffff",
        "theme_color": "#007bff",
        "icons": [
            {
                "src": "/icon-192.png",
                "sizes": "192x192",
                "type": "image/png"
            }
        ]
    }

    s3_manager.upload_json(website_bucket, 'manifest.json', manifest)

    # Generate humans.txt
    humans_txt = f"""/* TEAM */
Generated by: Agentic Local SEO Content Factory
Location: AWS Lambda

/* SITE */
Last update: {datetime.utcnow().strftime('%Y/%m/%d')}
Language: English
Doctype: HTML5
Standards: HTML5, CSS3
Components: Jinja2, Amazon Bedrock, AWS Lambda
Software: Python 3.12
"""

    s3_manager.upload_text(website_bucket, 'humans.txt', humans_txt, 'text/plain')

    logger.info("Generated additional SEO files")


def setup_analytics(s3_manager: S3Manager, website_bucket: str, pages: List[Dict[str, Any]]):
    """
    Setup basic analytics and monitoring.

    Args:
        s3_manager: S3 manager instance
        website_bucket: Website S3 bucket name
        pages: List of rendered pages
    """
    # Create a simple analytics snippet (placeholder for real analytics)
    analytics_js = f"""
// Basic analytics tracking
(function() {{
    // Track page views
    var pageData = {{
        url: window.location.pathname,
        title: document.title,
        timestamp: new Date().toISOString(),
        userAgent: navigator.userAgent
    }};

    // In a real implementation, this would send data to your analytics service
    console.log('Page view:', pageData);

    // Track clicks on business links
    document.addEventListener('click', function(e) {{
        if (e.target.tagName === 'A' && e.target.href.includes('/pages/')) {{
            console.log('Business page click:', e.target.href);
        }}
    }});
}})();
"""

    s3_manager.upload_text(website_bucket, 'analytics.js', analytics_js, 'application/javascript')

    # Create performance monitoring data
    performance_data = {
        'site_generated': datetime.utcnow().isoformat(),
        'total_pages': len(pages),
        'average_quality_score': sum(page.get('quality_score', 0) for page in pages) / len(pages) if pages else 0,
        'pages_by_quality': {
            'high': len([p for p in pages if p.get('quality_score', 0) >= 0.8]),
            'medium': len([p for p in pages if 0.6 <= p.get('quality_score', 0) < 0.8]),
            'low': len([p for p in pages if p.get('quality_score', 0) < 0.6])
        }
    }

    s3_manager.upload_json(website_bucket, 'performance.json', performance_data)

    logger.info("Setup analytics and monitoring")


def generate_performance_report(pages: List[Dict[str, Any]], execution_id: str) -> Dict[str, Any]:
    """
    Generate comprehensive performance report.

    Args:
        pages: List of rendered pages
        execution_id: Pipeline execution ID

    Returns:
        Performance report dictionary
    """
    quality_scores = [page.get('quality_score', 0) for page in pages]

    report = {
        'execution_id': execution_id,
        'generated_at': datetime.utcnow().isoformat(),
        'summary': {
            'total_pages': len(pages),
            'average_quality_score': sum(quality_scores) / len(quality_scores) if quality_scores else 0,
            'min_quality_score': min(quality_scores) if quality_scores else 0,
            'max_quality_score': max(quality_scores) if quality_scores else 0
        },
        'quality_distribution': {
            'excellent': len([s for s in quality_scores if s >= 0.9]),
            'good': len([s for s in quality_scores if 0.8 <= s < 0.9]),
            'fair': len([s for s in quality_scores if 0.6 <= s < 0.8]),
            'poor': len([s for s in quality_scores if s < 0.6])
        },
        'top_performing_pages': sorted(
            [{'business_name': p['business_name'], 'quality_score': p.get('quality_score', 0)}
             for p in pages],
            key=lambda x: x['quality_score'],
            reverse=True
        )[:10],
        'recommendations': generate_recommendations(pages)
    }

    return report


def create_deployment_summary(pages: List[Dict[str, Any]], execution_id: str,
                             publishing_results: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create deployment summary document.

    Args:
        pages: List of rendered pages
        execution_id: Pipeline execution ID
        publishing_results: Results from publishing tasks

    Returns:
        Deployment summary dictionary
    """
    summary = {
        'deployment_info': {
            'execution_id': execution_id,
            'timestamp': datetime.utcnow().isoformat(),
            'total_pages': len(pages),
            'successful_tasks': len([r for r in publishing_results.values() if not str(r).startswith('error:')])
        },
        'pages_deployed': pages,
        'publishing_results': publishing_results,
        'next_steps': [
            'Monitor website performance and user engagement',
            'Set up analytics tracking for detailed insights',
            'Schedule regular content updates and regeneration',
            'Consider adding structured data validation',
            'Implement automated SEO monitoring'
        ],
        'technical_details': {
            'generator': 'Agentic Local SEO Content Factory',
            'version': '1.0',
            'aws_region': os.environ.get('AWS_REGION', 'us-east-1'),
            'content_model': 'Claude-3-Haiku',
            'quality_model': 'Claude-3-Sonnet'
        }
    }

    return summary


def generate_recommendations(pages: List[Dict[str, Any]]) -> List[str]:
    """
    Generate recommendations based on page performance.

    Args:
        pages: List of rendered pages

    Returns:
        List of recommendation strings
    """
    recommendations = []
    quality_scores = [page.get('quality_score', 0) for page in pages]

    if not quality_scores:
        return ["No pages to analyze"]

    avg_quality = sum(quality_scores) / len(quality_scores)

    if avg_quality < 0.7:
        recommendations.append("Consider improving content generation prompts to increase overall quality")

    if len([s for s in quality_scores if s < 0.6]) > len(pages) * 0.2:
        recommendations.append("More than 20% of pages have low quality scores - review generation process")

    if len(pages) < 50:
        recommendations.append("Consider generating more business pages for better SEO coverage")

    recommendations.extend([
        "Implement regular content audits and updates",
        "Add customer review integration for enhanced credibility",
        "Consider implementing structured data validation",
        "Monitor page loading speeds and optimize as needed",
        "Set up automated backlink monitoring"
    ])

    return recommendations[:10]  # Limit to top 10 recommendations


def get_website_url(website_bucket: str) -> str:
    """
    Get the website URL for the S3 bucket.

    Args:
        website_bucket: Website S3 bucket name

    Returns:
        Website URL
    """
    # In production, this might be a CloudFront distribution or custom domain
    region = os.environ.get('AWS_REGION', 'us-east-1')
    if region == 'us-east-1':
        return f"https://{website_bucket}.s3-website-us-east-1.amazonaws.com"
    else:
        return f"https://{website_bucket}.s3-website-{region}.amazonaws.com"


def invalidate_cdn_cache(distribution_id: str = None):
    """
    Invalidate CloudFront cache if distribution is configured.

    Args:
        distribution_id: CloudFront distribution ID (optional)
    """
    if not distribution_id:
        logger.info("No CloudFront distribution configured, skipping cache invalidation")
        return

    try:
        cloudfront = boto3.client('cloudfront')

        response = cloudfront.create_invalidation(
            DistributionId=distribution_id,
            InvalidationBatch={
                'Paths': {
                    'Quantity': 1,
                    'Items': ['/*']
                },
                'CallerReference': f"invalidation-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
            }
        )

        logger.info(f"Created CloudFront invalidation: {response['Invalidation']['Id']}")

    except Exception as e:
        logger.warning(f"Failed to invalidate CloudFront cache: {str(e)}")