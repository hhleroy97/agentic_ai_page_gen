"""
Site Publishing Lambda function.
Finalizes the website, updates metadata, and handles post-publication tasks.
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
from schemas import Business, PageSpec, PipelineStatus
from s3_utils import S3Manager


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for site publishing.

    Args:
        event: Lambda event (Step Functions input)
        context: Lambda context

    Returns:
        Dictionary with publishing results
    """
    logger.info(f"Starting site publishing with event: {json.dumps(event, default=str)}")

    try:
        # Initialize S3 manager
        s3_manager = S3Manager(region_name=os.environ.get('AWS_REGION', 'us-east-1'))

        # Get bucket names from environment
        processed_bucket = os.environ['PROCESSED_BUCKET']
        website_bucket = os.environ['WEBSITE_BUCKET']

        # Extract parameters from event
        execution_id = event.get('execution_id', context.aws_request_id)
        input_file = event.get('output_file')  # From render_html step

        if not input_file:
            raise Exception("No input file specified from previous step")

        # Initialize pipeline status
        pipeline_status = PipelineStatus(
            execution_id=execution_id,
            stage='publish_site',
            total_businesses=0
        )

        # Download rendering results data
        logger.info(f"Downloading rendering results: s3://{processed_bucket}/{input_file}")
        render_data = s3_manager.download_json(processed_bucket, input_file)

        if not render_data:
            error_msg = f"Failed to download rendering results from {input_file}"
            logger.error(error_msg)
            pipeline_status.errors.append(error_msg)
            pipeline_status.stage = 'failed'
            s3_manager.save_pipeline_status(processed_bucket, pipeline_status)
            raise Exception(error_msg)

        rendered_pages = render_data.get('rendered_pages', [])
        successful_renders = render_data.get('successful_renders', 0)
        failed_renders = render_data.get('failed_renders', 0)
        pipeline_status.total_businesses = len(rendered_pages)

        logger.info(f"Publishing site with {successful_renders} successful pages")

        # Generate site metadata
        site_metadata = generate_site_metadata(rendered_pages, execution_id)

        # Upload site metadata
        metadata_key = "site-metadata.json"
        if not s3_manager.upload_json(website_bucket, metadata_key, site_metadata):
            logger.warning("Failed to upload site metadata")

        # Generate and upload analytics tracking (optional)
        analytics_code = generate_analytics_code()
        analytics_key = "analytics.js"
        if analytics_code:
            s3_manager.upload_text(website_bucket, analytics_key, analytics_code)

        # Generate and upload CSS
        css_content = generate_global_css()
        css_key = "styles.css"
        s3_manager.upload_text(website_bucket, css_key, css_content)

        # Update pipeline status with final results
        pipeline_status.processed_businesses = successful_renders

        # Generate final execution report
        execution_report = generate_execution_report(
            execution_id, rendered_pages, successful_renders, failed_renders
        )

        # Save execution report
        report_key = f"reports/execution_report_{execution_id}.json"
        s3_manager.upload_json(processed_bucket, report_key, execution_report)

        # Get website URL
        website_url = get_website_url(website_bucket)

        # Log final results
        logger.info(f"Site publication completed successfully")
        logger.info(f"Website URL: {website_url}")
        logger.info(f"Total pages: {len(rendered_pages)}")
        logger.info(f"Successful: {successful_renders}")
        logger.info(f"Failed: {failed_renders}")

        # Save pipeline status
        pipeline_status.stage = 'completed'
        try:
            s3_manager.save_pipeline_status(processed_bucket, pipeline_status)
            logger.info("Pipeline status saved successfully")
        except Exception as e:
            logger.warning(f"Failed to save pipeline status: {str(e)} - continuing anyway")

        # Prepare final response
        response = {
            'statusCode': 200,
            'execution_id': execution_id,
            'website_url': website_url,
            'total_pages': len(rendered_pages),
            'successful_renders': successful_renders,
            'failed_renders': failed_renders,
            'site_metadata': site_metadata,
            'execution_report_file': report_key
        }

        logger.info("Site publishing completed successfully")
        return response

    except Exception as e:
        error_msg = f"Site publishing failed: {str(e)}"
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


def generate_site_metadata(rendered_pages: List[Dict[str, Any]], execution_id: str) -> Dict[str, Any]:
    """
    Generate comprehensive site metadata.

    Args:
        rendered_pages: List of rendered page information
        execution_id: Pipeline execution ID

    Returns:
        Site metadata dictionary
    """
    successful_pages = [p for p in rendered_pages if p.get('render_successful')]

    # Calculate quality statistics
    quality_scores = [p.get('quality_score', 0.0) for p in successful_pages if p.get('quality_score')]
    avg_quality = sum(quality_scores) / len(quality_scores) if quality_scores else 0.0

    # Categorize pages by quality
    high_quality = len([s for s in quality_scores if s >= 0.8])
    medium_quality = len([s for s in quality_scores if 0.6 <= s < 0.8])
    low_quality = len([s for s in quality_scores if s < 0.6])

    metadata = {
        'site_info': {
            'generation_date': datetime.now().isoformat(),
            'execution_id': execution_id,
            'generator': 'Agentic Local SEO Content Factory v1.0',
            'total_pages': len(rendered_pages),
            'successful_pages': len(successful_pages),
            'failed_pages': len(rendered_pages) - len(successful_pages)
        },
        'quality_metrics': {
            'average_quality_score': round(avg_quality, 3),
            'high_quality_pages': high_quality,
            'medium_quality_pages': medium_quality,
            'low_quality_pages': low_quality,
            'quality_distribution': {
                'excellent': high_quality,
                'good': medium_quality,
                'needs_improvement': low_quality
            }
        },
        'pages': [
            {
                'business_id': page.get('business_id'),
                'slug': page.get('slug'),
                'title': page.get('title'),
                'html_file': page.get('html_file'),
                'quality_score': page.get('quality_score'),
                'render_successful': page.get('render_successful', False)
            }
            for page in rendered_pages
        ],
        'seo_info': {
            'sitemap_url': '/sitemap.xml',
            'robots_txt_url': '/robots.txt',
            'index_page': '/index.html',
            'total_seo_optimized_pages': len(successful_pages)
        },
        'technical_details': {
            'aws_region': os.environ.get('AWS_REGION', 'us-east-1'),
            'website_bucket': os.environ.get('WEBSITE_BUCKET'),
            'content_format': 'HTML5',
            'responsive_design': True,
            'schema_org_markup': True
        }
    }

    return metadata


def generate_execution_report(execution_id: str, rendered_pages: List[Dict[str, Any]],
                            successful: int, failed: int) -> Dict[str, Any]:
    """
    Generate comprehensive execution report.

    Args:
        execution_id: Pipeline execution ID
        rendered_pages: List of rendered page information
        successful: Number of successful renders
        failed: Number of failed renders

    Returns:
        Execution report dictionary
    """
    report = {
        'execution_summary': {
            'execution_id': execution_id,
            'completion_time': datetime.now().isoformat(),
            'total_duration_estimated': '15-30 minutes',  # Typical pipeline duration
            'overall_status': 'success' if failed == 0 else 'partial_success' if successful > 0 else 'failed'
        },
        'pipeline_stages': {
            'ingest_raw': {'status': 'completed', 'description': 'Business data ingestion and validation'},
            'clean_transform': {'status': 'completed', 'description': 'Data cleaning and transformation'},
            'agent_generate': {'status': 'completed', 'description': 'AI content generation'},
            'agent_qc': {'status': 'completed', 'description': 'Quality control and validation'},
            'render_html': {'status': 'completed', 'description': 'HTML rendering and template processing'},
            'publish_site': {'status': 'completed', 'description': 'Site publishing and finalization'}
        },
        'results': {
            'total_businesses_processed': len(rendered_pages),
            'successful_pages_generated': successful,
            'failed_page_generations': failed,
            'success_rate': round((successful / len(rendered_pages)) * 100, 1) if rendered_pages else 0
        },
        'quality_analysis': analyze_page_quality(rendered_pages),
        'recommendations': generate_recommendations(rendered_pages, successful, failed),
        'next_steps': [
            'Review generated content for accuracy and relevance',
            'Consider updating business data for failed generations',
            'Monitor website performance and SEO rankings',
            'Plan future content updates and regeneration cycles'
        ]
    }

    return report


def analyze_page_quality(rendered_pages: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Analyze the quality of generated pages.

    Args:
        rendered_pages: List of rendered page information

    Returns:
        Quality analysis results
    """
    successful_pages = [p for p in rendered_pages if p.get('render_successful')]
    quality_scores = [p.get('quality_score', 0.0) for p in successful_pages if p.get('quality_score')]

    if not quality_scores:
        return {'message': 'No quality scores available for analysis'}

    analysis = {
        'total_pages_analyzed': len(quality_scores),
        'average_quality': round(sum(quality_scores) / len(quality_scores), 3),
        'highest_quality': round(max(quality_scores), 3),
        'lowest_quality': round(min(quality_scores), 3),
        'quality_distribution': {
            'excellent_90_100': len([s for s in quality_scores if s >= 0.9]),
            'good_80_89': len([s for s in quality_scores if 0.8 <= s < 0.9]),
            'average_70_79': len([s for s in quality_scores if 0.7 <= s < 0.8]),
            'below_average_60_69': len([s for s in quality_scores if 0.6 <= s < 0.7]),
            'poor_below_60': len([s for s in quality_scores if s < 0.6])
        },
        'recommendations_based_on_quality': []
    }

    # Add quality-based recommendations
    avg_quality = analysis['average_quality']
    if avg_quality >= 0.85:
        analysis['recommendations_based_on_quality'].append('Excellent quality! Consider this as a template for future generations.')
    elif avg_quality >= 0.75:
        analysis['recommendations_based_on_quality'].append('Good quality overall. Minor improvements could enhance SEO performance.')
    elif avg_quality >= 0.65:
        analysis['recommendations_based_on_quality'].append('Average quality. Consider reviewing content generation prompts.')
    else:
        analysis['recommendations_based_on_quality'].append('Below average quality. Recommend regenerating content with improved prompts.')

    return analysis


def generate_recommendations(rendered_pages: List[Dict[str, Any]], successful: int, failed: int) -> List[str]:
    """
    Generate actionable recommendations based on results.

    Args:
        rendered_pages: List of rendered page information
        successful: Number of successful renders
        failed: Number of failed renders

    Returns:
        List of recommendation strings
    """
    recommendations = []

    # Success rate based recommendations
    success_rate = (successful / len(rendered_pages)) * 100 if rendered_pages else 0

    if success_rate >= 95:
        recommendations.append("Excellent pipeline performance! Consider scaling to more businesses.")
    elif success_rate >= 80:
        recommendations.append("Good pipeline performance. Review failed cases for improvement opportunities.")
    elif success_rate >= 60:
        recommendations.append("Moderate success rate. Consider reviewing data quality and generation prompts.")
    else:
        recommendations.append("Low success rate. Investigate data quality, prompts, and system configuration.")

    # Quality-based recommendations
    quality_scores = [p.get('quality_score', 0.0) for p in rendered_pages
                     if p.get('render_successful') and p.get('quality_score')]

    if quality_scores:
        avg_quality = sum(quality_scores) / len(quality_scores)

        if avg_quality < 0.7:
            recommendations.append("Consider refining content generation prompts to improve quality scores.")

        low_quality_count = len([s for s in quality_scores if s < 0.6])
        if low_quality_count > 0:
            recommendations.append(f"Review and potentially regenerate {low_quality_count} low-quality pages.")

    # Operational recommendations
    if failed > 0:
        recommendations.append("Check CloudWatch logs for detailed error analysis of failed generations.")

    recommendations.extend([
        "Set up monitoring for website performance and SEO metrics.",
        "Consider A/B testing different content templates for optimization.",
        "Plan regular content updates to maintain freshness and relevance."
    ])

    return recommendations


def generate_analytics_code() -> Optional[str]:
    """
    Generate basic analytics tracking code.

    Returns:
        JavaScript analytics code or None
    """
    # Simple analytics stub - in production, integrate with Google Analytics, etc.
    return """
// Basic analytics tracking
(function() {
    var analytics = {
        track: function(event, properties) {
            console.log('Analytics event:', event, properties);
            // In production, send to your analytics service
        },

        pageView: function(page) {
            this.track('page_view', { page: page });
        }
    };

    // Track page load
    analytics.pageView(window.location.pathname);

    // Track external link clicks
    document.addEventListener('click', function(e) {
        if (e.target.tagName === 'A' && e.target.hostname !== window.location.hostname) {
            analytics.track('external_link_click', { url: e.target.href });
        }
    });

    window.analytics = analytics;
})();
"""


def generate_global_css() -> str:
    """
    Generate global CSS styles for the website.

    Returns:
        CSS content string
    """
    return """
/* Global styles for Agentic SEO Content Factory */

/* Reset and base styles */
* {
    box-sizing: border-box;
}

body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
    line-height: 1.6;
    color: #333;
    margin: 0;
    padding: 0;
    background-color: #fff;
}

.container {
    max-width: 1200px;
    margin: 0 auto;
    padding: 0 20px;
}

/* Typography */
h1, h2, h3, h4, h5, h6 {
    margin-top: 0;
    margin-bottom: 0.5em;
    font-weight: 600;
    line-height: 1.3;
}

h1 { font-size: 2.5em; color: #2c3e50; }
h2 { font-size: 2em; color: #34495e; }
h3 { font-size: 1.5em; color: #34495e; }

p {
    margin-bottom: 1em;
}

a {
    color: #3498db;
    text-decoration: none;
}

a:hover {
    text-decoration: underline;
}

/* Utility classes */
.text-center { text-align: center; }
.text-right { text-align: right; }
.mb-1 { margin-bottom: 1rem; }
.mb-2 { margin-bottom: 2rem; }
.mt-1 { margin-top: 1rem; }
.mt-2 { margin-top: 2rem; }

/* Responsive design */
@media (max-width: 768px) {
    .container {
        padding: 0 15px;
    }

    h1 { font-size: 2em; }
    h2 { font-size: 1.5em; }

    .business-grid {
        grid-template-columns: 1fr !important;
    }
}

/* Print styles */
@media print {
    .no-print {
        display: none !important;
    }

    body {
        font-size: 12pt;
        line-height: 1.4;
    }

    a {
        color: inherit;
        text-decoration: none;
    }

    a:after {
        content: " (" attr(href) ")";
        font-size: 0.8em;
        color: #666;
    }
}
"""


def get_website_url(website_bucket: str) -> str:
    """
    Get the public website URL for the S3 bucket.

    Args:
        website_bucket: S3 bucket name

    Returns:
        Website URL string
    """
    # In production, this would be the actual CloudFront or custom domain URL
    # For now, return the S3 website endpoint
    region = os.environ.get('AWS_REGION', 'us-east-1')
    return f"http://{website_bucket}.s3-website-{region}.amazonaws.com"