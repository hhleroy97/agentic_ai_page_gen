"""
Amazon Bedrock client for LLM interactions in the Content Factory.
Handles Claude model invocations with proper error handling and retry logic.
"""

import json
import boto3
import logging
import time
from typing import Dict, Any, Optional, Tuple
from botocore.exceptions import ClientError
from .schemas import GenerationTrace

logger = logging.getLogger(__name__)


class BedrockClient:
    """Manages interactions with Amazon Bedrock LLM services"""

    def __init__(self, region_name: str = 'us-east-1'):
        """
        Initialize Bedrock client.

        Args:
            region_name: AWS region for Bedrock service
        """
        self.region = region_name
        self.client = boto3.client('bedrock-runtime', region_name=region_name)

        # Model configurations
        self.models = {
            'claude-3-haiku': {
                'model_id': 'anthropic.claude-3-haiku-20240307-v1:0',
                'max_tokens': 4096,
                'temperature': 0.1,
                'top_p': 0.9
            },
            'claude-3-sonnet': {
                'model_id': 'anthropic.claude-3-sonnet-20240229-v1:0',
                'max_tokens': 4096,
                'temperature': 0.3,
                'top_p': 0.9
            }
        }

        self.default_model = 'claude-3-haiku'
        logger.info(f"Initialized BedrockClient for region: {region_name}")

    def invoke_model(self, prompt: str, system_prompt: str = None,
                    model_name: str = None, max_retries: int = 3) -> Tuple[Optional[str], GenerationTrace]:
        """
        Invoke Bedrock model with prompt and return response.

        Args:
            prompt: User prompt for the model
            system_prompt: System prompt for context
            model_name: Model to use (defaults to claude-3-haiku)
            max_retries: Maximum retry attempts

        Returns:
            Tuple of (response_text, generation_trace)
        """
        model_name = model_name or self.default_model
        model_config = self.models.get(model_name)

        if not model_config:
            raise ValueError(f"Unknown model: {model_name}")

        start_time = time.time()
        trace = GenerationTrace(
            business_id='unknown',
            prompt_version='1.0',
            model_name=model_name,
            generation_time_ms=0,
            retry_count=0
        )

        for attempt in range(max_retries + 1):
            try:
                # Prepare request body
                body = {
                    "anthropic_version": "bedrock-2023-05-31",
                    "max_tokens": model_config['max_tokens'],
                    "temperature": model_config['temperature'],
                    "top_p": model_config['top_p'],
                    "messages": [
                        {
                            "role": "user",
                            "content": prompt
                        }
                    ]
                }

                # Add system prompt if provided
                if system_prompt:
                    body["system"] = system_prompt

                # Invoke model
                response = self.client.invoke_model(
                    modelId=model_config['model_id'],
                    body=json.dumps(body),
                    contentType='application/json',
                    accept='application/json'
                )

                # Parse response
                response_body = json.loads(response['body'].read())

                # Extract content
                if 'content' in response_body and response_body['content']:
                    content = response_body['content'][0].get('text', '')

                    # Update trace
                    end_time = time.time()
                    trace.generation_time_ms = int((end_time - start_time) * 1000)
                    trace.retry_count = attempt
                    trace.token_count = response_body.get('usage', {}).get('output_tokens', 0)

                    logger.info(f"Successfully invoked {model_name} after {attempt + 1} attempts")
                    return content, trace

                else:
                    raise ValueError("No content in model response")

            except ClientError as e:
                error_code = e.response['Error']['Code']
                error_msg = f"Bedrock API error: {error_code} - {e.response['Error']['Message']}"

                trace.errors.append(error_msg)
                logger.warning(f"Attempt {attempt + 1} failed: {error_msg}")

                # Check if we should retry
                if attempt < max_retries and error_code in ['ThrottlingException', 'ServiceUnavailableException']:
                    wait_time = (2 ** attempt) + 1  # Exponential backoff
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
                else:
                    break

            except Exception as e:
                error_msg = f"Unexpected error: {str(e)}"
                trace.errors.append(error_msg)
                logger.error(f"Attempt {attempt + 1} failed: {error_msg}")

                if attempt < max_retries:
                    wait_time = (2 ** attempt) + 1
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
                else:
                    break

        # All attempts failed
        end_time = time.time()
        trace.generation_time_ms = int((end_time - start_time) * 1000)
        trace.retry_count = max_retries + 1

        logger.error(f"Failed to invoke {model_name} after {max_retries + 1} attempts")
        return None, trace

    def generate_content(self, prompt: str, business_id: str = 'unknown',
                        model_name: str = None) -> Tuple[Optional[Dict[str, Any]], GenerationTrace]:
        """
        Generate structured content using LLM.

        Args:
            prompt: Generation prompt
            business_id: Business ID for tracking
            model_name: Model to use

        Returns:
            Tuple of (parsed_json_response, generation_trace)
        """
        system_prompt = """You are an expert SEO content writer specializing in local business pages.
        You must respond with valid JSON that matches the specified schema exactly.
        Do not include any text outside the JSON response."""

        # Invoke model
        response_text, trace = self.invoke_model(prompt, system_prompt, model_name)
        trace.business_id = business_id

        if not response_text:
            return None, trace

        # Parse JSON response
        try:
            # Clean response text (remove potential markdown formatting)
            cleaned_response = response_text.strip()
            if cleaned_response.startswith('```json'):
                cleaned_response = cleaned_response[7:]
            if cleaned_response.endswith('```'):
                cleaned_response = cleaned_response[:-3]

            parsed_response = json.loads(cleaned_response)
            trace.quality_checks.append("JSON parsing successful")

            logger.info(f"Successfully generated content for business {business_id}")
            return parsed_response, trace

        except json.JSONDecodeError as e:
            error_msg = f"Failed to parse JSON response: {str(e)}"
            trace.errors.append(error_msg)
            logger.error(error_msg)
            return None, trace

    def quality_check_content(self, content_data: Dict[str, Any], business_id: str = 'unknown',
                             model_name: str = None) -> Tuple[Optional[Dict[str, Any]], GenerationTrace]:
        """
        Perform quality check on generated content.

        Args:
            content_data: Generated content to check
            business_id: Business ID for tracking
            model_name: Model to use for QC

        Returns:
            Tuple of (quality_feedback, generation_trace)
        """
        # Create QC prompt
        qc_prompt = f"""Evaluate this generated SEO content for quality and compliance:

        CONTENT TO EVALUATE:
        {json.dumps(content_data, indent=2)}

        Evaluate based on:
        1. SEO technical requirements (title length, meta description, word count)
        2. Content quality and relevance
        3. Local business appropriateness
        4. Schema.org compliance

        Return a QualityFeedback JSON with:
        - quality_score (0.0-1.0)
        - passed_checks (list of strings)
        - failed_checks (list of strings)
        - suggestions (list of strings)
        - needs_regeneration (boolean)"""

        system_prompt = """You are a quality assurance specialist for SEO content.
        Evaluate content strictly and provide detailed feedback.
        Respond with valid QualityFeedback JSON only."""

        # Invoke model for QC
        response_text, trace = self.invoke_model(qc_prompt, system_prompt, model_name)
        trace.business_id = business_id

        if not response_text:
            return None, trace

        # Parse QC response
        try:
            cleaned_response = response_text.strip()
            if cleaned_response.startswith('```json'):
                cleaned_response = cleaned_response[7:]
            if cleaned_response.endswith('```'):
                cleaned_response = cleaned_response[:-3]

            qc_result = json.loads(cleaned_response)
            trace.quality_checks.append("QC evaluation completed")

            logger.info(f"Quality check completed for business {business_id}")
            return qc_result, trace

        except json.JSONDecodeError as e:
            error_msg = f"Failed to parse QC response: {str(e)}"
            trace.errors.append(error_msg)
            logger.error(error_msg)
            return None, trace

    def get_model_info(self, model_name: str = None) -> Dict[str, Any]:
        """
        Get information about available models.

        Args:
            model_name: Specific model to get info for

        Returns:
            Model information dictionary
        """
        if model_name:
            return self.models.get(model_name, {})
        else:
            return self.models

    def set_default_model(self, model_name: str) -> bool:
        """
        Set the default model for generation.

        Args:
            model_name: Model to set as default

        Returns:
            True if successful, False if model not found
        """
        if model_name in self.models:
            self.default_model = model_name
            logger.info(f"Default model set to: {model_name}")
            return True
        else:
            logger.error(f"Model not found: {model_name}")
            return False

    def estimate_cost(self, input_tokens: int, output_tokens: int, model_name: str = None) -> float:
        """
        Estimate the cost of a Bedrock API call.

        Args:
            input_tokens: Number of input tokens
            output_tokens: Number of output tokens
            model_name: Model used

        Returns:
            Estimated cost in USD
        """
        model_name = model_name or self.default_model

        # Pricing per 1000 tokens (as of 2024)
        pricing = {
            'claude-3-haiku': {'input': 0.00025, 'output': 0.00125},
            'claude-3-sonnet': {'input': 0.003, 'output': 0.015}
        }

        if model_name not in pricing:
            return 0.0

        model_pricing = pricing[model_name]
        input_cost = (input_tokens / 1000) * model_pricing['input']
        output_cost = (output_tokens / 1000) * model_pricing['output']

        return round(input_cost + output_cost, 6)