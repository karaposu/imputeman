# imputeman/services/extractor_service.py
"""AI-powered data extraction service for structured data extraction from HTML"""

import asyncio
import time
import json
from typing import Dict, Any, List, Optional
import httpx

from ..core.config import ExtractConfig
from ..core.entities import ExtractResult, WhatToRetain, ScrapeResult


class ExtractorService:
    """
    Service for AI-powered data extraction from HTML content
    
    This service abstracts away the details of different extraction methods
    and provides a consistent interface for structured data extraction.
    """
    
    def __init__(self, config: ExtractConfig):
        self.config = config
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(self.config.timeout_seconds)
        )
    
    async def extract_from_scrapes(
        self, 
        scrape_results: Dict[str, ScrapeResult], 
        schema: List[WhatToRetain]
    ) -> Dict[str, ExtractResult]:
        """
        Extract structured data from multiple scrape results
        
        Args:
            scrape_results: Dictionary of URL -> ScrapeResult
            schema: List of WhatToRetain objects defining extraction schema
            
        Returns:
            Dictionary of URL -> ExtractResult
        """
        # Filter successful scrapes
        valid_scrapes = {
            url: result for url, result in scrape_results.items()
            if result.status == "ready" and result.data
        }
        
        if not valid_scrapes:
            return {}
        
        # Extract from each valid scrape
        extract_tasks = [
            self.extract_single(url, scrape_result, schema)
            for url, scrape_result in valid_scrapes.items()
        ]
        
        results = await asyncio.gather(*extract_tasks, return_exceptions=True)
        
        # Process results
        extract_results = {}
        for (url, _), result in zip(valid_scrapes.items(), results):
            if isinstance(result, Exception):
                extract_results[url] = ExtractResult(
                    url=url,
                    content=None,
                    success=False,
                    error_message=str(result)
                )
            else:
                extract_results[url] = result
        
        return extract_results
    
    async def extract_single(
        self, 
        url: str, 
        scrape_result: ScrapeResult, 
        schema: List[WhatToRetain]
    ) -> ExtractResult:
        """
        Extract structured data from a single scraped page
        
        Args:
            url: URL of the page
            scrape_result: Scraping result containing HTML
            schema: Extraction schema
            
        Returns:
            ExtractResult with extracted data
        """
        start_time = time.time()
        
        try:
            # Preprocess HTML content
            cleaned_html = self._preprocess_html(scrape_result.data)
            
            # Choose extraction method based on config
            if self.config.extraction_model.startswith("gpt"):
                result = await self._extract_with_openai(
                    cleaned_html, schema, url
                )
            else:
                # Fallback to rule-based extraction
                result = await self._extract_with_rules(
                    cleaned_html, schema, url
                )
            
            result.elapsed_time = time.time() - start_time
            return result
            
        except Exception as e:
            elapsed_time = time.time() - start_time
            return ExtractResult(
                url=url,
                content=None,
                elapsed_time=elapsed_time,
                success=False,
                error_message=f"Extraction failed: {str(e)}"
            )
    
    def _preprocess_html(self, html: str) -> str:
        """
        Clean and preprocess HTML for better extraction
        
        Args:
            html: Raw HTML content
            
        Returns:
            Cleaned HTML suitable for extraction
        """
        if not html:
            return ""
        
        # Remove script and style tags
        import re
        
        # Remove scripts
        html = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
        
        # Remove styles
        html = re.sub(r'<style[^>]*>.*?</style>', '', html, flags=re.DOTALL | re.IGNORECASE)
        
        # Remove comments
        html = re.sub(r'<!--.*?-->', '', html, flags=re.DOTALL)
        
        # Normalize whitespace
        html = re.sub(r'\s+', ' ', html)
        
        # Truncate if too long (to fit within token limits)
        max_chars = self.config.max_tokens * 3  # Rough estimate
        if len(html) > max_chars:
            html = html[:max_chars] + "..."
        
        return html.strip()
    
    async def _extract_with_openai(
        self, 
        html: str, 
        schema: List[WhatToRetain], 
        url: str
    ) -> ExtractResult:
        """
        Extract data using OpenAI GPT models
        
        Args:
            html: Cleaned HTML content
            schema: Extraction schema
            url: Source URL
            
        Returns:
            ExtractResult with extracted data
        """
        try:
            # TODO: Replace with your actual OpenAI integration
            # This is where you'd call your existing extractor.extract_async method
            # Example:
            # extract_op = await self.extractor.extract_async(
            #     html,
            #     schema,
            #     text_type="html"
            # )
            # return self._convert_to_extract_result(extract_op, url)
            
            # Create prompt for extraction
            prompt = self._build_extraction_prompt(html, schema)
            
            # Call OpenAI API
            headers = {
                "Authorization": f"Bearer {self.config.api_key}",
                "Content-Type": "application/json"
            }
            
            payload = {
                "model": self.config.extraction_model,
                "messages": [
                    {"role": "system", "content": "You are a data extraction expert. Extract the requested information from HTML content and return it as valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                "max_tokens": self.config.max_tokens,
                "temperature": 0.1  # Low temperature for consistent extraction
            }
            
            response = await self.client.post(
                "https://api.openai.com/v1/chat/completions",
                headers=headers,
                json=payload
            )
            
            if response.status_code != 200:
                raise Exception(f"OpenAI API error: {response.status_code} - {response.text}")
            
            result_data = response.json()
            
            # Parse extracted content
            extracted_text = result_data["choices"][0]["message"]["content"]
            extracted_data = self._parse_extraction_result(extracted_text, schema)
            
            # Calculate costs and confidence
            usage = result_data.get("usage", {})
            tokens_used = usage.get("total_tokens", 0)
            cost = self._calculate_cost(tokens_used, self.config.extraction_model)
            confidence = self._estimate_confidence(extracted_data, schema)
            
            return ExtractResult(
                url=url,
                content=extracted_data,
                confidence_score=confidence,
                tokens_used=tokens_used,
                cost=cost,
                extraction_method=self.config.extraction_model,
                success=bool(extracted_data),
                metadata={
                    "prompt_length": len(prompt),
                    "response_length": len(extracted_text),
                    "api_model": self.config.extraction_model
                }
            )
            
        except Exception as e:
            return ExtractResult(
                url=url,
                content=None,
                success=False,
                error_message=f"OpenAI extraction error: {str(e)}",
                extraction_method=self.config.extraction_model
            )
    
    def _build_extraction_prompt(self, html: str, schema: List[WhatToRetain]) -> str:
        """
        Build extraction prompt for AI model
        
        Args:
            html: HTML content to extract from
            schema: List of fields to extract
            
        Returns:
            Formatted prompt string
        """
        # Build schema description
        schema_desc = "Extract the following fields:\n"
        for field in schema:
            schema_desc += f"- {field.name}: {field.desc}"
            if field.example:
                schema_desc += f" (example: {field.example})"
            schema_desc += "\n"
        
        prompt = f"""
{schema_desc}

Return the extracted data as a JSON object with the field names as keys. If a field cannot be found, use null as the value.

HTML Content:
{html}

JSON Response:
"""
        return prompt
    
    def _parse_extraction_result(self, response_text: str, schema: List[WhatToRetain]) -> Dict[str, Any]:
        """
        Parse AI model response into structured data
        
        Args:
            response_text: Raw response from AI model
            schema: Expected schema
            
        Returns:
            Parsed structured data
        """
        try:
            # Try to parse as JSON
            if "{" in response_text and "}" in response_text:
                # Extract JSON part
                start = response_text.find("{")
                end = response_text.rfind("}") + 1
                json_text = response_text[start:end]
                
                data = json.loads(json_text)
                
                # Validate against schema
                validated_data = {}
                for field in schema:
                    field_name = field.name
                    if field_name in data:
                        validated_data[field_name] = data[field_name]
                    else:
                        validated_data[field_name] = None
                
                return validated_data
            
        except (json.JSONDecodeError, ValueError):
            pass
        
        # Fallback: Try to extract field values using simple patterns
        fallback_data = {}
        for field in schema:
            fallback_data[field.name] = self._extract_field_fallback(
                response_text, field
            )
        
        return fallback_data
    
    def _extract_field_fallback(self, text: str, field: WhatToRetain) -> Optional[str]:
        """
        Fallback extraction using simple pattern matching
        
        Args:
            text: Text to extract from
            field: Field definition
            
        Returns:
            Extracted value or None
        """
        import re
        
        field_name = field.name.lower()
        
        # Look for patterns like "field_name: value" or "field_name is value"
        patterns = [
            rf"{field_name}[:\s]+([^\n,]+)",
            rf"{field_name}[:\s]+([\d,]+)",
            rf"{field.desc}[:\s]+([^\n,]+)"
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                value = match.group(1).strip()
                # Clean up common prefixes/suffixes
                value = re.sub(r'^["\'\s]+|["\'\s]+$', '', value)
                return value if value else None
        
        return None
    
    async def _extract_with_rules(
        self, 
        html: str, 
        schema: List[WhatToRetain], 
        url: str
    ) -> ExtractResult:
        """
        Extract data using rule-based methods (fallback)
        
        Args:
            html: HTML content
            schema: Extraction schema
            url: Source URL
            
        Returns:
            ExtractResult with extracted data
        """
        # Simple rule-based extraction for fallback
        extracted_data = {}
        
        for field in schema:
            value = self._extract_field_with_rules(html, field)
            extracted_data[field.name] = value
        
        confidence = 0.6 if any(v for v in extracted_data.values()) else 0.2
        
        return ExtractResult(
            url=url,
            content=extracted_data,
            confidence_score=confidence,
            tokens_used=0,
            cost=0.0,  # Rule-based extraction is free
            extraction_method="rule_based",
            success=bool(any(v for v in extracted_data.values())),
            metadata={"method": "rule_based_fallback"}
        )
    
    def _extract_field_with_rules(self, html: str, field: WhatToRetain) -> Optional[str]:
        """
        Extract single field using HTML parsing rules
        
        Args:
            html: HTML content
            field: Field to extract
            
        Returns:
            Extracted value or None
        """
        import re
        
        # Try different extraction strategies based on field description
        desc_lower = field.desc.lower()
        
        if "name" in desc_lower and "company" in desc_lower:
            # Look for company name in title, h1, or meta tags
            patterns = [
                r'<title[^>]*>([^<]+)</title>',
                r'<h1[^>]*>([^<]+)</h1>',
                r'<meta[^>]*name=["\']?title["\']?[^>]*content=["\']([^"\']+)["\']'
            ]
        elif "revenue" in desc_lower or "sales" in desc_lower:
            # Look for revenue/financial numbers
            patterns = [
                r'revenue[:\s]*\$?([\d,\.]+[MBK]?)',
                r'sales[:\s]*\$?([\d,\.]+[MBK]?)'
            ]
        elif "year" in desc_lower or "founded" in desc_lower:
            # Look for years
            patterns = [
                r'founded[:\s]*(\d{4})',
                r'established[:\s]*(\d{4})',
                r'since[:\s]*(\d{4})'
            ]
        else:
            # Generic text extraction
            patterns = [
                rf'{field.name}[:\s]+([^\n,<]+)',
                rf'{desc_lower}[:\s]+([^\n,<]+)'
            ]
        
        for pattern in patterns:
            match = re.search(pattern, html, re.IGNORECASE)
            if match:
                value = match.group(1).strip()
                # Clean HTML entities and tags
                value = re.sub(r'<[^>]+>', '', value)
                value = re.sub(r'&[^;]+;', '', value)
                return value if value else None
        
        return None
    
    def _calculate_cost(self, tokens: int, model: str) -> float:
        """
        Calculate cost based on token usage and model
        
        Args:
            tokens: Number of tokens used
            model: Model name
            
        Returns:
            Cost in dollars
        """
        # Rough cost estimates (update with current pricing)
        cost_per_1k_tokens = {
            "gpt-4": 0.03,
            "gpt-4-turbo": 0.01,
            "gpt-3.5-turbo": 0.002
        }
        
        rate = cost_per_1k_tokens.get(model, 0.01)
        return (tokens / 1000) * rate
    
    def _estimate_confidence(self, extracted_data: Dict[str, Any], schema: List[WhatToRetain]) -> float:
        """
        Estimate confidence score based on extraction completeness
        
        Args:
            extracted_data: Extracted data
            schema: Expected schema
            
        Returns:
            Confidence score between 0 and 1
        """
        if not extracted_data:
            return 0.0
        
        # Count non-null values
        non_null_count = sum(1 for v in extracted_data.values() if v is not None)
        total_fields = len(schema)
        
        if total_fields == 0:
            return 0.0
        
        # Base confidence on completeness
        completeness = non_null_count / total_fields
        
        # Adjust based on data quality (simple heuristics)
        quality_score = 1.0
        for value in extracted_data.values():
            if value is not None:
                # Penalize very short or very long values
                if isinstance(value, str):
                    if len(value) < 2 or len(value) > 200:
                        quality_score *= 0.9
        
        return min(completeness * quality_score, 1.0)
    
    async def close(self):
        """Clean up resources"""
        await self.client.aclose()