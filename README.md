# Imputeman 

AI powered context aware data imputation pipeline

imputeman is build upon 5 modules

SerpEngine for link collection
Brightdata for scraping
Extracthero for information extraction



## Features

- 🔍 **Intelligent Search**: SERP API integration for finding relevant data sources
- 🕷️ **Robust Web Scraping**: Concurrent scraping with retry logic and cost monitoring  
- 🧠 **AI-Powered Extraction**: Structured data extraction using LLMs
- 🌊 **Conditional Workflows**: Dynamic pipeline routing based on cost, quality, and success metrics
- 💰 **Cost-Aware**: Budget-conscious scraping modes and cost monitoring
- 🔄 **Retry Logic**: Configurable retry strategies for each pipeline stage
- 📊 **Rich Observability**: Detailed logging, metrics,

## Quick Start

### Installation

```bash
# Install the package
pip install -e .

# Install with development dependencies
pip install -e ".[dev]"

```

### Basic Usage

```python
import asyncio
from imputeman.core.entities import EntityToImpute, WhatToRetain
from imputeman.flows.main_flow import simple_imputeman_flow

async def main():
    # Create entity to research - bav99 is an electronic component part number
    entity = EntityToImpute(
        name="bav99",
        identifier_context="electronic component part number",
        impute_task_purpose="component specification research"
    )
    
    # Define expected data schema using WhatToRetain
    # Extract technical specifications we DON'T already know
    schema = [
        WhatToRetain(
            name="component_type",
            desc="Type of electronic component",
            example="NPN transistor"
        ),
        WhatToRetain(
            name="voltage_rating", 
            desc="Maximum voltage rating in volts",
            example="75V"
        ),
        WhatToRetain(
            name="package_type",
            desc="Physical package type",
            example="SOT-23"
        )
    ]
    
    # Run pipeline
    result = await simple_imputeman_flow(entity, schema, top_k=5)
    
    if result.success:
        print(f"✅ Success! Extracted data: {result.final_data}")
    else:
        print(f"❌ Failed: {result.metadata.get('error', 'Unknown error')}")

# Run the pipeline
asyncio.run(main())
```

### Environment Setup

Create a `.env` file with your API keys:

```bash
# Search API
SERP_API_KEY=your_serp_api_key

# Web scraping
BRIGHT_DATA_TOKEN=your_bright_data_token

# AI extraction  
OPENAI_API_KEY=your_openai_api_key

# Prefect (optional)
PREFECT_API_URL=http://localhost:4200/api
```

## Use Cases

Imputeman can research and extract structured data for any type of entity:

### **Electronic Components**
```python
entity = EntityToImpute(name="bav99")  # Component part number
schema = [
    WhatToRetain(name="component_type", desc="Type of electronic component"),
    WhatToRetain(name="voltage_rating", desc="Maximum voltage rating"),
    WhatToRetain(name="manufacturer", desc="Component manufacturer")
]
```

### **Pharmaceutical Compounds**  
```python
entity = EntityToImpute(name="aspirin")  # Chemical compound
schema = [
    WhatToRetain(name="chemical_formula", desc="Chemical molecular formula"),
    WhatToRetain(name="mechanism_of_action", desc="How the drug works"),
    WhatToRetain(name="half_life", desc="Elimination half-life")
]
```

### **Companies & Organizations**
```python
entity = EntityToImpute(name="OpenAI")  # Company name
schema = [
    WhatToRetain(name="current_valuation", desc="Current company valuation"),
    WhatToRetain(name="employee_count", desc="Number of employees"),
    WhatToRetain(name="latest_funding", desc="Most recent funding round")
]
```

### **Research Papers, Patents, Materials, etc.**
The pipeline works for any entity you can search for and want to extract structured data about.

## Architecture

### Pipeline Stages

1. **Search (SERP)**: Find relevant URLs using search APIs
2. **Scraping**: Extract HTML content from discovered URLs
3. **Extraction**: Use AI to extract structured data from HTML
4. **Aggregation**: Combine results into final dataset

### Conditional Logic

The pipeline includes intelligent branching:

```python
# Cost-based routing
if scraping_cost > threshold:
    → switch_to_budget_scraping()

# Quality-based validation  
if confidence < threshold:
    → retry_with_relaxed_criteria()

# Success-based continuation
if successful_extractions < minimum:
    → try_alternative_approach()
```

### Configuration

Each stage is highly configurable:

```python
from imputeman.core.config import PipelineConfig, ScrapeConfig

config = PipelineConfig()
config.scrape_config.max_cost_threshold = 50.0
config.extract_config.confidence_threshold = 0.8
config.cost_threshold_for_budget_mode = 100.0

result = await imputeman_flow(entity, schema, config)
```

## Advanced Usage

### Custom Workflows

```python
from imputeman.flows.main_flow import imputeman_flow
from imputeman.core.config import get_development_config
from imputeman.core.entities import WhatToRetain

# Use development config with custom settings
config = get_development_config()
config.serp_config.top_k_results = 15
config.scrape_config.concurrent_limit = 8

entity = EntityToImpute(
    name="bc547",  # Electronic component
    identifier_context="transistor part number", 
    impute_task_purpose="component research"
)

schema = [
    WhatToRetain(
        name="component_type",
        desc="Type of transistor",
        example="NPN bipolar junction transistor"
    ),
    WhatToRetain(
        name="max_collector_current",
        desc="Maximum collector current rating",
        example="100mA"
    )
]

result = await imputeman_flow(entity, schema, config)
```

### Prefect Deployments

Deploy your workflows for production:

```bash
# Deploy to Prefect
prefect deploy --name imputeman-prod

# Run deployed flow
prefect deployment run 'imputeman-pipeline/imputeman-prod' \
  --param entity='{"name": "bav99"}' \
  --param schema='[{"name": "component_type", "desc": "Type of electronic component"}]'
```

### Monitoring & Observability

The Prefect UI provides rich monitoring:

- **Flow runs**: Track pipeline executions
- **Task details**: See individual stage performance  
- **Logs**: Detailed execution logs
- **Metrics**: Cost, timing, and success rates
- **Retries**: Automatic retry visualization

Access the UI at `http://localhost:4200` after starting the Prefect server.

## Configuration Options

### Stage Configurations

#### SERP Configuration
- `max_retries`: Retry attempts for failed searches
- `top_k_results`: Number of URLs to find
- `timeout_seconds`: Search timeout
- `rate_limit_per_minute`: API rate limiting

#### Scrape Configuration  
- `concurrent_limit`: Max parallel scraping requests
- `max_cost_threshold`: Budget limit for scraping
- `use_browser_fallback`: Enable browser-based scraping
- `poll_timeout`: Max wait time for scraping completion

#### Extract Configuration
- `confidence_threshold`: Minimum confidence for valid extractions
- `max_tokens`: Token limit for AI processing
- `extraction_model`: AI model to use (gpt-4, gpt-3.5-turbo)

### Pipeline Configuration
- `cost_threshold_for_budget_mode`: When to switch to budget scraping
- `min_successful_extractions`: Minimum extractions needed for success
- `enable_caching`: Cache search and scrape results

## Error Handling

The pipeline gracefully handles various failure modes:

- **Search failures**: Retry with backoff, return partial results
- **Scraping failures**: Individual URL failures don't stop the pipeline
- **Extraction failures**: Continue with successful extractions
- **Cost overruns**: Automatically switch to budget mode
- **Quality issues**: Retry with relaxed thresholds

## Development

### Project Structure

```
imputeman/
├── core/           # Data models and configuration
├── tasks/          # Prefect tasks for each stage
├── flows/          # Workflow orchestration
├── services/       # External service integrations
└── utils/          # Utilities and helpers
```

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=imputeman

# Run specific test category
pytest tests/test_tasks.py -v
```

### Code Quality

```bash
# Format code
black imputeman/

# Lint code  
ruff check imputeman/

# Type checking
mypy imputeman/
```

## Migrating from Legacy Code

If you have existing Imputeman code, migration is straightforward:

### Before (Legacy)
```python
iman = Imputeman()
impute_op = iman.run_sync("bav99", schema=schema, top_k=1)
print(impute_op.results)
```

### After (Prefect)
```python
from imputeman.core.entities import EntityToImpute, WhatToRetain

entity = EntityToImpute(name="bav99")
schema = [
    WhatToRetain(
        name="component_type",
        desc="Type of electronic component"
    )
]
result = await simple_imputeman_flow(entity, schema, top_k=1)
print(result.final_data)
```

### Key Changes
- **EntityToImpute dataclass** instead of raw strings
- **Async/await** for better performance
- **Rich result objects** with detailed metadata
- **Configurable pipeline** stages
- **Built-in observability** and monitoring
