# test_simple.py
# Simple smoke test for Imputeman (no test framework)

# to run python -m imputeman.simple_test

from .imputeman import Imputeman
from extracthero import ItemToExtract


def main():
    # Define a simple extraction schema
    schema = [
        ItemToExtract(name="title", desc="Page title", example="Example Title"),
        ItemToExtract(name="description", desc="Meta description", example="Example description"),
    ]

    # Initialize Imputeman (override credentials or rely on .env)
    man = Imputeman(
        serpengine_cfg={
            # Replace with real keys or ensure .env is set
            # "GOOGLE_SEARCH_API_KEY": "YOUR_GOOGLE_SEARCH_API_KEY",
            # "GOOGLE_CSE_ID": "YOUR_GOOGLE_CSE_ID",
        },
        extracthero_cfg={},
        # OPENAI_API_KEY="YOUR_OPENAI_API_KEY",
        # BRIGHTDATA_TOKEN="YOUR_BRIGHTDATA_TOKEN",
        scrape_concurrency=40,
    )

    # Run synchronous pipeline for a sample query
    query = "OpenAI GPT-4 release date"
    print(f"Running collect_sync for query: '{query}'\n")
    result = man.collect_sync(query, schema, top_k=3)

    # Display the results
    print("\n=== collect_sync Result ===")
    print(result)


if __name__ == "__main__":
    main()
