from serpengine import SERPEngine
# from brightdata import trigger_scrape_url, scrape_url, trigger_scrape_url_with_fallback
# from extracthero import Extractor

company_name= "EETECH Company"
what_we_want_to_impute= ["email", "phone number", "linkedin", "workers linkedin url" ]


serpengine = SERPEngine()

result_data_obj = serpengine.collect(
    query=company_name,
    num_urls=5,
    search_sources=["google_search_via_api"],
    regex_based_link_validation=False,             
    allow_links_forwarding_to_files=False,        
    output_format="object"  
)
# print(result_data_obj)

links= result_data_obj.all_links()

print(links)


batch_scrape_job_results= scrape_urls(links)


# extractor = Extractor()

# for key, value in scraped_data.items():
#   if value
#   op = extractor.extract(sample_html, what_we_want_to_impute, text_type="html")

