# here is myllmservice.py

import logging

# logger = logging.getLogger(__name__)
import asyncio
from llmservice.base_service import BaseLLMService
from llmservice.generation_engine import GenerationRequest, GenerationResult
from typing import Optional, Union
from  imputeman import prompts



class MyLLMService(BaseLLMService):
    def __init__(self, logger=None, max_concurrent_requests=200):
        super().__init__(
            logger=logging.getLogger(__name__),
            # default_model_name="gpt-4o-mini",
            default_model_name="gpt-4.1-nano",
            max_rpm=500,
            max_concurrent_requests=max_concurrent_requests,
        )
       
    # def filter, parse

    def parse_via_llm(
        self,
        corpus: str,
        parse_keywords=None, 
        model = None,
    ) -> GenerationResult:
        
        
        user_prompt = prompts.PARSE_VIA_LLM_PROMPT.format(
            corpus=corpus,
            parse_keywords=parse_keywords,
           
        )
        

       
        
        pipeline_config = [
            {
                "type": "ConvertToDict",
                "params": {},
            }
        ]
       
        
        if model is None:
            model= "gpt-4o-mini"
            # model=  "gpt-4.1-nano"
          
           
        
        generation_request = GenerationRequest(
            formatted_prompt=user_prompt,
            model=model,
            output_type="str",
            operation_name="parse_via_llm",
            pipeline_config=pipeline_config,
            # request_id=request_id,
        )

        result = self.execute_generation(generation_request)
        return result
    
    def filter_via_llm(
        self,
        corpus: str,
        thing_to_extract,
        model = None,
        filter_strategy=None
    ) -> GenerationResult:
        
     

       
        if filter_strategy=="liberal":
            user_prompt = prompts.PROPMT_filter_via_llm_liberal.format(
            corpus=corpus,
            thing_to_extract=thing_to_extract
   
            )
        elif filter_strategy=="inclusive": 
            user_prompt = prompts.PROPMT_filter_via_llm_INCLUSIVE.format(
            corpus=corpus,
            thing_to_extract=thing_to_extract
   
            )
        elif filter_strategy=="contextual":

            user_prompt = prompts.PROPMT_filter_via_llm_contextual.format(
            corpus=corpus,
            thing_to_extract=thing_to_extract
   
            )

        elif filter_strategy=="recall":
            user_prompt = prompts.PROPMT_filter_via_llm_recall.format(
            corpus=corpus,
            thing_to_extract=thing_to_extract
   
            )
             
        elif filter_strategy=="base":
            user_prompt = prompts.PROPMT_filter_via_llm_base.format(
            corpus=corpus,
            thing_to_extract=thing_to_extract
   
            )
        
    
    
        if model is None:
            #model= "gpt-4o-mini"
            
            model=  "gpt-4.1-nano"
        
        generation_request = GenerationRequest(
            formatted_prompt=user_prompt,
            model=model,
            output_type="str",
            operation_name="filter_via_llm",
            # pipeline_config=pipeline_config,
            # request_id=request_id,
        )

        result = self.execute_generation(generation_request)
        return result
    
    





    async def filter_via_llm_async(
        self,
        corpus: str,
        thing_to_extract,
        model = None,
        filter_strategy=None
    ) -> GenerationResult:
        
        
       
        if filter_strategy=="liberal":
            user_prompt = prompts.PROPMT_filter_via_llm_liberal.format(
            corpus=corpus,
            thing_to_extract=thing_to_extract
   
            )
        elif filter_strategy=="inclusive": 
            user_prompt = prompts.PROPMT_filter_via_llm_INCLUSIVE.format(
            corpus=corpus,
            thing_to_extract=thing_to_extract
   
            )
        elif filter_strategy=="contextual":

            user_prompt = prompts.PROPMT_filter_via_llm_contextual.format(
            corpus=corpus,
            thing_to_extract=thing_to_extract
   
            )

        elif filter_strategy=="recall":
            user_prompt = prompts.PROPMT_filter_via_llm_recall.format(
            corpus=corpus,
            thing_to_extract=thing_to_extract
   
            )
             
        else:
            user_prompt = prompts.PROPMT_filter_via_llm_base.format(
            corpus=corpus,
            thing_to_extract=thing_to_extract
   
            )
    
         
        if model is None:
            model= "gpt-4o-mini"

        generation_request = GenerationRequest(
            formatted_prompt=user_prompt,
            model=model,
            output_type="str",
            operation_name="filter_via_llm",
            # pipeline_config=pipeline_config,
            # request_id=request_id,
        )

        # BaseLLMService already exposes an async runner:
        result = await self.execute_generation_async(generation_request)
        return result
    

    
        # ────────────────────────── async variant ──────────────────────────
    async def parse_via_llm_async(
        self,
        corpus: str,
        parse_keywords: list[str] | None = None,
        model: str | None = None,
    ) -> GenerationResult:
        """
        Non-blocking version of parse_via_llm().
        Requires BaseLLMService.execute_generation_async.
        """
        user_prompt = prompts.PARSE_VIA_LLM_PROMPT.format(
            corpus=corpus,
            parse_keywords=parse_keywords,
           
        )

        pipeline_config = [
            {"type": "ConvertToDict", "params": {}},
        ]

        model = model or "gpt-4o-mini"
        
        gen_request = GenerationRequest(
            formatted_prompt=user_prompt,
            model=model,
            output_type="str",
            operation_name="parse_via_llm_async",
            pipeline_config=pipeline_config,
        )

        # BaseLLMService supplies execute_generation_async()
        return await self.execute_generation_async(gen_request)



def main():
    """
    Main function to test the categorize_simple method of MyLLMService.
    """
    # Initialize the service
    my_llm_service = MyLLMService()

    # Sample data for testing
    sample_record = "The company reported a significant increase in revenue this quarter."
    sample_classes = ["Finance", "Marketing", "Operations", "Human Resources"]
    request_id = 1

    try:
        # Perform categorization
        result = my_llm_service.categorize_simple(
            record=sample_record,
            list_of_classes=sample_classes,
            request_id=request_id
        )

        # Print the result
        print("Generation Result:", result)
        if result.success:
            print("Categorized Content:", result.content)
        else:
            print("Error:", result.error_message)
    except Exception as e:
        print(f"An exception occurred: {e}")


if __name__ == "__main__":
    main()
