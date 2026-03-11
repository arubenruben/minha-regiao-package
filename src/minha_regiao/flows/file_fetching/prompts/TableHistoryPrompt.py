from typing import List, Union
from langchain.messages import SystemMessage, HumanMessage
from minha_regiao.prompts.PromptFactory import PromptFactory

class TableHistoryPrompt(PromptFactory):    
    @staticmethod
    def system_prompt()->SystemMessage:
        return SystemMessage(content="You are an expert in parsing HTML tables containing historical election data. Your task is to extract the relevant information from the provided HTML table and structure it into an Elections object.")

    @staticmethod
    def prompt(normalize_instructor: bool = False, **kwargs)->Union[List[Union[SystemMessage, HumanMessage]], List[dict]]:
        if 'raw_html' not in kwargs:
            raise ValueError("The 'raw_html' keyword argument is required to generate the prompt.")
        
        raw_html = kwargs['raw_html']
        
        messages = [
            TableHistoryPrompt.system_prompt(),
            HumanMessage(content=f"""
                Please parse the following HTML table and extract the election type, election name, and election dates. 
                {raw_html}
                The election dates should be returned in ISO format (YYYY-MM-DD).
            """)
        ]


        if not normalize_instructor:
            return messages
        
        return [
            {
                "role": "system",
                "content": messages[0].content
            },
            {
                "role": "user",
                "content": messages[1].content
            }
        ]