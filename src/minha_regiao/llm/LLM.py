from typing import Union
from pydantic import BaseModel
from abc import ABC, abstractmethod
from langchain_core.prompt_values import ChatPromptValue

class LLM(ABC):
    
    @abstractmethod
    def _invoke_non_structured_output(
        self,
        prompt: ChatPromptValue,
        api_key: str,
        temperature: float
    ) -> str:
        raise NotImplementedError("LLM subclasses must implement the _invoke_non_structured_output method")


    @abstractmethod    
    def _invoke_structured_output(        
        self,
        prompt: ChatPromptValue,
        api_key: str,
        temperature: float,
        structured_output: type[BaseModel]
    ) -> BaseModel:
        raise NotImplementedError("LLM subclasses must implement the _invoke_structured_output method")

    def invoke(
        self,
        prompt: ChatPromptValue,
        api_key: str,
        temperature: float,
        structured_output: type[BaseModel]
    ) -> Union[str, BaseModel]:
        
        if not structured_output:
            return self._invoke_non_structured_output(prompt, api_key, temperature)
        
        return self._invoke_structured_output(prompt, api_key, temperature, structured_output)
    