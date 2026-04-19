from typing import Union
from pydantic import BaseModel
from abc import ABC, abstractmethod
from minha_regiao.llm.LLM import LLM
from langchain_core.prompt_values import ChatPromptValue

"""
client = instructor.from_provider(
    "google/gemini-3-flash",
)

resp = client.create(
    response_model=User,
    messages=[
        {
            "role": "user",
            "content": "Extract Jason is 25 years old.",
        }
    ],
)
"""

class Gemini(LLM):
    def _invoke_non_structured_output(
        self,
        prompt: ChatPromptValue,
        api_key: str,
        temperature: float
    ) -> str:
        raise NotImplementedError("Gemini LLM does not support non-structured output")

    def _invoke_structured_output(
        self,
        prompt: ChatPromptValue,
        api_key: str,
        temperature: float,
        structured_output: type[BaseModel]
    ) -> BaseModel:
        raise NotImplementedError("Gemini LLM does not support structured output")
    