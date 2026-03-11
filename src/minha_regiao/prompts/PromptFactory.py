from typing import List, Union
from abc import ABC, abstractmethod
from langchain.messages import SystemMessage, HumanMessage

class PromptFactory(ABC):
    @staticmethod
    @abstractmethod
    def system_prompt()->SystemMessage:
        raise NotImplementedError("This method should be implemented by subclasses.")

    @staticmethod
    @abstractmethod
    def prompt(normalize_instructor: bool = False, **kwargs)->Union[List[Union[SystemMessage, HumanMessage]], List[dict]]:
        raise NotImplementedError("This method should be implemented by subclasses.")