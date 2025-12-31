from typing import Any
from abc import ABC, abstractmethod


class Crawler(ABC):
    @abstractmethod
    def extract(self, *args, **kwargs) -> Any:
        raise NotImplementedError("Subclasses must implement this method")

    @abstractmethod
    def transform(self, *args, **kwargs) -> Any:
        raise NotImplementedError("Subclasses must implement this method")

    @abstractmethod
    def load(self, *args, **kwargs) -> Any:
        raise NotImplementedError("Subclasses must implement this method")
