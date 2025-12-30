from abc import ABC, abstractmethod


class Crawler(ABC):
    @abstractmethod
    def extract(self):
        raise NotImplementedError("Subclasses must implement this method")

    @abstractmethod
    def transform(self):
        raise NotImplementedError("Subclasses must implement this method")

    @abstractmethod
    def load(self):
        raise NotImplementedError("Subclasses must implement this method")
