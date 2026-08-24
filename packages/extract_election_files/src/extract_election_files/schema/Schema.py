from abc import ABC
from pydantic import BaseModel

class Schema(BaseModel, ABC):
    pass