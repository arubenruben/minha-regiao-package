from enum import Enum

from tortoise.models import Model
from tortoise import fields


class ParishEra(str, Enum):
    PRE_2013 = "pre_2013"
    POST_2013 = "post_2013"
    POST_2021 = "post_2021"


class Parish(Model):
    id = fields.IntField(primary_key=True)
    name = fields.CharField(max_length=255)
    wikipedia_url = fields.CharField(max_length=2048, null=True)
    ine_code = fields.CharField(max_length=10)
    era = fields.CharEnumField(ParishEra)
    city = fields.ForeignKeyField("models.City", related_name="parishes")

    class Meta:
        table = "parish"
        unique_together = (("ine_code", "era"),)

    def __str__(self) -> str:
        return self.name
