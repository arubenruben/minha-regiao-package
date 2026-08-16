from tortoise.models import Model
from tortoise import fields


class District(Model):
    id = fields.IntField(primary_key=True)
    name = fields.CharField(max_length=255, unique=True)
    wikipedia_url = fields.CharField(max_length=2048, null=True)

    class Meta:
        table = "district"

    def __str__(self) -> str:
        return self.name
