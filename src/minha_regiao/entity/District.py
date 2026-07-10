from tortoise.models import Model
from tortoise import fields


class District(Model):
    id = fields.IntField(primary_key=True)
    name = fields.CharField(max_length=255)
    wikipedia_url = fields.CharField(max_length=2048)

    class Meta:
        table = "district"

    def __str__(self) -> str:
        return self.name
