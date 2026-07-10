from tortoise.models import Model
from tortoise import fields


class Parish(Model):
    id = fields.IntField(primary_key=True)
    name = fields.CharField(max_length=255)
    wikipedia_url = fields.CharField(max_length=2048)
    ine_code = fields.CharField(max_length=10, unique=True)
    city = fields.ForeignKeyField("models.City", related_name="parishes")

    class Meta:
        table = "parish"

    def __str__(self) -> str:
        return self.name
