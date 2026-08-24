from tortoise.models import Model
from tortoise import fields


class Consulate(Model):
    id = fields.IntField(primary_key=True)
    name = fields.CharField(max_length=255)
    code = fields.CharField(max_length=10, unique=True)
    country = fields.ForeignKeyField("models.Country", related_name="consulates")

    class Meta:
        table = "consulate"

    def __str__(self) -> str:
        return self.name
