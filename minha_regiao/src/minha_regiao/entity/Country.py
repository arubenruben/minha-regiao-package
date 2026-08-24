from tortoise.models import Model
from tortoise import fields


class Country(Model):
    id = fields.IntField(primary_key=True)
    name = fields.CharField(max_length=255, unique=True)
    electoral_circle = fields.ForeignKeyField("models.ElectoralCircle", related_name="countries", null=True)

    class Meta:
        table = "country"

    def __str__(self) -> str:
        return self.name
