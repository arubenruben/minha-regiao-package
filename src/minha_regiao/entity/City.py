from tortoise.models import Model
from tortoise import fields


class City(Model):
    id = fields.IntField(primary_key=True)
    name = fields.CharField(max_length=255)
    ine_code = fields.CharField(max_length=10, unique=True)
    email = fields.CharField(max_length=255, null=True)
    website = fields.CharField(max_length=2048, null=True)
    district = fields.ForeignKeyField("models.District", related_name="cities", null=True)

    class Meta:
        table = "city"

    def __str__(self) -> str:
        return self.name
