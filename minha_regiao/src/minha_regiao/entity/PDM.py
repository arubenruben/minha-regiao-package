from tortoise.models import Model
from tortoise import fields


class PDM(Model):
    id = fields.IntField(primary_key=True)
    city = fields.ForeignKeyField("models.City", related_name="pdm", unique=True)
    source_url = fields.CharField(max_length=2048)
    pdf_url = fields.CharField(max_length=2048)

    class Meta:
        table = "pdm"

    def __str__(self) -> str:
        return f"{self.city_id}: {self.pdf_url}"
