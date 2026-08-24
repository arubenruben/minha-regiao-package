from tortoise.models import Model
from tortoise import fields


class RMUE(Model):
    id = fields.IntField(primary_key=True)
    city = fields.ForeignKeyField("models.City", related_name="rmues")
    year = fields.IntField()
    name = fields.CharField(max_length=255)
    is_complete = fields.BooleanField()
    dre_url = fields.CharField(max_length=2048)
    pdf_url = fields.CharField(max_length=2048, null=True)

    class Meta:
        table = "rmue"
        unique_together = (("city", "dre_url"),)

    def __str__(self) -> str:
        return f"{self.city_id}: {self.name}"
