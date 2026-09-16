from tortoise.models import Model
from tortoise import fields

from minha_regiao.entity.RegulationDocumentStatus import RegulationDocumentStatus


class FeeRegulation(Model):
    id = fields.IntField(primary_key=True)
    city = fields.ForeignKeyField("models.City", related_name="fee_regulations")
    year = fields.IntField()
    name = fields.CharField(max_length=255)
    is_complete = fields.BooleanField()
    dre_url = fields.CharField(max_length=2048)
    pdf_url = fields.CharField(max_length=2048, null=True)

    # See RMUE.status/raw_text/structure -- same pipeline, same shape.
    status = fields.CharEnumField(RegulationDocumentStatus, default=RegulationDocumentStatus.PENDING)
    raw_text = fields.TextField(null=True)
    structure = fields.JSONField(null=True)

    class Meta:
        table = "fee_regulation"
        unique_together = (("city", "dre_url"),)

    def __str__(self) -> str:
        return f"{self.city_id}: {self.name}"
