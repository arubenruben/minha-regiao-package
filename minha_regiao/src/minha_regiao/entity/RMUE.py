from tortoise.models import Model
from tortoise import fields

from minha_regiao.entity.RegulationDocumentStatus import RegulationDocumentStatus


class RMUE(Model):
    id = fields.IntField(primary_key=True)
    city = fields.ForeignKeyField("models.City", related_name="rmues")
    year = fields.IntField()
    name = fields.CharField(max_length=255)
    is_complete = fields.BooleanField()
    dre_url = fields.CharField(max_length=2048)
    pdf_url = fields.CharField(max_length=2048, null=True)

    # Populated once this document's PDF is resolved/downloaded and its own
    # notice narrowed out of the raw DR page range -- see
    # extract_rmues.tasks.ExtractNoticeText and
    # extract_rmues.services.RMURepository.persist_regulation_results.
    # `raw_text`/`structure` are null whenever `status` isn't OK.
    status = fields.CharEnumField(RegulationDocumentStatus, default=RegulationDocumentStatus.PENDING)
    raw_text = fields.TextField(null=True)
    structure = fields.JSONField(null=True)

    class Meta:
        table = "rmue"
        unique_together = (("city", "dre_url"),)

    def __str__(self) -> str:
        return f"{self.city_id}: {self.name}"
