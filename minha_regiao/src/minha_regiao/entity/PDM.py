from tortoise.models import Model
from tortoise import fields

from minha_regiao.entity.RegulationDocumentStatus import RegulationDocumentStatus


class PDM(Model):
    id = fields.IntField(primary_key=True)
    city = fields.ForeignKeyField("models.City", related_name="pdm", unique=True)
    # Top-level fields of extract_pdms.schema.PDMRecord -- title/identifier
    # are nullable since they're new on rows written before this field
    # existed; every row persist_pdms.persist_pdms writes going forward
    # always sets them.
    title = fields.CharField(max_length=255, null=True)
    identifier = fields.CharField(max_length=255, null=True)
    source_url = fields.CharField(max_length=2048)
    # The most recent successfully-extracted document's url (see
    # extract_pdms.services.PDMRepository._select_latest_document) -- a
    # convenience pointer, kept alongside the full history in `documents`.
    # Nullable: a PDM can have documents on record with none of them
    # successfully extracted yet.
    pdf_url = fields.CharField(max_length=2048, null=True)

    class Meta:
        table = "pdm"

    def __str__(self) -> str:
        return f"{self.city_id}: {self.pdf_url}"


class PDMDocument(Model):
    """One dre.pt-published regulation PDF (revision, correction, amendment,
    ...) attached to a PDM, mirroring
    extract_pdms.schema.RegulationDocument.RegulationDocument. `text`/
    `structure` are null whenever `status` isn't OK -- see that schema's
    docstring for what "narrowed to this document's own notice" means.
    """

    id = fields.IntField(primary_key=True)
    pdm = fields.ForeignKeyField("models.PDM", related_name="documents")
    url = fields.CharField(max_length=2048)
    doc_type = fields.CharField(max_length=255)
    number = fields.CharField(max_length=64)
    year = fields.IntField()
    suffix = fields.IntField(null=True)
    data_publicacao = fields.CharField(max_length=64, null=True)
    dinamica = fields.CharField(max_length=255, null=True)
    publicacao = fields.CharField(max_length=255, null=True)
    status = fields.CharEnumField(RegulationDocumentStatus, default=RegulationDocumentStatus.PENDING)
    text = fields.TextField(null=True)
    structure = fields.JSONField(null=True)

    class Meta:
        table = "pdm_document"
        unique_together = (("pdm", "url"),)

    def __str__(self) -> str:
        return f"{self.pdm_id}: {self.url}"
