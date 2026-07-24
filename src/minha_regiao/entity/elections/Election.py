from tortoise.models import Model
from tortoise import fields


class Election(Model):
    id = fields.IntField(primary_key=True)
    name = fields.CharField(max_length=255)
    date = fields.DateField()
    url = fields.CharField(max_length=2048)
    filename = fields.CharField(max_length=255)

    class Meta:
        abstract = True

    def __str__(self) -> str:
        return self.name
