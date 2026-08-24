from tortoise.models import Model
from tortoise import fields


class ElectoralCircle(Model):
    """
    The unit at which parliament seats are apportioned: each mainland
    district, Açores, and Madeira is its own circle (1:1 with District), plus
    two diaspora circles ("Europa", "Fora da Europa") that group countries
    and have no district at all.
    """

    id = fields.IntField(primary_key=True)
    name = fields.CharField(max_length=255, unique=True)
    district = fields.OneToOneField("models.District", related_name="electoral_circle", null=True)

    class Meta:
        table = "electoral_circle"

    def __str__(self) -> str:
        return self.name
