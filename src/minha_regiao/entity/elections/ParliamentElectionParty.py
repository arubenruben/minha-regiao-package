from tortoise.models import Model
from tortoise import fields


class ParliamentElectionParty(Model):
    id = fields.IntField(primary_key=True)
    name = fields.CharField(max_length=255)
    election = fields.ForeignKeyField("models.ParliamentElection", related_name="parties")

    class Meta:
        table = "parliament_election_party"
        unique_together = (("election", "name"),)

    def __str__(self) -> str:
        return self.name
