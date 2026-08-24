from tortoise.models import Model
from tortoise import fields


class PresidentialElectionCandidate(Model):
    id = fields.IntField(primary_key=True)
    name = fields.CharField(max_length=255)
    election = fields.ForeignKeyField("models.PresidentialElection", related_name="candidates")

    class Meta:
        table = "presidential_election_candidate"
        unique_together = (("election", "name"),)

    def __str__(self) -> str:
        return self.name
