from tortoise.models import Model
from tortoise import fields


class ParliamentElectionPartyResult(Model):
    id = fields.IntField(primary_key=True)
    result = fields.ForeignKeyField("models.ParliamentElectionResult", related_name="party_results")
    party = fields.ForeignKeyField("models.ParliamentElectionParty", related_name="results")
    votes = fields.IntField()

    class Meta:
        table = "parliament_election_party_result"
        unique_together = (("result", "party"),)
