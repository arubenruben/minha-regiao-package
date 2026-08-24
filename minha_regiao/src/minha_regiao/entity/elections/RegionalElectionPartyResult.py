from tortoise.models import Model
from tortoise import fields


class RegionalElectionPartyResult(Model):
    id = fields.IntField(primary_key=True)
    result = fields.ForeignKeyField("models.RegionalElectionResult", related_name="party_results")
    party = fields.ForeignKeyField("models.RegionalElectionParty", related_name="results")
    votes = fields.IntField()

    class Meta:
        table = "regional_election_party_result"
        unique_together = (("result", "party"),)
