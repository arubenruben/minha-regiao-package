from tortoise.models import Model
from tortoise import fields


class EuropeanElectionPartyResult(Model):
    id = fields.IntField(primary_key=True)
    result = fields.ForeignKeyField("models.EuropeanElectionResult", related_name="party_results")
    party = fields.ForeignKeyField("models.EuropeanElectionParty", related_name="results")
    votes = fields.IntField()

    class Meta:
        table = "european_election_party_result"
        unique_together = (("result", "party"),)
