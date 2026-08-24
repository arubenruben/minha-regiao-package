from tortoise.models import Model
from tortoise import fields


class ParliamentElectionCityPartyResult(Model):
    id = fields.IntField(primary_key=True)
    city_result = fields.ForeignKeyField("models.ParliamentElectionCityResult", related_name="party_results")
    party = fields.ForeignKeyField("models.ParliamentElectionParty", related_name="city_results")
    votes = fields.IntField()

    class Meta:
        table = "parliament_election_city_party_result"
        unique_together = (("city_result", "party"),)
