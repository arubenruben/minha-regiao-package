from tortoise.models import Model
from tortoise import fields


class RegionalElectionCityPartyResult(Model):
    id = fields.IntField(primary_key=True)
    city_result = fields.ForeignKeyField("models.RegionalElectionCityResult", related_name="party_results")
    party = fields.ForeignKeyField("models.RegionalElectionParty", related_name="city_results")
    votes = fields.IntField()

    class Meta:
        table = "regional_election_city_party_result"
        unique_together = (("city_result", "party"),)
