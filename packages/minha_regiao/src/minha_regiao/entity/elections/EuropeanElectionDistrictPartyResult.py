from tortoise.models import Model
from tortoise import fields


class EuropeanElectionDistrictPartyResult(Model):
    id = fields.IntField(primary_key=True)
    district_result = fields.ForeignKeyField(
        "models.EuropeanElectionDistrictResult", related_name="party_results"
    )
    party = fields.ForeignKeyField("models.EuropeanElectionParty", related_name="district_results")
    votes = fields.IntField()

    class Meta:
        table = "european_election_district_party_result"
        unique_together = (("district_result", "party"),)
