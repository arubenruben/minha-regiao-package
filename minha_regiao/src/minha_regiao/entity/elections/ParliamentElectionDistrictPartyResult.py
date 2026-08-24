from tortoise.models import Model
from tortoise import fields


class ParliamentElectionDistrictPartyResult(Model):
    id = fields.IntField(primary_key=True)
    district_result = fields.ForeignKeyField(
        "models.ParliamentElectionDistrictResult", related_name="party_results"
    )
    party = fields.ForeignKeyField("models.ParliamentElectionParty", related_name="district_results")
    votes = fields.IntField()

    class Meta:
        table = "parliament_election_district_party_result"
        unique_together = (("district_result", "party"),)
