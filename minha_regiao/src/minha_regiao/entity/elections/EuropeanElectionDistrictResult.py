from tortoise.models import Model
from tortoise import fields


class EuropeanElectionDistrictResult(Model):
    """
    District-level vote tally for one European Parliament election,
    aggregated from the parish-level EuropeanElectionResult rows for that
    district. This is a denormalized rollup populated by the ETL, not a
    live-computed view.
    """

    id = fields.IntField(primary_key=True)
    election = fields.ForeignKeyField("models.EuropeanElection", related_name="district_results")
    district = fields.ForeignKeyField("models.District", related_name="european_election_district_results")
    registered_voters = fields.IntField()
    voters = fields.IntField()
    blank_votes = fields.IntField()
    null_votes = fields.IntField()
    valid_votes = fields.IntField()

    class Meta:
        table = "european_election_district_result"
        unique_together = (("election", "district"),)

    def __str__(self) -> str:
        return f"{self.election_id} - {self.district_id}"
