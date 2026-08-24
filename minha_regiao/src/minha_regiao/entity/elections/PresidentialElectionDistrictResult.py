from tortoise.models import Model
from tortoise import fields


class PresidentialElectionDistrictResult(Model):
    """
    District-level vote tally for one round of a presidential election,
    aggregated from the parish-level PresidentialElectionResult rows for
    that district. This is a denormalized rollup populated by the ETL, not
    a live-computed view.
    """

    id = fields.IntField(primary_key=True)
    election = fields.ForeignKeyField("models.PresidentialElection", related_name="district_results")
    district = fields.ForeignKeyField("models.District", related_name="presidential_election_district_results")
    registered_voters = fields.IntField()
    voters = fields.IntField()
    blank_votes = fields.IntField()
    null_votes = fields.IntField()
    valid_votes = fields.IntField()

    class Meta:
        table = "presidential_election_district_result"
        unique_together = (("election", "district"),)

    def __str__(self) -> str:
        return f"{self.election_id} - {self.district_id}"
