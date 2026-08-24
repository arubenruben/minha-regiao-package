from tortoise.models import Model
from tortoise import fields


class ParliamentElectionDistrictResult(Model):
    """
    District-level vote tally for one parliament election, aggregated from
    the parish-level ParliamentElectionResult rows for that district. This
    is a denormalized rollup populated by the ETL, not a live-computed view.
    Parliament seats are apportioned per district (see ElectoralCircle), so
    this rollup lines up with the circle boundaries directly.
    """

    id = fields.IntField(primary_key=True)
    election = fields.ForeignKeyField("models.ParliamentElection", related_name="district_results")
    district = fields.ForeignKeyField("models.District", related_name="parliament_election_district_results")
    registered_voters = fields.IntField()
    voters = fields.IntField()
    blank_votes = fields.IntField()
    null_votes = fields.IntField()
    valid_votes = fields.IntField()

    class Meta:
        table = "parliament_election_district_result"
        unique_together = (("election", "district"),)

    def __str__(self) -> str:
        return f"{self.election_id} - {self.district_id}"
