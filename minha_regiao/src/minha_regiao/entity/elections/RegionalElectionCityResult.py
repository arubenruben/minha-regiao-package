from tortoise.models import Model
from tortoise import fields


class RegionalElectionCityResult(Model):
    """
    City-level vote tally for one regional assembly election, aggregated
    from the parish-level RegionalElectionResult rows for that city. This is
    a denormalized rollup populated by the ETL, not a live-computed view.
    """

    id = fields.IntField(primary_key=True)
    election = fields.ForeignKeyField("models.RegionalElection", related_name="city_results")
    city = fields.ForeignKeyField("models.City", related_name="regional_election_city_results")
    registered_voters = fields.IntField()
    voters = fields.IntField()
    blank_votes = fields.IntField()
    null_votes = fields.IntField()
    valid_votes = fields.IntField()

    class Meta:
        table = "regional_election_city_result"
        unique_together = (("election", "city"),)

    def __str__(self) -> str:
        return f"{self.election_id} - {self.city_id}"
