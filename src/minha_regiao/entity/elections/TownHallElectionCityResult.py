from tortoise.models import Model
from tortoise import fields


class TownHallElectionCityResult(Model):
    """
    City-level vote tally for one town hall election, aggregated from the
    parish-level TownHallElectionResult rows for that city. For
    council/assembly elections this is the natural circle total; for parish
    assembly elections it rolls up otherwise-independent per-parish contests
    into one city-wide view. This is a denormalized rollup populated by the
    ETL, not a live-computed view.
    """

    id = fields.IntField(primary_key=True)
    election = fields.ForeignKeyField("models.TownHallElection", related_name="city_results")
    city = fields.ForeignKeyField("models.City", related_name="town_hall_election_city_results")
    registered_voters = fields.IntField()
    voters = fields.IntField()
    blank_votes = fields.IntField()
    null_votes = fields.IntField()
    valid_votes = fields.IntField()

    class Meta:
        table = "town_hall_election_city_result"
        unique_together = (("election", "city"),)

    def __str__(self) -> str:
        return f"{self.election_id} - {self.city_id}"
