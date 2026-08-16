from tortoise.models import Model
from tortoise import fields


class TownHallElectionResult(Model):
    """
    Leaf-level vote tally for one town hall election, scoped to a parish —
    the physical polling/reporting unit even for council and municipal
    assembly elections, where the actual contest spans the whole
    municipality (see TownHallElectionList). Town hall elections have no
    diaspora leg, so there is no consulate alternative to guard against.
    City-level totals are stored separately as a denormalized rollup (see
    TownHallElectionCityResult) populated by the ETL.
    """

    id = fields.IntField(primary_key=True)
    election = fields.ForeignKeyField("models.TownHallElection", related_name="results")
    parish = fields.ForeignKeyField("models.Parish", related_name="town_hall_election_results")
    registered_voters = fields.IntField()
    voters = fields.IntField()
    blank_votes = fields.IntField()
    null_votes = fields.IntField()
    valid_votes = fields.IntField()

    class Meta:
        table = "town_hall_election_result"
        unique_together = (("election", "parish"),)

    def __str__(self) -> str:
        return f"{self.election_id} - {self.parish_id}"
