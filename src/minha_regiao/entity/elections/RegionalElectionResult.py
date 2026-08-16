from tortoise.models import Model
from tortoise import fields


class RegionalElectionResult(Model):
    """
    Leaf-level vote tally for one regional assembly election (Madeira or
    Açores), scoped to a parish. Regional elections have no diaspora leg, so
    — unlike Parliament/Presidential/European — every row belongs to a
    parish; there is no consulate alternative to guard against. City-level
    totals are stored separately as a denormalized rollup (see
    RegionalElectionCityResult) populated by the ETL.
    """

    id = fields.IntField(primary_key=True)
    election = fields.ForeignKeyField("models.RegionalElection", related_name="results")
    parish = fields.ForeignKeyField("models.Parish", related_name="regional_election_results")
    registered_voters = fields.IntField()
    voters = fields.IntField()
    blank_votes = fields.IntField()
    null_votes = fields.IntField()
    valid_votes = fields.IntField()

    class Meta:
        table = "regional_election_result"
        unique_together = (("election", "parish"),)

    def __str__(self) -> str:
        return f"{self.election_id} - {self.parish_id}"
