from tortoise.models import Model
from tortoise import fields


class ParliamentElectionResult(Model):
    """
    Leaf-level vote tally for one parliament election, scoped to exactly one
    territory: either a parish (mainland/islands) or a consulate (diaspora)
    — never both, never neither. City/district/country/global totals are
    aggregates of these rows and are not stored separately.

    The parish/consulate exclusivity and the one-result-per-territory
    constraints are enforced in the migration (CHECK + partial unique
    indexes), since Tortoise has no declarative API for either.
    """

    id = fields.IntField(primary_key=True)
    election = fields.ForeignKeyField("models.ParliamentElection", related_name="results")
    parish = fields.ForeignKeyField("models.Parish", related_name="parliament_election_results", null=True)
    consulate = fields.ForeignKeyField("models.Consulate", related_name="parliament_election_results", null=True)
    registered_voters = fields.IntField()
    voters = fields.IntField()
    blank_votes = fields.IntField()
    null_votes = fields.IntField()
    valid_votes = fields.IntField()

    class Meta:
        table = "parliament_election_result"

    def __str__(self) -> str:
        return f"{self.election_id} - {self.parish_id or self.consulate_id}"
