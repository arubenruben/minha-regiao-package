from tortoise.models import Model
from tortoise import fields


class TownHallElectionList(Model):
    """
    A candidacy list (party, coalition, or independent movement) competing
    in one town hall election. Unlike Parliament's parties, a list name has
    no stable national identity — the same label in two different
    municipalities is an unrelated candidacy — so identity is scoped to a
    territory: a city for council/assembly elections (one contest per
    municipality, reported by parish) or a parish for parish assembly
    elections (one contest per parish). Scoped to exactly one of city or
    parish — never both, never neither — matching whichever grain the
    election's sub_type contests at.

    The city/parish exclusivity and the one-name-per-territory constraints
    are enforced in the migration (CHECK + partial unique indexes), since
    Tortoise has no declarative API for either.
    """

    id = fields.IntField(primary_key=True)
    name = fields.CharField(max_length=255)
    election = fields.ForeignKeyField("models.TownHallElection", related_name="lists")
    city = fields.ForeignKeyField("models.City", related_name="town_hall_election_lists", null=True)
    parish = fields.ForeignKeyField("models.Parish", related_name="town_hall_election_lists", null=True)

    class Meta:
        table = "town_hall_election_list"

    def __str__(self) -> str:
        return self.name
