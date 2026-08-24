from tortoise.models import Model
from tortoise import fields


class TownHallElectionSeat(Model):
    """
    Seats won by a list in a town hall election. Scoped to exactly one of
    city (council/assembly, apportioned per municipality) or parish (parish
    assembly, apportioned per parish) — never both, never neither — matching
    the territory the associated TownHallElectionList is scoped to.

    The city/parish exclusivity and the one-seat-row-per-list-per-territory
    constraints are enforced in the migration (CHECK + partial unique
    indexes), since Tortoise has no declarative API for either.
    """

    id = fields.IntField(primary_key=True)
    election = fields.ForeignKeyField("models.TownHallElection", related_name="seats")
    city = fields.ForeignKeyField("models.City", related_name="town_hall_election_seats", null=True)
    parish = fields.ForeignKeyField("models.Parish", related_name="town_hall_election_seats", null=True)
    list = fields.ForeignKeyField("models.TownHallElectionList", related_name="seats")
    seats = fields.IntField()

    class Meta:
        table = "town_hall_election_seat"

    def __str__(self) -> str:
        return f"{self.election_id} - {self.city_id or self.parish_id}"
