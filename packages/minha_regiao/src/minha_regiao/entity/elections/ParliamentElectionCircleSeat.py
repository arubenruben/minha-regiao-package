from tortoise.models import Model
from tortoise import fields


class ParliamentElectionCircleSeat(Model):
    """
    Seats won by a party in an electoral circle. Unlike vote tallies, this is
    not derivable from parish/consulate-level data — it's the output of the
    D'Hondt apportionment run once per circle — so it's stored as its own
    fact at circle grain rather than computed from the leaf results.
    """

    id = fields.IntField(primary_key=True)
    election = fields.ForeignKeyField("models.ParliamentElection", related_name="circle_seats")
    circle = fields.ForeignKeyField("models.ElectoralCircle", related_name="seats")
    party = fields.ForeignKeyField("models.ParliamentElectionParty", related_name="circle_seats")
    seats = fields.IntField()

    class Meta:
        table = "parliament_election_circle_seat"
        unique_together = (("election", "circle", "party"),)
