from tortoise.models import Model
from tortoise import fields


class EuropeanElectionPartySeat(Model):
    """
    Seats won by a party in a European Parliament election. Portugal elects
    its MEPs from a single national constituency, so — unlike Parliament's
    per-circle apportionment — this is one row per party per election rather
    than per circle.
    """

    id = fields.IntField(primary_key=True)
    election = fields.ForeignKeyField("models.EuropeanElection", related_name="party_seats")
    party = fields.ForeignKeyField("models.EuropeanElectionParty", related_name="seats")
    seats = fields.IntField()

    class Meta:
        table = "european_election_party_seat"
        unique_together = (("election", "party"),)
