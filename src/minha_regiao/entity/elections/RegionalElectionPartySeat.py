from tortoise.models import Model
from tortoise import fields


class RegionalElectionPartySeat(Model):
    """
    Seats won by a party in a regional assembly election. Each region
    (Madeira, Açores) is a single circle for its own assembly — the region
    is already fixed by the election's sub_type — so this is one row per
    party per election rather than per circle.
    """

    id = fields.IntField(primary_key=True)
    election = fields.ForeignKeyField("models.RegionalElection", related_name="party_seats")
    party = fields.ForeignKeyField("models.RegionalElectionParty", related_name="seats")
    seats = fields.IntField()

    class Meta:
        table = "regional_election_party_seat"
        unique_together = (("election", "party"),)
