from tortoise.models import Model
from tortoise import fields


class TownHallElectionListResult(Model):
    id = fields.IntField(primary_key=True)
    result = fields.ForeignKeyField("models.TownHallElectionResult", related_name="list_results")
    list = fields.ForeignKeyField("models.TownHallElectionList", related_name="results")
    votes = fields.IntField()

    class Meta:
        table = "town_hall_election_list_result"
        unique_together = (("result", "list"),)
