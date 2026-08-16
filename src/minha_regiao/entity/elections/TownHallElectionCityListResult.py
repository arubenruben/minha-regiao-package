from tortoise.models import Model
from tortoise import fields


class TownHallElectionCityListResult(Model):
    """
    For parish assembly elections a city aggregates several independent
    per-parish contests, so more than one list row can legitimately share
    the same name under the same city_result (e.g. "Lista A" in two
    different parishes) — they are still distinct lists.
    """

    id = fields.IntField(primary_key=True)
    city_result = fields.ForeignKeyField("models.TownHallElectionCityResult", related_name="list_results")
    list = fields.ForeignKeyField("models.TownHallElectionList", related_name="city_results")
    votes = fields.IntField()

    class Meta:
        table = "town_hall_election_city_list_result"
        unique_together = (("city_result", "list"),)
