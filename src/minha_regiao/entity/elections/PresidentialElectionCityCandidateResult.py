from tortoise.models import Model
from tortoise import fields


class PresidentialElectionCityCandidateResult(Model):
    id = fields.IntField(primary_key=True)
    city_result = fields.ForeignKeyField("models.PresidentialElectionCityResult", related_name="candidate_results")
    candidate = fields.ForeignKeyField("models.PresidentialElectionCandidate", related_name="city_results")
    votes = fields.IntField()

    class Meta:
        table = "presidential_election_city_candidate_result"
        unique_together = (("city_result", "candidate"),)
