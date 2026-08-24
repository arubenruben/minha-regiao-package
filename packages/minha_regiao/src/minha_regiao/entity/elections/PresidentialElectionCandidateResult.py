from tortoise.models import Model
from tortoise import fields


class PresidentialElectionCandidateResult(Model):
    id = fields.IntField(primary_key=True)
    result = fields.ForeignKeyField("models.PresidentialElectionResult", related_name="candidate_results")
    candidate = fields.ForeignKeyField("models.PresidentialElectionCandidate", related_name="results")
    votes = fields.IntField()

    class Meta:
        table = "presidential_election_candidate_result"
        unique_together = (("result", "candidate"),)
