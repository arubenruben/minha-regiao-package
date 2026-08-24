from tortoise.models import Model
from tortoise import fields


class PresidentialElectionDistrictCandidateResult(Model):
    id = fields.IntField(primary_key=True)
    district_result = fields.ForeignKeyField(
        "models.PresidentialElectionDistrictResult", related_name="candidate_results"
    )
    candidate = fields.ForeignKeyField("models.PresidentialElectionCandidate", related_name="district_results")
    votes = fields.IntField()

    class Meta:
        table = "presidential_election_district_candidate_result"
        unique_together = (("district_result", "candidate"),)
