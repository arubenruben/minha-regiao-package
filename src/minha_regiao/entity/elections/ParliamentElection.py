from minha_regiao.entity.elections.Election import Election


class ParliamentElection(Election):
    class Meta:
        table = "parliament_election"
