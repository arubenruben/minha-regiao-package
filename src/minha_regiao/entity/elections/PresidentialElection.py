from minha_regiao.entity.elections.Election import Election


class PresidentialElection(Election):
    class Meta:
        table = "presidential_election"
