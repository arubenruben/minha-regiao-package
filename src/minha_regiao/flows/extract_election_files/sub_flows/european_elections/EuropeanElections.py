from minha_regiao.flows.extract_election_files.factories.ElectionFlowFactory import ElectionFlowFactory
from minha_regiao.flows.extract_election_files.sub_flows.european_elections.services.european_election_service import (
    european_election_matcher,
)
from minha_regiao.flows.extract_election_files.sub_flows.european_elections.Settings import (
    settings,
)

european_elections = ElectionFlowFactory("european", european_election_matcher).build()


if __name__ == "__main__":
    european_elections(settings.base_url)
