from extract_election_files.factories.ElectionFlowFactory import ElectionFlowFactory
from extract_election_files.sub_flows.presidential_elections.services.presidential_election_service import (
    presidential_election_matcher,
)
from extract_election_files.sub_flows.presidential_elections.Settings import (
    settings,
)

presidential_elections = ElectionFlowFactory("presidential", presidential_election_matcher).build()


if __name__ == "__main__":
    presidential_elections(settings.base_url)
