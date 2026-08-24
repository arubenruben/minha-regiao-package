from extract_election_files.factories.ElectionFlowFactory import ElectionFlowFactory
from extract_election_files.sub_flows.regional_elections.services.regional_election_service import (
    regional_election_matcher,
)
from extract_election_files.sub_flows.regional_elections.Settings import (
    settings,
)

regional_elections = ElectionFlowFactory("regional", regional_election_matcher).build()


if __name__ == "__main__":
    regional_elections(settings.base_url)
