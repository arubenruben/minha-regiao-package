from extract_election_files.factories.ElectionFlowFactory import ElectionFlowFactory
from extract_election_files.sub_flows.parliament_elections.services.parliament_election_service import (
    parliament_election_matcher,
)
from extract_election_files.sub_flows.parliament_elections.Settings import (
    settings,
)

parliament_elections = ElectionFlowFactory("parliament", parliament_election_matcher).build()


if __name__ == "__main__":
    parliament_elections(settings.base_url)
