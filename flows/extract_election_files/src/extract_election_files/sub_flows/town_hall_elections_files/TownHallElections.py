from extract_election_files.factories.ElectionFlowFactory import ElectionFlowFactory
from extract_election_files.sub_flows.town_hall_elections_files.services.town_hall_election_service import (
    town_hall_election_matcher,
)
from extract_election_files.sub_flows.town_hall_elections_files.Settings import (
    settings,
)

town_hall_elections = ElectionFlowFactory("town_hall", town_hall_election_matcher).build()


if __name__ == "__main__":
    town_hall_elections(settings.base_url)
