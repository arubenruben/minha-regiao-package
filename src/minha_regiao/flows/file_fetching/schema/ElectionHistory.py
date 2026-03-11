from datetime import date
from typing import List, Literal
from minha_regiao.flows.file_fetching.schema.Schema import Schema
from minha_regiao.flows.file_fetching.schema.Election import Election


class ElectionHistory(Schema):
    election_type: Literal[
        'PR', 
        'AL', 
        'AR', 
        'ALRAM', 
        'PE', 
        'ALRAA', 
        'REF', 
        'AC'
    ]
    election_name: Literal[
        'PRESIDENTE DA REPÚBLICA - 2º Sufrágio',
        'PRESIDENTE DA REPÚBLICA - 1º Sufrágio',
        'AUTARQUIAS LOCAIS',
        'ASSEMBLEIA DA REPÚBLICA',
        'ASSEMBLEIA LEGISLATIVA REGIONAL - MADEIRA',
        'PARLAMENTO EUROPEU',
        'ASSEMBLEIA LEGISLATIVA REGIONAL - AÇORES',
        'REFERENDO NACIONAL (IVG)',
        'REFERENDO NACIONAL (REGIONALIZAÇÃO)',
        'ASSEMBLEIA CONSTITUINTE'
    ]
    election_date: List[date]
    elections: List[Election]

    @staticmethod
    def from_elections(elections: List[Election]) -> List["ElectionHistory"]:
        elections_history_dict = {}
        
        for election in elections:
            key = (election.election_type, election.election_name)
            
            if key not in elections_history_dict:
                elections_history_dict[key] = ElectionHistory(
                    election_type=election.election_type,
                    election_name=election.election_name,
                    election_date=[election.election_date],
                    elections=[election]
                )
            else:
                elections_history_dict[key].election_date.append(election.election_date)
                elections_history_dict[key].elections.append(election)
        
        return list(elections_history_dict.values())