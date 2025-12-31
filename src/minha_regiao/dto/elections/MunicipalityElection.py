from typing import List
from minha_regiao.dto.Election import Election
from minha_regiao.dto.ElectionFile import ElectionFile


class MunicipalityElection(Election):
    town_hall_file: ElectionFile
    municipal_assembly_file: ElectionFile
    parish_assembly_file: ElectionFile

    def _prepare_election_files(self) -> List[ElectionFile]:
        self.town_hall_file.hf_file_id = (
            f"elections/municipality_town_hall_{self.year}.xlsx"
        )
        self.municipal_assembly_file.hf_file_id = (
            f"elections/municipality_municipal_assembly_{self.year}.xlsx"
        )
        self.parish_assembly_file.hf_file_id = (
            f"elections/municipality_parish_assembly_{self.year}.xlsx"
        )

        return [
            self.town_hall_file,
            self.municipal_assembly_file,
            self.parish_assembly_file,
        ]
