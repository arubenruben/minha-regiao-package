from typing import List
from minha_regiao.dto.Election import Election
from minha_regiao.dto.ElectionFile import ElectionFile


class EuropeanElection(Election):
    election_file: ElectionFile

    def _prepare_election_files(self) -> List[ElectionFile]:
        self.election_file.hf_file_id = f"elections/european_{self.year}.xlsx"

        return [self.election_file]
