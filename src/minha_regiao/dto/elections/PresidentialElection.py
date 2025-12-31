from pydantic import Field
from typing import Optional, List
from minha_regiao.dto.Election import Election
from minha_regiao.dto.ElectionFile import ElectionFile


class PresidentialElection(Election):
    first_round_file: ElectionFile
    second_round_file: Optional[ElectionFile] = Field(default=None)

    def _prepare_election_files(self) -> List[ElectionFile]:
        self.first_round_file.hf_file_id = (
            f"elections/presidential_first_round_{self.year}.xlsx"
        )

        files = [self.first_round_file]

        if self.second_round_file:
            self.second_round_file.hf_file_id = (
                f"elections/presidential_second_round_{self.year}.xlsx"
            )
            files.append(self.second_round_file)

        return files
