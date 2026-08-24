from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=Path(__file__).with_name(".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    election_results_dataset_repo_id: str = "minharegiao/portuguese-elections"

    # Freguesias existing before the 2013 "Lei Relvas" aggregation.
    pre_2013_election_results_filename: str = "raw/presidential/PR_2006.xlsx"
    # Freguesias resulting from the 2013 aggregation, in effect until the 2021 reversal law.
    post_2013_election_results_filename: str = "raw/presidential/PR_2021_Globais.xlsx"
    # Current freguesias, reflecting reversals allowed since Lei n.º 39/2021.
    post_2021_election_results_filename: str = "raw/presidential/PR_2026_Globais.xlsx"

    parish_dataset_config_name: str = "parishes"


settings = Settings()
