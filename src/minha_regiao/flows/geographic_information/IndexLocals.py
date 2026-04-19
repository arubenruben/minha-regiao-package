from huggingface_hub import login
from datasets import load_dataset, Dataset
from prefect import flow, task, get_run_logger
from minha_regiao.flows.geographic_information.Settings import Settings

settings = Settings()

@task(name="Login to Hugging Face")
def login_to_hf():
    login(token=settings.hf_api_key)

@task(name="Load Dataset")
def fetch_dataset()-> Dataset:
    dataset = load_dataset(settings.hf_repo_name, split="train")
    
    return dataset

@task(name="Download Election Files")
def download_files():
    pass

@task(name="Upload Files to Hugging Face Repo")
def process_files():
    pass

@task(name="Aggregate Results")
def aggregate_results():
    pass

@task(name="Insert into Database")
def insert_into_db():
    pass


@flow(name="Index Local Election Files")
def index_locals():
    dataset = fetch_dataset()

if __name__ == "__main__":
    index_locals()