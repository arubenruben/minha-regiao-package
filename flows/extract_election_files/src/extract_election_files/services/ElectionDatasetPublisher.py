# ElectionDatasetPublisher was structurally identical to
# minha_regiao.loader.HuggingFaceLoader (build a datasets.Dataset from
# pydantic records and push it to the Hub) -- kept as a thin re-export so
# anything still importing this exact path continues to work. See
# flows/CLAUDE.md's "Composable Loader abstraction" section.
from minha_regiao.loader.HuggingFaceLoader import HuggingFaceLoader as ElectionDatasetPublisher

__all__ = ["ElectionDatasetPublisher"]
