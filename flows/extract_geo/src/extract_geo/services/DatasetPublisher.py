# `DatasetPublisher` was moved to `minha_regiao.loader.HuggingFaceLoader` as
# the canonical Loader implementation for Hugging Face publishing (see
# flows/CLAUDE.md's "Composable Loader abstraction" section). This module is
# kept as a thin re-export so anything still importing this exact path
# continues to work.
from minha_regiao.loader.HuggingFaceLoader import HuggingFaceLoader as DatasetPublisher

__all__ = ["DatasetPublisher"]
