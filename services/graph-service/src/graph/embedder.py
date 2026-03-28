from sentence_transformers import SentenceTransformer

from graph.config import EMBEDDING_MODEL

_model: SentenceTransformer | None = None


def load_embedder() -> None:
    global _model
    _model = SentenceTransformer(EMBEDDING_MODEL)


def embed(text: str) -> list[float]:
    if _model is None:
        raise RuntimeError("Embedder is not loaded. Call load_embedder() first.")
    return _model.encode(text, convert_to_numpy=True).tolist()
