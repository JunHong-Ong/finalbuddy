from gliner2 import GLiNER2

_model: GLiNER2 | None = None


def init_gliner() -> None:
    global _model
    _model = GLiNER2.from_pretrained("fastino/gliner2-base-v1")


def get_gliner() -> GLiNER2:
    if _model is None:
        raise RuntimeError("GLiNER is not initialized")
    return _model
