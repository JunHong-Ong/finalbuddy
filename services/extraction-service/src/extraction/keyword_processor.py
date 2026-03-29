from bookbuddy_models import Keyword
from flashtext import KeywordProcessor

_processor: KeywordProcessor | None = None


def init_processor(keywords: list[Keyword]) -> None:
    global _processor
    _processor = KeywordProcessor(case_sensitive=False)
    for kw in keywords:
        _processor.add_keyword(kw.display_name, kw.id)


def get_processor() -> KeywordProcessor:
    if _processor is None:
        raise RuntimeError("KeywordProcessor is not initialized")
    return _processor
