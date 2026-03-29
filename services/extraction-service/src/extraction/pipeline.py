from bookbuddy_models import Chunk, Entity, ExtractionResult

from extraction.keyword_processor import get_processor


def _run_flashtext_layer(chunk: Chunk) -> list[Entity]:
    processor = get_processor()
    matches = processor.extract_keywords(chunk.text, span_info=True)
    return [
        Entity(keyword_id=keyword_id, span=chunk.text[start:end], start=start, end=end)
        for keyword_id, start, end in matches
    ]


def run_pipeline(chunk: Chunk) -> ExtractionResult:
    entities = _run_flashtext_layer(chunk)
    return ExtractionResult(chunk_id=chunk.uuid, entities=entities)
