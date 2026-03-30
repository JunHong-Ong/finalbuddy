from pathlib import Path
from uuid import uuid4

import yaml
from bookbuddy_models import Chunk, Entity, ExtractionResult

from extraction.gliner import get_gliner
from extraction.keyword_processor import get_processor

_ENTITY_TYPES_PATH = Path(__file__).parent / "entity_types.yaml"


def _load_entity_types() -> dict[str, str]:
    with open(_ENTITY_TYPES_PATH) as f:
        entries = yaml.safe_load(f)
    return {entry["label"]: entry["description"] for entry in entries}


def _run_flashtext_layer(chunk: Chunk) -> list[Entity]:
    processor = get_processor()
    matches = processor.extract_keywords(chunk.text, span_info=True)
    return [
        Entity(keyword_id=keyword_id, span=chunk.text[start:end], start=start, end=end)
        for keyword_id, start, end in matches
    ]


def _run_gliner_ner(chunk: Chunk) -> list[Entity]:
    model = get_gliner()
    entity_types = _load_entity_types()
    entities = model.extract_entities(
        chunk.text,
        entity_types,
        threshold=0.4,
        include_confidence=True,
        include_spans=True,
    )

    return [
        Entity(
            keyword_id=uuid4(),
            span=entity["text"],
            start=entity["start"],
            end=entity["end"],
        )
        for label, values in entities["entities"].items()
        for entity in values
    ]


def run_pipeline(chunk: Chunk) -> ExtractionResult:
    all_entities = []
    flashtext_entities = _run_flashtext_layer(chunk)

    keyword_matches = set([entity.span for entity in flashtext_entities])
    gliner_entities = _run_gliner_ner(chunk)

    all_entities.extend(flashtext_entities)
    all_entities.extend(
        [entity for entity in gliner_entities if entity.span not in keyword_matches]
    )
    return ExtractionResult(chunk_id=chunk.uuid, entities=all_entities)
