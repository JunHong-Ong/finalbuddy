from uuid import uuid4

from bookbuddy_models import Chunk, Document


def build_segment_payloads(document: Document, chunks: list[Chunk]) -> list[dict]:
    sections: dict[str, list[Chunk]] = {}
    for chunk in chunks:
        sections.setdefault(chunk.section_title, []).append(chunk)

    return [
        {
            "id": str(uuid4()),
            "index": seg_index,
            "chunks": [
                {"id": str(c.id), "chunk_index": c.chunk_index, "text": c.text}
                for c in section_chunks
            ],
        }
        for seg_index, (_, section_chunks) in enumerate(sections.items())
    ]
