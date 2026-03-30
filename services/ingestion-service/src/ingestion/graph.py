from uuid import UUID

from bookbuddy_models import Chunk, Document
from bookbuddy_models.document import Segment


def build_segment_nodes_payload(document: Document) -> list[dict]:
    segments = sorted(document.segments, key=lambda s: s.index)

    # For each segment, find the next segment at the same level
    next_segment_by_level: dict[int, dict[UUID, UUID | None]] = {}
    segments_by_level: dict[int, list[Segment]] = {}
    for seg in segments:
        segments_by_level.setdefault(seg.level, []).append(seg)
    for level_segs in segments_by_level.values():
        next_segment_by_level[level_segs[0].level] = {
            seg.uuid: level_segs[i + 1].uuid if i + 1 < len(level_segs) else None
            for i, seg in enumerate(level_segs)
        }

    # For each segment, find its parent (most recent ancestor at level - 1)
    last_at_level: dict[int, UUID] = {}
    parent_segment_id: dict[UUID, UUID | None] = {}
    for seg in segments:
        parent_segment_id[seg.uuid] = (
            last_at_level.get(seg.level - 1) if seg.level > 0 else None
        )
        last_at_level[seg.level] = seg.uuid

    payloads = []
    for segment in segments:
        next_seg_id = next_segment_by_level[segment.level][segment.uuid]
        payloads.append(
            {
                "uuid": str(segment.uuid),
                "index": segment.index,
                "level": segment.level,
                "next_segment_id": str(next_seg_id) if next_seg_id else None,
                "parent_segment_id": str(parent_segment_id[segment.uuid])
                if parent_segment_id[segment.uuid]
                else None,
            }
        )
    return payloads


def build_chunk_nodes_payload(document: Document, chunks: list[Chunk]) -> list[dict]:
    chunks_by_segment: dict[UUID, list[Chunk]] = {}
    for chunk in chunks:
        chunks_by_segment.setdefault(chunk.segment_id, []).append(chunk)

    segments = sorted(document.segments, key=lambda s: s.index)

    # Build a flat ordered list of all chunks (by segment index, then chunk_index)
    # to wire the NEXT relationship across chunks
    all_chunks: list[Chunk] = []
    for seg in segments:
        all_chunks.extend(
            sorted(chunks_by_segment.get(seg.uuid, []), key=lambda c: c.chunk_index)
        )
    next_chunk_id: dict[UUID, UUID | None] = {
        c.uuid: all_chunks[i + 1].uuid if i + 1 < len(all_chunks) else None
        for i, c in enumerate(all_chunks)
    }

    return [
        {
            "uuid": str(c.uuid),
            "chunk_index": c.chunk_index,
            "chunk_position": c.chunk_position,
            "text": c.text,
            "segment_id": str(c.segment_id),
            "next_chunk_id": str(next_chunk_id[c.uuid]) if next_chunk_id[c.uuid] else None,
        }
        for c in all_chunks
    ]
 