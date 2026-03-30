from datetime import datetime
from uuid import UUID

from pydantic import BaseModel

from bookbuddy_models.base import BaseDocument, BaseSegment


class BaseNode(BaseModel):
    uuid: UUID
    created_at: datetime
    updated_at: datetime


class AliasNode(BaseNode):
    """
    Represents an alternative naming to the linked node.

    (:Alias)-[:SYNONYM_FOR]->()
    """

    disply_name: str


# ---------------------------------------------------------------------------
# Document related nodes
# ---------------------------------------------------------------------------


class DocumentNode(BaseDocument):
    """
    Represents an ingested document in the knowledge graph.

    (:DocumentNode)-[:HAS_SEGMENT]->(:SegmentNode)
    """

    status: str
    total_chunks: int | None = None


class SegmentNode(BaseSegment):
    """
    Represents a logical container within a document — e.g. a page, chapter,
    slide, or time window.  Corresponds to a parsed Segment produced by the
    ingestion pipeline.

    (:SegmentNode)-[:HAS_CHUNK]->(:ChunkNode)
    """


class ChunkNode(BaseNode):
    """
    Represents a text chunk produced by the recursive chunker.  A chunk
    stitches together one or more Elements from a Segment into a contiguous
    passage suitable for embedding and retrieval.
    """

    chunk_index: int
    chunk_position: float
    processed: bool
    embedding: list[float]
    text: str


# ---------------------------------------------------------------------------
# OpenAlex ontology nodes
# ---------------------------------------------------------------------------


class OpenAlexNode(BaseNode):
    openalex_id: str
    display_name: str
    description: str | None = None
    wikidata_id: str | None = None
    wikipedia_id: str | None = None
    works_count: int
    cited_by_count: int


class Domain(OpenAlexNode):
    """
    Top-level classification (e.g. 'Physical Sciences').
    
    (:Domain)-[:HAS_FIELD]->(:Field)
    """


class Field(OpenAlexNode):
    """
    Second-level classification.

    (:Field)-[:HAS_SUBFIELD]->(:Subfield)
    """


class Subfield(OpenAlexNode):
    """
    Third-level classification.

    (:Subfield)-[:HAS_TOPIC]->(:Topic)
    """


class Topic(OpenAlexNode):
    """
    Fourth-level classification.
    """


class Keyword(OpenAlexNode):
    """
    Keyword attached to a Topic.

    (:Keyword)-[:DESCRIBES]->(:Topic)
    """


# ---------------------------------------------------------------------------
# OpenAlex ontology nodes (Legacy - Microsoft Academic Graph)
# ---------------------------------------------------------------------------


class Concept(OpenAlexNode):
    """
    Legacy OpenAlex concept (being replaced by Topics in OpenAlex v2).
    """

    level: int  # 0 = root, higher = more specific
    umls_aui_id: str | None = None
    umls_cui_id: str | None = None
    mag_id: str | None = None
