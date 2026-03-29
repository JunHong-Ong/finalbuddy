from datetime import datetime
from uuid import UUID

from pydantic import BaseModel

from bookbuddy_models.base import BaseDocument


class BaseNode(BaseModel):
    id: UUID
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
    status: str


# ---------------------------------------------------------------------------
# OpenAlex ontology nodes
# ---------------------------------------------------------------------------


class OpenAlexNode(BaseModel):
    openalex_id: str
    display_name: str
    description: str | None = None
    wikidata_id: str | None = None
    wikipedia_id: str | None = None
    works_count: int
    cited_by_count: int


class Domain(BaseNode, OpenAlexNode):
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


class Keyword(BaseNode):
    """
    Keyword attached to a Topic.

    (:Keyword)-[:DESCRIBES]->(:Topic)
    """

    display_name: str


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
