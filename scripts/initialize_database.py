#!/usr/bin/env python3
"""
Initialize Neo4j database with OpenAlex taxonomy data.

Usage:
    python scripts/initialize_database.py [--csv-dir PATH] [--batch-size N]

Reads connection settings from environment variables (or .env file):
    NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD
"""

import argparse
import csv
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

from neo4j import GraphDatabase


BATCH_SIZE = 10000


def to_int(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        return int(value.strip())
    except (ValueError, AttributeError):
        return None


def read_csv(path: Path) -> list[dict]:
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def batched(rows: list, size: int):
    for i in range(0, len(rows), size):
        yield rows[i : i + size]


# ---------------------------------------------------------------------------
# Constraints
# ---------------------------------------------------------------------------

CONSTRAINTS = [
    ("openalex_id_Concept_uniq", "Concept"),
    ("openalex_id_Domain_uniq", "Domain"),
    ("openalex_id_Field_uniq", "Field"),
    ("openalex_id_Keyword_uniq", "Keyword"),
    ("openalex_id_Subfield_uniq", "Subfield"),
    ("openalex_id_Topic_uniq", "Topic"),
]


def create_constraints(session) -> None:
    print("Creating constraints...")
    for name, label in CONSTRAINTS:
        session.run(
            f"CREATE CONSTRAINT `{name}` IF NOT EXISTS "
            f"FOR (n:`{label}`) REQUIRE (n.`openalex_id`) IS UNIQUE"
        )
        print(f"  {name} — OK")


# ---------------------------------------------------------------------------
# Indexes
# ---------------------------------------------------------------------------

INDEXES = [
    ("display_name_Keyword", "Keyword", "display_name"),
]


def create_indexes(session) -> None:
    print("Creating indexes...")
    for name, label, field in INDEXES:
        session.run(
            f"CREATE INDEX `{name}` IF NOT EXISTS "
            f"FOR (n:`{label}`) ON (n.`{field}`)"
        )
        print(f"  {name} — OK")


# ---------------------------------------------------------------------------
# Node loaders
# ---------------------------------------------------------------------------

def _run_node_batch(session, query: str, rows: list[dict], csv_path: Path, batch_size: int) -> None:
    total = len(rows)
    print(f"  Loading {total} rows from {csv_path.name}...")
    loaded = 0
    for b in batched(rows, batch_size):
        session.run(query, batch=b, ts=datetime.now(timezone.utc))
        loaded += len(b)
        print(f"    {loaded}/{total}", end="\r")
    print(f"    {total}/{total} — done")


def load_concepts(session, csv_dir: Path, batch_size: int) -> None:
    print("Loading Concept nodes...")
    rows = [
        {
            "openalex_id": r["ids.openalex"],
            "uuid": str(uuid.uuid4()),
            "display_name": r.get("display_name"),
            "level": to_int(r.get("level")),
            "description": r.get("description"),
            "wikidata_id": r.get("ids.wikidata"),
            "wikipedia_id": r.get("ids.wikipedia"),
            "umls_aui_id": r.get("ids.umls_aui"),
            "umls_cui_id": r.get("ids.umls_cui"),
            "mag_id": r.get("ids.mag"),
            "works_count": to_int(r.get("works_count")),
            "cited_by_count": to_int(r.get("cited_by_count")),
        }
        for r in read_csv(csv_dir / "concepts.csv")
        if r.get("ids.openalex")
    ]
    query = """
    UNWIND $batch AS row
    MERGE (n:`Concept` {openalex_id: row.openalex_id})
    SET n.openalex_id   = row.openalex_id,
        n.uuid          = row.uuid,
        n.display_name  = row.display_name,
        n.level         = row.level,
        n.description   = row.description,
        n.wikidata_id   = row.wikidata_id,
        n.wikipedia_id  = row.wikipedia_id,
        n.umls_aui_id   = row.umls_aui_id,
        n.umls_cui_id   = row.umls_cui_id,
        n.mag_id        = row.mag_id,
        n.works_count   = row.works_count,
        n.cited_by_count= row.cited_by_count,
        n.created_at    = $ts,
        n.updated_at    = $ts
    """
    _run_node_batch(session, query, rows, csv_dir / "concepts.csv", batch_size)


def load_domains(session, csv_dir: Path, batch_size: int) -> None:
    print("Loading Domain nodes...")
    rows = [
        {
            "openalex_id": r["ids.openalex"],
            "uuid": str(uuid.uuid4()),
            "display_name": r.get("display_name"),
            "description": r.get("description"),
            "wikidata_id": r.get("ids.wikidata"),
            "wikipedia_id": r.get("ids.wikipedia"),
            "works_count": to_int(r.get("works_count")),
            "cited_by_count": to_int(r.get("cited_by_count")),
        }
        for r in read_csv(csv_dir / "domains.csv")
        if r.get("ids.openalex")
    ]
    query = """
    UNWIND $batch AS row
    MERGE (n:`Domain` {openalex_id: row.openalex_id})
    SET n.openalex_id   = row.openalex_id,
        n.uuid          = row.uuid,
        n.display_name  = row.display_name,
        n.description   = row.description,
        n.wikidata_id   = row.wikidata_id,
        n.wikipedia_id  = row.wikipedia_id,
        n.works_count   = row.works_count,
        n.cited_by_count= row.cited_by_count,
        n.created_at    = $ts,
        n.updated_at    = $ts
    """
    _run_node_batch(session, query, rows, csv_dir / "domains.csv", batch_size)


def load_fields(session, csv_dir: Path, batch_size: int) -> None:
    print("Loading Field nodes...")
    rows = [
        {
            "openalex_id": r["ids.openalex"],
            "uuid": str(uuid.uuid4()),
            "display_name": r.get("display_name"),
            "description": r.get("description"),
            "wikidata_id": r.get("ids.wikidata"),
            "wikipedia_id": r.get("ids.wikipedia"),
            "works_count": to_int(r.get("works_count")),
            "cited_by_count": to_int(r.get("cited_by_count")),
        }
        for r in read_csv(csv_dir / "fields.csv")
        if r.get("ids.openalex")
    ]
    query = """
    UNWIND $batch AS row
    MERGE (n:`Field` {openalex_id: row.openalex_id})
    SET n.openalex_id   = row.openalex_id,
        n.uuid          = row.uuid,
        n.display_name  = row.display_name,
        n.description   = row.description,
        n.wikidata_id   = row.wikidata_id,
        n.wikipedia_id  = row.wikipedia_id,
        n.works_count   = row.works_count,
        n.cited_by_count= row.cited_by_count,
        n.created_at    = $ts,
        n.updated_at    = $ts
    """
    _run_node_batch(session, query, rows, csv_dir / "fields.csv", batch_size)


def load_keywords(session, csv_dir: Path, batch_size: int) -> None:
    print("Loading Keyword nodes...")
    rows = [
        {
            "openalex_id": r["id"],
            "uuid": str(uuid.uuid4()),
            "display_name": r.get("display_name"),
            "works_count": to_int(r.get("works_count")),
            "cited_by_count": to_int(r.get("cited_by_count")),
        }
        for r in read_csv(csv_dir / "keywords.csv")
        if r.get("id")
    ]
    query = """
    UNWIND $batch AS row
    MERGE (n:`Keyword` {openalex_id: row.openalex_id})
    SET n.openalex_id   = row.openalex_id,
        n.uuid          = row.uuid,
        n.display_name  = row.display_name,
        n.works_count   = row.works_count,
        n.cited_by_count= row.cited_by_count,
        n.created_at    = $ts,
        n.updated_at    = $ts
    """
    _run_node_batch(session, query, rows, csv_dir / "keywords.csv", batch_size)


def load_subfields(session, csv_dir: Path, batch_size: int) -> None:
    print("Loading Subfield nodes...")
    rows = [
        {
            "openalex_id": r["ids.openalex"],
            "uuid": str(uuid.uuid4()),
            "display_name": r.get("display_name"),
            "description": r.get("description"),
            "wikidata_id": r.get("ids.wikidata"),
            "wikipedia_id": r.get("ids.wikipedia"),
            "works_count": to_int(r.get("works_count")),
            "cited_by_count": to_int(r.get("cited_by_count")),
        }
        for r in read_csv(csv_dir / "subfields.csv")
        if r.get("ids.openalex")
    ]
    query = """
    UNWIND $batch AS row
    MERGE (n:`Subfield` {openalex_id: row.openalex_id})
    SET n.openalex_id   = row.openalex_id,
        n.uuid          = row.uuid,
        n.display_name  = row.display_name,
        n.description   = row.description,
        n.wikidata_id   = row.wikidata_id,
        n.wikipedia_id  = row.wikipedia_id,
        n.works_count   = row.works_count,
        n.cited_by_count= row.cited_by_count,
        n.created_at    = $ts,
        n.updated_at    = $ts
    """
    _run_node_batch(session, query, rows, csv_dir / "subfields.csv", batch_size)


def load_topics(session, csv_dir: Path, batch_size: int) -> None:
    print("Loading Topic nodes...")
    rows = [
        {
            "openalex_id": r["ids.openalex"],
            "uuid": str(uuid.uuid4()),
            "display_name": r.get("display_name"),
            "description": r.get("description"),
            "wikipedia_id": r.get("ids.wikipedia"),
            "works_count": to_int(r.get("works_count")),
            "cited_by_count": to_int(r.get("cited_by_count")),
        }
        for r in read_csv(csv_dir / "topics.csv")
        if r.get("ids.openalex")
    ]
    query = """
    UNWIND $batch AS row
    MERGE (n:`Topic` {openalex_id: row.openalex_id})
    SET n.openalex_id   = row.openalex_id,
        n.uuid          = row.uuid,
        n.display_name  = row.display_name,
        n.description   = row.description,
        n.wikipedia_id  = row.wikipedia_id,
        n.works_count   = row.works_count,
        n.cited_by_count= row.cited_by_count,
        n.created_at    = $ts,
        n.updated_at    = $ts
    """
    _run_node_batch(session, query, rows, csv_dir / "topics.csv", batch_size)


# ---------------------------------------------------------------------------
# Relationship loaders
# ---------------------------------------------------------------------------

def _run_rel_batch(session, query: str, rows: list[dict], csv_path: Path, batch_size: int) -> None:
    total = len(rows)
    print(f"  Loading {total} rows from {csv_path.name}...")
    loaded = 0
    for b in batched(rows, batch_size):
        session.run(query, batch=b)
        loaded += len(b)
        print(f"    {loaded}/{total}", end="\r")
    print(f"    {total}/{total} — done")


def load_domains_has_field(session, csv_dir: Path, batch_size: int) -> None:
    print("Loading Domain-HAS_FIELD->Field relationships...")
    rows = [
        {"source_id": r["source_id"], "target_id": r["target.id"]}
        for r in read_csv(csv_dir / "domains_has_field.csv")
    ]
    query = """
    UNWIND $batch AS row
    MATCH (source:`Domain` {openalex_id: row.source_id})
    MATCH (target:`Field`  {openalex_id: row.target_id})
    MERGE (source)-[r:`HAS_FIELD`]->(target)
    """
    _run_rel_batch(session, query, rows, csv_dir / "domains_has_field.csv", batch_size)


def load_fields_has_subfield(session, csv_dir: Path, batch_size: int) -> None:
    print("Loading Field-HAS_SUBFIELD->Subfield relationships...")
    rows = [
        {"source_id": r["source_id"], "target_id": r["target.id"]}
        for r in read_csv(csv_dir / "fields_has_subfield.csv")
    ]
    query = """
    UNWIND $batch AS row
    MATCH (source:`Field`    {openalex_id: row.source_id})
    MATCH (target:`Subfield` {openalex_id: row.target_id})
    MERGE (source)-[r:`HAS_SUBFIELD`]->(target)
    """
    _run_rel_batch(session, query, rows, csv_dir / "fields_has_subfield.csv", batch_size)


def load_subfields_has_topic(session, csv_dir: Path, batch_size: int) -> None:
    print("Loading Subfield-HAS_TOPIC->Topic relationships...")
    rows = [
        {"source_id": r["source_id"], "target_id": r["target.id"]}
        for r in read_csv(csv_dir / "subfields_has_topic.csv")
    ]
    query = """
    UNWIND $batch AS row
    MATCH (source:`Subfield` {openalex_id: row.source_id})
    MATCH (target:`Topic`    {openalex_id: row.target_id})
    MERGE (source)-[r:`HAS_TOPIC`]->(target)
    """
    _run_rel_batch(session, query, rows, csv_dir / "subfields_has_topic.csv", batch_size)


def load_topics_has_keyword(session, csv_dir: Path, batch_size: int) -> None:
    print("Loading Keyword-DESCRIBES->Topic relationships...")
    rows = [
        {"source_id": r["source_id"], "target_display_name": r["target"]}
        for r in read_csv(csv_dir / "topics_has_keyword.csv")
    ]
    query = """
    UNWIND $batch AS row
    MATCH (source:`Topic`   {openalex_id:  row.source_id})
    MATCH (target:`Keyword` {display_name: row.target_display_name})
    MERGE (source)<-[r:`DESCRIBES`]-(target)
    """
    _run_rel_batch(session, query, rows, csv_dir / "topics_has_keyword.csv", batch_size)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--csv-dir",
        type=Path,
        default=Path("openalex/csv"),
        help="Directory containing the OpenAlex CSV files (default: openalex/csv)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=BATCH_SIZE,
        help=f"Number of rows per transaction batch (default: {BATCH_SIZE})",
    )
    args = parser.parse_args()

    uri = os.environ["NEO4J_URI"]
    user = os.environ["NEO4J_USER"]
    password = os.environ["NEO4J_PASSWORD"]

    print(f"Connecting to {uri}...")
    with GraphDatabase.driver(uri, auth=(user, password)) as driver:
        driver.verify_connectivity()
        print("Connected.\n")

        with driver.session() as session:
            create_constraints(session)
            print()
            create_indexes(session)
            print()

            # Nodes
            load_concepts(session, args.csv_dir, args.batch_size)
            load_domains(session, args.csv_dir, args.batch_size)
            load_fields(session, args.csv_dir, args.batch_size)
            load_keywords(session, args.csv_dir, args.batch_size)
            load_subfields(session, args.csv_dir, args.batch_size)
            load_topics(session, args.csv_dir, args.batch_size)
            print()

            # Relationships
            load_domains_has_field(session, args.csv_dir, args.batch_size)
            load_fields_has_subfield(session, args.csv_dir, args.batch_size)
            load_subfields_has_topic(session, args.csv_dir, args.batch_size)
            load_topics_has_keyword(session, args.csv_dir, args.batch_size)

    print("\nDatabase initialization complete.")


if __name__ == "__main__":
    main()
