#!/usr/bin/env bash
set -euo pipefail

# Download and combine OpenAlex taxonomy data into CSVs,
# and extract relationships for graph ingestion.

# Concepts
python scripts/download_openalex.py concepts ./openalex/concepts
python scripts/combine_openalex.py ./openalex/concepts ./openalex/csv/concepts.csv

# Domains
python scripts/download_openalex.py domains ./openalex/domains
python scripts/combine_openalex.py ./openalex/domains ./openalex/csv/domains.csv
python scripts/extract_relationships.py ./openalex/domains ./openalex/csv/domains_has_field.csv \
    --source-col id --target-col fields --relationship HAS_FIELD

# Fields
python scripts/download_openalex.py fields ./openalex/fields
python scripts/combine_openalex.py ./openalex/fields ./openalex/csv/fields.csv
python scripts/extract_relationships.py ./openalex/fields ./openalex/csv/fields_has_subfield.csv \
    --source-col id --target-col subfields --relationship HAS_SUBFIELD

# Keywords
python scripts/download_openalex.py keywords ./openalex/keywords
python scripts/combine_openalex.py ./openalex/keywords ./openalex/csv/keywords.csv

# Subfields
python scripts/download_openalex.py subfields ./openalex/subfields
python scripts/combine_openalex.py ./openalex/subfields ./openalex/csv/subfields.csv
python scripts/extract_relationships.py ./openalex/subfields ./openalex/csv/subfields_has_topic.csv \
    --source-col id --target-col topics --relationship HAS_TOPIC

# Topics
python scripts/download_openalex.py topics ./openalex/topics
python scripts/combine_openalex.py ./openalex/topics ./openalex/csv/topics.csv
python scripts/extract_relationships.py ./openalex/topics ./openalex/csv/topics_has_keyword.csv \
    --source-col id --target-col keywords --relationship HAS_KEYWORD
