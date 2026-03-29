"""
Extract entity relationships from OpenAlex NDJSON part files into a CSV.

For each record, resolves a scalar source column and an array target column,
then emits one row per (source, target item) pair.

Output columns:
    source_id    — value of --source-col
    relationship — the relationship name
    target.*     — flattened fields of each target object

Usage:
    python scripts/extract_relationships.py \\
        ./openalex/domains \\
        domains_has_field.csv \\
        --source-col id \\
        --target-col fields \\
        --relationship HAS_FIELD

    # Nested source / target columns use dot notation:
    python scripts/extract_relationships.py \\
        ./openalex/domains \\
        out.csv \\
        --source-col ids.openalex \\
        --target-col siblings \\
        --relationship HAS_SIBLING
"""

import argparse
import csv
import json
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_nested(obj: dict, dotted_key: str):
    """Resolve a dot-separated key path from a dict. Returns None if missing."""
    keys = dotted_key.split(".")
    current = obj
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _flatten_item(obj, prefix: str = "target") -> dict[str, str]:
    """
    Flatten a target item into a dict with keys prefixed by *prefix*.
    Scalars are stringified; nested dicts are recursed; lists become JSON strings.
    """
    if not isinstance(obj, dict):
        # Scalar target value (e.g. a plain string or number)
        return {prefix: "" if obj is None else str(obj)}

    result: dict[str, str] = {}
    for key, value in obj.items():
        full_key = f"{prefix}.{key}"
        if isinstance(value, dict):
            result.update(_flatten_item(value, full_key))
        elif isinstance(value, list):
            result[full_key] = json.dumps(value, ensure_ascii=False)
        elif value is None:
            result[full_key] = ""
        else:
            result[full_key] = str(value)
    return result


# ---------------------------------------------------------------------------
# Core
# ---------------------------------------------------------------------------


def extract_relationships(
    directory: str | Path,
    output: str | Path,
    source_col: str,
    target_col: str,
    relationship: str,
    glob: str = "*",
) -> Path:
    """
    Read all NDJSON part files in *directory* and write a relationship CSV.

    Args:
        directory:    Directory containing NDJSON part files.
        output:       Destination CSV file path.
        source_col:   Dot-separated key for the source value (e.g. "id").
        target_col:   Dot-separated key for the target array (e.g. "fields").
        relationship: Relationship label written to every row (e.g. "HAS_FIELD").
        glob:         Glob pattern to select files (default: "*").

    Returns:
        Path to the written CSV file.
    """
    src = Path(directory)
    if not src.is_dir():
        print(f"Directory not found: {src}", file=sys.stderr)
        sys.exit(1)

    files = sorted(src.glob(glob))
    if not files:
        print(f"No files matched '{glob}' in {src}", file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(files)} file(s).")

    rows: list[dict] = []
    columns: list[str] = []
    seen_columns: set[str] = set()

    skipped_no_source = 0
    skipped_no_target = 0
    skipped_not_list = 0

    def _add_columns(row: dict) -> None:
        for col in row:
            if col not in seen_columns:
                seen_columns.add(col)
                columns.append(col)

    fixed_cols = ["source_id", "relationship"]
    for col in fixed_cols:
        seen_columns.add(col)
        columns.append(col)

    for i, path in enumerate(files, 1):
        print(f"  [{i}/{len(files)}] processing: {path.name}")
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError as exc:
                    print(
                        f"    WARNING: skipping malformed line — {exc}", file=sys.stderr
                    )
                    continue

                source_value = _get_nested(record, source_col)
                if source_value is None:
                    skipped_no_source += 1
                    continue

                target_value = _get_nested(record, target_col)
                if target_value is None:
                    skipped_no_target += 1
                    continue
                if not isinstance(target_value, list):
                    skipped_not_list += 1
                    continue

                for item in target_value:
                    row = {
                        "source_id": str(source_value),
                        "relationship": relationship,
                        **_flatten_item(item),
                    }
                    _add_columns(row)
                    rows.append(row)

    if skipped_no_source:
        print(
            f"  Skipped {skipped_no_source} record(s) — "
            "source column '{source_col}' missing."
        )
    if skipped_no_target:
        print(
            f"  Skipped {skipped_no_target} record(s) — "
            "target column '{target_col}' missing."
        )
    if skipped_not_list:
        print(
            f"  Skipped {skipped_not_list} record(s) — "
            "target column '{target_col}' is not a list."
        )

    if not rows:
        print("No relationship rows produced.", file=sys.stderr)
        sys.exit(1)

    dest = Path(output)
    dest.parent.mkdir(parents=True, exist_ok=True)

    print(f"\nWriting {len(rows):,} rows with columns: {columns}")
    with open(dest, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    print(f"Done → {dest}")
    return dest


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract entity relationships from OpenAlex JSON files into a CSV."
    )
    parser.add_argument("directory", help="Directory containing JSON part files")
    parser.add_argument("output", help="Output CSV file path")
    parser.add_argument(
        "--source-col",
        required=True,
        help="Dot-separated column for the source node ID (e.g. 'id')",
    )
    parser.add_argument(
        "--target-col",
        required=True,
        help="Dot-separated column containing the target array (e.g. 'fields')",
    )
    parser.add_argument(
        "--relationship",
        required=True,
        help="Relationship label written to every row (e.g. 'HAS_FIELD')",
    )
    parser.add_argument(
        "--glob",
        default="*",
        help="Glob pattern to filter files in the directory (default: '*')",
    )
    args = parser.parse_args()

    extract_relationships(
        args.directory,
        args.output,
        source_col=args.source_col,
        target_col=args.target_col,
        relationship=args.relationship,
        glob=args.glob,
    )


if __name__ == "__main__":
    main()
