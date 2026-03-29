"""
Combine downloaded OpenAlex entity files (NDJSON) from a local directory into one CSV.

Each file in the directory is treated as newline-delimited JSON. Nested dicts are
flattened with dot-separated keys (e.g. "ids.openalex"). Lists are kept as JSON strings.

Usage:
    python scripts/combine_openalex.py ./data/domains domains.csv
    python scripts/combine_openalex.py ./data/domains domains.csv --glob "*.ndjson"
"""

import argparse
import csv
import json
import sys
from pathlib import Path


def flatten(obj: dict, prefix: str = "", sep: str = ".") -> dict[str, str]:
    """Recursively flatten a nested dict. Lists are serialised to JSON strings."""
    result: dict[str, str] = {}
    for key, value in obj.items():
        full_key = f"{prefix}{sep}{key}" if prefix else key
        if isinstance(value, dict):
            result.update(flatten(value, full_key, sep))
        elif isinstance(value, list):
            result[full_key] = json.dumps(value, ensure_ascii=False)
        elif value is None:
            result[full_key] = ""
        else:
            result[full_key] = str(value)
    return result


def iter_records(directory: Path, glob: str) -> tuple[list[dict], list[str]]:
    """
    Yield flattened records from all matching files and collect the full column set.
    Returns (records, ordered_columns).
    """
    files = sorted(directory.glob(glob))
    if not files:
        print(f"No files matched '{glob}' in {directory}", file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(files)} file(s).")

    records: list[dict] = []
    columns: list[str] = []
    seen_columns: set[str] = set()

    for i, path in enumerate(files, 1):
        print(f"  [{i}/{len(files)}] reading: {path.name}")
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = flatten(json.loads(line))
                except json.JSONDecodeError as exc:
                    print(f"    WARNING: skipping malformed line — {exc}", file=sys.stderr)
                    continue

                # Track column order by first appearance
                for col in record:
                    if col not in seen_columns:
                        seen_columns.add(col)
                        columns.append(col)

                records.append(record)

    return records, columns


def combine(directory: str | Path, output: str | Path, glob: str = "*") -> Path:
    """
    Read all NDJSON files in *directory* and write a combined CSV to *output*.

    Args:
        directory: Local directory containing extracted NDJSON files.
        output:    Destination CSV file path.
        glob:      Glob pattern to filter files within the directory (default: "*").

    Returns:
        Path to the written CSV file.
    """
    src = Path(directory)
    if not src.is_dir():
        print(f"Directory not found: {src}", file=sys.stderr)
        sys.exit(1)

    records, columns = iter_records(src, glob)

    if not records:
        print("No records found.", file=sys.stderr)
        sys.exit(1)

    dest = Path(output)
    dest.parent.mkdir(parents=True, exist_ok=True)

    print(f"\nWriting {len(records):,} records with {len(columns)} columns to: {dest}")
    with open(dest, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for record in records:
            writer.writerow(record)

    print("Done.")
    return dest


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Combine OpenAlex NDJSON files in a directory into one CSV."
    )
    parser.add_argument("directory", help="Directory containing extracted NDJSON files")
    parser.add_argument("output", help="Output CSV file path")
    parser.add_argument(
        "--glob",
        default="*",
        help="Glob pattern to filter files in the directory (default: '*')",
    )
    args = parser.parse_args()

    combine(args.directory, args.output, glob=args.glob)


if __name__ == "__main__":
    main()
