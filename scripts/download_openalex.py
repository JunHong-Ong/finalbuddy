"""
Download and extract OpenAlex entity data from the public S3 bucket.

Usage:
    python scripts/download_openalex.py domains ./data/domains
    python scripts/download_openalex.py works ./data/works --workers 4
"""

import argparse
import gzip
import json
import shutil
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.request import urlopen, urlretrieve

OPENALEX_BASE_URL = "https://openalex.s3.amazonaws.com"


def _s3_url_to_https(s3_url: str) -> str:
    """Convert s3://openalex/... to https://openalex.s3.amazonaws.com/..."""
    if s3_url.startswith("s3://openalex/"):
        return OPENALEX_BASE_URL + "/" + s3_url[len("s3://openalex/") :]
    return s3_url


def fetch_manifest(entity: str) -> dict:
    """Fetch and parse the manifest file for the given entity type."""
    url = f"{OPENALEX_BASE_URL}/data/{entity}/manifest"
    print(f"Fetching manifest: {url}")
    with urlopen(url) as response:
        return json.load(response)


def download_and_extract(url: str, dest_dir: Path) -> Path:
    """
    Download a single .gz file and extract it into dest_dir.
    Returns the path of the extracted file.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)

    # Derive a filename from the URL, e.g. "updated_date=2024-01-01__part_000.gz"
    parts = url.split("/")
    partition = parts[-2]  # e.g. "updated_date=2024-01-01"
    filename = parts[-1]  # e.g. "part_000.gz"
    gz_path = dest_dir / f"{partition}__{filename}"
    out_path = gz_path.with_suffix("")  # strip .gz

    if out_path.exists():
        print(f"  skip (already extracted): {out_path.name}")
        return out_path

    print(f"  downloading: {gz_path.name}")
    urlretrieve(url, gz_path)

    print(f"  extracting: {out_path.name}")
    with gzip.open(gz_path, "rb") as f_in, open(out_path, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)

    gz_path.unlink()
    return out_path


def download_entity(entity: str, dest_dir: str | Path, workers: int = 1) -> list[Path]:
    """
    Download all data files for an OpenAlex entity type.

    Args:
        entity:   Entity name as it appears in the S3 path, e.g. "domains", "works".
        dest_dir: Local directory to write extracted files into.
        workers:  Number of parallel download threads.

    Returns:
        List of paths to extracted files.
    """
    dest = Path(dest_dir)
    manifest = fetch_manifest(entity)

    entries = manifest.get("entries", [])
    if not entries:
        print("Manifest contains no entries.")
        return []

    total = len(entries)
    print(f"Found {total} file(s) to download.")

    urls = [_s3_url_to_https(entry["url"]) for entry in entries]
    results: list[Path] = []

    if workers > 1:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(download_and_extract, url, dest): url for url in urls
            }
            for i, future in enumerate(as_completed(futures), 1):
                try:
                    path = future.result()
                    results.append(path)
                    print(f"[{i}/{total}] done: {path.name}")
                except Exception as exc:
                    print(
                        f"[{i}/{total}] ERROR for {futures[future]}: {exc}",
                        file=sys.stderr,
                    )
    else:
        for i, url in enumerate(urls, 1):
            try:
                path = download_and_extract(url, dest)
                results.append(path)
                print(f"[{i}/{total}] done: {path.name}")
            except Exception as exc:
                print(f"[{i}/{total}] ERROR for {url}: {exc}", file=sys.stderr)

    print(f"\nFinished. {len(results)}/{total} files extracted to: {dest}")
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Download OpenAlex entity data.")
    parser.add_argument("entity", help="Entity type, e.g. domains, works, authors")
    parser.add_argument("dest_dir", help="Local directory to store extracted files")
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of parallel download threads (default: 1)",
    )
    args = parser.parse_args()

    download_entity(args.entity, args.dest_dir, workers=args.workers)


if __name__ == "__main__":
    main()
