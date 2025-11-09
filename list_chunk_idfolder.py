#!/usr/bin/env python3
"""
list_and_chunk_id_folders.py

- Loads a list of IDs (txt/csv).
- Maps each ID to an existing folder inside the source directory.
- Lists files for each mapped folder (optional recursion).
- Splits the mapped folder list into chunks (batches).
- Optionally saves chunk files (json/text) or processes each chunk (copy or move).

Usage examples:
  # Print mapping and split into chunks of 50 (just print)
  python list_and_chunk_id_folders.py -s "/path/to/main" -i ids.txt --chunk-size 50

  # Save chunk files (json) with prefix chunk_
  python list_and_chunk_id_folders.py -s "/path/to/main" -i ids.txt --chunk-size 100 --save-chunks --chunk-prefix chunk_

  # Copy folders chunk-by-chunk into /tmp/dest (for each chunk, create subfolder chunk_1, chunk_2...)
  python list_and_chunk_id_folders.py -s "/path/to/main" -i ids.txt --chunk-size 20 --process-chunks copy --process-dest "/tmp/dest"

Author: Your Name
"""

from pathlib import Path
import argparse
import csv
import json
import shutil
from typing import List, Dict, Tuple, Iterable


# ---------- ID loading helpers (same behavior as previous script) ----------
def read_ids_from_txt(path: Path) -> List[str]:
    ids = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            s = line.strip()
            if s:
                ids.append(s)
    return ids


def read_ids_from_csv(path: Path, column: str = None) -> List[str]:
    ids = []
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        if reader.fieldnames:
            if column and column in reader.fieldnames:
                for row in reader:
                    val = (row.get(column) or "").strip()
                    if val:
                        ids.append(val)
            else:
                first_col = reader.fieldnames[0]
                for row in reader:
                    val = (row.get(first_col) or "").strip()
                    if val:
                        ids.append(val)
        else:
            fh.seek(0)
            simple_reader = csv.reader(fh)
            for row in simple_reader:
                if row:
                    val = row[0].strip()
                    if val:
                        ids.append(val)
    return ids


def load_ids(ids_path: str, csv_column: str = None) -> List[str]:
    p = Path(ids_path)
    if not p.exists():
        raise FileNotFoundError(f"IDs file not found: {ids_path}")
    suffix = p.suffix.lower()
    if suffix == ".txt":
        return read_ids_from_txt(p)
    elif suffix == ".csv":
        return read_ids_from_csv(p, column=csv_column)
    else:
        return read_ids_from_txt(p)


# ---------- Mapping and listing ----------
def map_ids_to_folders(source_folder: str, ids: Iterable[str]) -> Tuple[Dict[str, Path], List[str]]:
    """
    For each ID, check if a folder named exactly as the ID exists inside source_folder.
    Returns:
      - mapping: dict ID -> Path (only for existing folders)
      - missing: list of IDs that were not found or not directories
    """
    src = Path(source_folder)
    if not src.exists() or not src.is_dir():
        raise NotADirectoryError(f"Source folder does not exist or is not a directory: {source_folder}")

    mapping: Dict[str, Path] = {}
    missing: List[str] = []

    for id_ in ids:
        candidate = src / id_
        if candidate.exists() and candidate.is_dir():
            mapping[id_] = candidate
        else:
            missing.append(id_)
    return mapping, missing


def list_files_in_folder(folder: Path, recursive: bool = True) -> List[str]:
    """Return list of file paths inside folder (absolute strings)."""
    files: List[str] = []
    if recursive:
        for p in folder.rglob("*"):
            if p.is_file():
                files.append(str(p.resolve()))
    else:
        for p in folder.iterdir():
            if p.is_file():
                files.append(str(p.resolve()))
    files.sort()
    return files


# ---------- Chunking helpers ----------
def chunkify(items: List, chunk_size: int) -> List[List]:
    """Split items into chunks of size chunk_size. Last chunk may be smaller."""
    if chunk_size <= 0:
        raise ValueError("chunk_size must be > 0")
    return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]


# ---------- Saving helpers ----------
def save_chunk_json(chunk: List[str], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as fh:
        json.dump(chunk, fh, indent=2, ensure_ascii=False)


def save_chunk_text(chunk: List[str], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as fh:
        for item in chunk:
            fh.write(f"{item}\n")


# ---------- Processing helpers (copy/move) ----------
def copy_chunk_folders(chunk_mapping: Dict[str, Path], dest_root: Path, chunk_name: str) -> None:
    """
    Copy the set of folders (mapping ID->Path) into dest_root/<chunk_name>/
    Each folder preserves its name (ID).
    """
    dest = dest_root / chunk_name
    dest.mkdir(parents=True, exist_ok=True)
    for id_, src_path in chunk_mapping.items():
        target = dest / id_
        # shutil.copytree requires target not exist, so remove existing first if present:
        if target.exists():
            # be careful: only remove if it's a directory (prevent accidental file removal)
            if target.is_dir():
                shutil.rmtree(target)
            else:
                target.unlink()
        shutil.copytree(src_path, target)


def move_chunk_folders(chunk_mapping: Dict[str, Path], dest_root: Path, chunk_name: str) -> None:
    """
    Move the set of folders (mapping ID->Path) into dest_root/<chunk_name>/
    """
    dest = dest_root / chunk_name
    dest.mkdir(parents=True, exist_ok=True)
    for id_, src_path in chunk_mapping.items():
        target = dest / id_
        if target.exists():
            if target.is_dir():
                shutil.rmtree(target)
            else:
                target.unlink()
        shutil.move(str(src_path), str(target))


# ---------- CLI and orchestration ----------
def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Map ID-named subfolders, list files, and split into chunks for saving/processing.")
    p.add_argument("--source", "-s", required=True, help="Path to main folder containing ID-named subfolders.")
    p.add_argument("--ids", "-i", required=True, help="Path to IDs file (.txt or .csv).")
    p.add_argument("--csv-column", "-c", default=None, help="(Optional) column name to read from when IDs file is CSV.")
    p.add_argument("--recursive", action="store_true", default=True, help="Recursively list files inside matched folders (default: True).")
    p.add_argument("--no-recursive", dest="recursive", action="store_false", help="Do not recurse; only list top-level files.")
    p.add_argument("--chunk-size", type=int, default=0, help="If >0, split matched IDs into chunks of this size.")
    p.add_argument("--save-chunks", action="store_true", help="Save each chunk to disk as files (json or text).")
    p.add_argument("--chunk-format", choices=["json", "text"], default="json", help="Format to save chunk files.")
    p.add_argument("--chunk-prefix", default="chunk_", help="Prefix for chunk files/folders, e.g., chunk_1.json or chunk_1")
    p.add_argument("--chunks-dir", default="chunks", help="Directory to write chunk files or processed chunk folders.")
    p.add_argument("--process-chunks", choices=["none", "copy", "move"], default="none",
                   help="Optionally process each chunk by copying or moving the chunk folders into chunks-dir/<chunk_name>.")
    p.add_argument("--process-dest", default=None, help="Destination root for processing (required when using copy/move).")
    p.add_argument("--print-only", action="store_true", help="Only print mapping and chunk summary; do not save or process.")
    return p


def main():
    parser = build_arg_parser()
    args = parser.parse_args()

    ids = load_ids(args.ids, csv_column=args.csv_column)
    if not ids:
        print("No IDs loaded from file.")
        return

    mapping, missing = map_ids_to_folders(args.source, ids)

    print(f"Total IDs provided: {len(ids)}")
    print(f"Found folders: {len(mapping)}")
    print(f"Missing/non-existent folders: {len(missing)}")
    if missing:
        print("Missing IDs (first 50 shown):")
        for m in missing[:50]:
            print(f"  - {m}")

    # collect entries: list of IDs that exist (ordered same as input order)
    existing_ids_in_order = [id_ for id_ in ids if id_ in mapping]

    # If requested, list files per folder
    detailed_map: Dict[str, List[str]] = {}
    for id_ in existing_ids_in_order:
        folder = mapping[id_]
        files = list_files_in_folder(folder, recursive=args.recursive)
        detailed_map[id_] = files

    # Print a short sample
    print("\nSample mapping + file counts (first 20):")
    for id_ in existing_ids_in_order[:20]:
        print(f"  {id_} -> {mapping[id_]} (files: {len(detailed_map[id_])})")

    # Chunking if requested
    if args.chunk_size and args.chunk_size > 0:
        chunks = chunkify(existing_ids_in_order, args.chunk_size)
        print(f"\nTotal chunks: {len(chunks)} (chunk size: {args.chunk_size})")

        # Show summary of each chunk
        for idx, ch in enumerate(chunks, start=1):
            print(f"  Chunk {idx}: {len(ch)} items (IDs: {ch[0]} ... {ch[-1]})")

        # Save chunk files if requested
        if args.save_chunks and not args.print_only:
            chunks_dir = Path(args.chunks_dir)
            chunks_dir.mkdir(parents=True, exist_ok=True)

            for idx, ch in enumerate(chunks, start=1):
                chunk_name = f"{args.chunk_prefix}{idx}"
                out_path = chunks_dir / f"{chunk_name}.{args.chunk_format}"
                if args.chunk_format == "json":
                    save_chunk_json(ch, out_path)
                else:
                    save_chunk_text(ch, out_path)
                print(f"Saved chunk file: {out_path}")

        # Process chunks (copy/move) if requested
        if args.process_chunks in ("copy", "move") and not args.print_only:
            if not args.process_dest:
                raise ValueError("When using --process-chunks copy|move you must provide --process-dest")
            dest_root = Path(args.process_dest)
            dest_root.mkdir(parents=True, exist_ok=True)

            for idx, ch in enumerate(chunks, start=1):
                chunk_name = f"{args.chunk_prefix}{idx}"
                # build mapping for this chunk: ID -> Path
                chunk_mapping = {id_: mapping[id_] for id_ in ch}
                print(f"Processing chunk {idx} -> {chunk_name} ({len(chunk_mapping)} folders)")
                if args.process_chunks == "copy":
                    copy_chunk_folders(chunk_mapping, dest_root, chunk_name)
                else:
                    move_chunk_folders(chunk_mapping, dest_root, chunk_name)
                print(f"  Done processing chunk {idx} into {dest_root / chunk_name}")
    else:
        print("\nChunking not requested (chunk-size <= 0).")

    # Optionally save a full detailed map to a JSON file if user asked to save chunks but chunk_size==0 (single file)
    if args.save_chunks and not args.chunk_size and not args.print_only:
        # Save everything in one file
        dest = Path(args.chunks_dir)
        dest.mkdir(parents=True, exist_ok=True)
        out_path = dest / "all_mapping.json"
        with out_path.open("w", encoding="utf-8") as fh:
            json.dump(detailed_map, fh, indent=2, ensure_ascii=False)
        print(f"Saved full mapping to {out_path}")


if __name__ == "__main__":
    main()
