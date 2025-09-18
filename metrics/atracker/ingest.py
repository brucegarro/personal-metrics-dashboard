import os
import json
from typing import Iterator
from datetime import datetime
from typing import Optional

import dropbox

from .dropbox import get_dropbox_client


def parse_atracker_datafile(filepath: str) -> Iterator[dict]:
    """Stream parse an Atracker JSON datafile and yield TaskEntry changes one-by-one.

    Yields TaskEntry dicts in the same structure as stored under
    changesByEntity.TaskEntry[]. Falls back to json.load if ijson is unavailable.
    """
    try:
        import ijson  # type: ignore
        with open(filepath, "rb") as f:
            for item in ijson.items(f, "changesByEntity.TaskEntry.item"):
                yield item
    except Exception:
        # Fallback: load whole file (less memory friendly)
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
            for item in data.get("changesByEntity", {}).get("TaskEntry", []) or []:
                yield item


# Defaults can be overridden at call-time
DEFAULT_DROPBOX_FOLDER = "/apps/atracker/mainstore.v2/baselines"
DEFAULT_LOCAL_FOLDER = "data/atracker"


def _find_existing_version(local_dir: str, filename: str) -> bool:
    """Return True if any date-prefixed version of filename exists in local_dir."""
    base_name = os.path.basename(filename)
    try:
        for f in os.listdir(local_dir):
            if f.endswith(base_name):
                return True
    except FileNotFoundError:
        return False
    return False


def _download_file(
    dbx: dropbox.Dropbox,
    dbx_path: str,
    local_path: str,
    dated: bool = True,
) -> str:
    """Download a file from Dropbox and return the local path used."""
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    _md, res = dbx.files_download(dbx_path)

    if dated:
        today_str = datetime.now().strftime("%m-%d-%Y")
        dirname, fname = os.path.split(local_path)
        local_path = os.path.join(dirname, f"{today_str}_{fname}")

    with open(local_path, "wb") as f:
        f.write(res.content)
    print(f"Downloaded {dbx_path} → {local_path}")
    return local_path


async def sync_folder(
    dropbox_path: str = DEFAULT_DROPBOX_FOLDER,
    local_folder: str = DEFAULT_LOCAL_FOLDER,
    dbx: Optional[dropbox.Dropbox] = None,
    user_id: Optional[str] = None,
) -> list[str]:
    """Sync a Dropbox folder to local storage.

    Returns a list of local file paths that were downloaded in this run.
    """
    import logging
    logger = logging.getLogger("atracker_etl")
    if dbx is None:
        dbx = await get_dropbox_client(user_id=user_id)

    downloaded_files: list[str] = []
    logger.info(f"Listing Dropbox folder: {dropbox_path}")
    result = dbx.files_list_folder(dropbox_path, recursive=True)

    def handle_entries(entries):
        nonlocal downloaded_files
        for entry in entries:
            if isinstance(entry, dropbox.files.FileMetadata):
                relative_path = entry.path_display.replace(dropbox_path, "").lstrip("/")
                local_path = os.path.join(local_folder, relative_path)

                # Ensure local folder exists
                os.makedirs(os.path.dirname(local_path), exist_ok=True)

                # Skip if already have any date-prefixed version locally
                if _find_existing_version(os.path.dirname(local_path), os.path.basename(local_path)):
                    logger.info(f"Skipping (already versioned locally): {local_path}")
                    continue

                logger.info(f"Downloading file {entry.path_display} to {local_path}")
                saved_path = _download_file(dbx, entry.path_display, local_path, dated=True)
                downloaded_files.append(saved_path)

    # First page
    handle_entries(result.entries)

    # Paginate if more results
    while result.has_more:
        result = dbx.files_list_folder_continue(result.cursor)
        handle_entries(result.entries)

    logger.info(f"Downloaded {len(downloaded_files)} files from Dropbox to {local_folder}")
    return downloaded_files
