import os
import json
from datetime import datetime, timezone
import dropbox

def parse_atracker_datafile(filepath: str) -> dict:
    """
    Example TaskEntry
        {'properties': [{'propertyName': 'createTimeStamp',
        'value': ['date', 1755730914057.948],
        'type': 0},
        {'propertyName': 'deletedNew', 'value': 0, 'type': 0},
        {'propertyName': 'endTime', 'value': ['date', 1755731386546.753], 'type': 0},
        {'propertyName': 'finished', 'value': 1, 'type': 0},
        {'propertyName': 'lastUpdateTimeStamp',
        'value': ['date', 1755731386546.76],
        'type': 0},
        {'propertyName': 'notes', 'value': '', 'type': 0},
        {'propertyName': 'startTime',
        'value': ['date', 1755730914052.386],
        'type': 0},
        {'propertyName': 'taskEntryID',
        'value': 'Coding€€icon/Programming_py.png€0€1€2025-08-20 23:01:51 +00002025-08-20-19:01:54:052',
        'type': 0},
        {'propertyName': 'taskID',
        'value': 'Coding€€icon/Programming_py.png€0€1€2025-08-20 23:01:51 +0000',
        'type': 0},
        {'propertyName': 'task',
        'relatedIdentifier': 'Coding€€icon/Programming_py.png€0€1€2025-08-20 23:01:51 +0000',
        'type': 1}],
        'type': 100,
        'globalIdentifier': 'Coding€€icon/Programming_py.png€0€1€2025-08-20 23:01:51 +00002025-08-20-19:01:54:052'}
    """
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)

    tasks_data = data['changesByEntity']['TaskEntry']
    return tasks_data

DROPBOX_ACCESS_TOKEN = os.environ.get("DROPBOX_ACCESS_TOKEN")
DROPBOX_FOLDER = "/apps/atracker/mainstore.v2/baselines"

# Local destination folder
LOCAL_FOLDER = "data/atracker"

dbx = dropbox.Dropbox(DROPBOX_ACCESS_TOKEN)

def to_utc(dt: datetime) -> datetime:
    """Return a UTC-aware datetime for comparison."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def is_up_to_date(local_path, entry: dropbox.files.FileMetadata) -> bool:
    if not os.path.exists(local_path):
        return False
    local_size = os.path.getsize(local_path)
    return local_size == entry.size

def find_existing_version(local_dir, filename):
    """Check if any version of this file (ignoring date prefix) already exists."""
    base_name = os.path.basename(filename)
    for f in os.listdir(local_dir):
        if f.endswith(base_name):  # e.g. "MM-DD-YYYY_filename.cdeevent"
            return True
    return False

def download_file(dbx_path, local_path, dated=True):
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    md, res = dbx.files_download(dbx_path)

    # Add date prefix if requested
    if dated:
        today_str = datetime.now().strftime("%m-%d-%Y")
        dirname, fname = os.path.split(local_path)
        local_path = os.path.join(dirname, f"{today_str}_{fname}")

    with open(local_path, "wb") as f:
        f.write(res.content)
    print(f"Downloaded {dbx_path} → {local_path}")


def sync_folder(dropbox_path, local_folder):
    result = dbx.files_list_folder(dropbox_path, recursive=True)

    def handle_entries(entries):
        for entry in entries:
            if isinstance(entry, dropbox.files.FileMetadata):
                relative_path = entry.path_display.replace(dropbox_path, "").lstrip("/")
                local_path = os.path.join(local_folder, relative_path)

                # Ensure local folder exists
                os.makedirs(os.path.dirname(local_path), exist_ok=True)

                # Check if we already have *any* version of this file
                if find_existing_version(os.path.dirname(local_path), os.path.basename(local_path)):
                    print(f"Skipping (already versioned locally): {local_path}")
                    continue

                download_file(entry.path_display, local_path, dated=True)

    handle_entries(result.entries)
    while result.has_more:
        result = dbx.files_list_folder_continue(result.cursor)
        handle_entries(result.entries)

# Run
sync_folder(DROPBOX_FOLDER, LOCAL_FOLDER)