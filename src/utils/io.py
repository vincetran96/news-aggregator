import json
import os


def make_filepath(path: str, extension: str = "") -> str:
    """
    Make a file path with the given extension.

    If `path` does not already end with `extension`, the extension is appended.
    Parent directories are created automatically if they do not exist.
    """
    if not path.endswith(extension):
        path = path + extension
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    return path


def write_plaintext(content: str, path: str, extension: str = ".txt") -> str:
    """
    Writes a string to a plaintext file.    

    Returns the resolved file path that was written.
    """
    path = make_filepath(path, extension)

    with open(path, "w", encoding="utf-8") as f:
        f.write(content)

    return path


def write_jsonl(records: list[dict], path: str) -> str:
    """
    Writes a list of dicts to a JSONL file (one JSON object per line).

    Returns the resolved file path that was written.
    """
    path = make_filepath(path, extension=".jsonl")

    with open(path, "w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    return path
