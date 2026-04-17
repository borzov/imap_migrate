"""IMAP folder name decoding and destination resolution."""

from imap_migrate.constants import BUILTIN_FOLDER_MAP


def decode_folder_name(name: str) -> str:
    """Decode IMAP modified UTF-7 folder name."""
    if "&" in name:
        try:
            return name.encode("ascii").decode("imap4-utf-7")
        except (UnicodeDecodeError, LookupError):
            pass
    return name


def resolve_destination_folder(
    src_folder: str,
    folder_map: dict[str, str],
    use_builtin_map: bool,
) -> str:
    """Map source folder name to destination folder name (same rules as IMAPMigrator)."""
    if src_folder in folder_map:
        return folder_map[src_folder]
    decoded = decode_folder_name(src_folder)
    if use_builtin_map:
        if decoded in BUILTIN_FOLDER_MAP:
            return BUILTIN_FOLDER_MAP[decoded]
        if src_folder in BUILTIN_FOLDER_MAP:
            return BUILTIN_FOLDER_MAP[src_folder]
    return decoded
