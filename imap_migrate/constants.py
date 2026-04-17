"""Built-in folder name mapping (Yandex, Mail.ru, Gmail, etc.) -> standard IMAP names."""

BUILTIN_FOLDER_MAP: dict[str, str] = {
    "Входящие": "INBOX",
    "Отправленные": "Sent",
    "Черновики": "Drafts",
    "Спам": "Junk",
    "Удалённые": "Trash",
    "Удаленные": "Trash",
    "Sent": "Sent",
    "Drafts": "Drafts",
    "Spam": "Junk",
    "Trash": "Trash",
    "Outbox": "Outbox",
    "INBOX": "INBOX",
    "[Gmail]/Sent Mail": "Sent",
    "[Gmail]/Drafts": "Drafts",
    "[Gmail]/Trash": "Trash",
    "[Gmail]/Spam": "Junk",
    "Корзина": "Trash",
}
