-- D1 schema mirroring your Tortoise models
CREATE TABLE IF NOT EXISTS message_records (
id INTEGER PRIMARY KEY AUTOINCREMENT,
message_id TEXT UNIQUE,
request_text TEXT NOT NULL,
agent_used TEXT NOT NULL,
status TEXT NOT NULL DEFAULT 'pending', -- queued, processing, success, failed, reject
output_data TEXT DEFAULT '{}', -- JSON string
created_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
updated_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
source TEXT
);


CREATE TRIGGER IF NOT EXISTS trg_message_records_updated
AFTER UPDATE ON message_records
BEGIN
UPDATE message_records
SET updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
WHERE id = NEW.id;
END;


CREATE TABLE IF NOT EXISTS whatsapp_media (
id INTEGER PRIMARY KEY AUTOINCREMENT,
file_name TEXT NOT NULL,
url TEXT,
uploaded_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);