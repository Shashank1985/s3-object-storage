import os
import config
import sqlite3
from cachetools import LRUCache

#DEFINE CACHE FOR OBJECT METADATA
METADATA_CACHE = LRUCache(maxsize=config.OBJECT_METADATA_CACHE_SIZE)


def init_db():
    """Initializes the database and creates tables if they don't exist."""
    os.makedirs(config.METADATA_DIR, exist_ok=True) 
    os.makedirs(config.OBJECT_STORAGE_DIR, exist_ok=True) 
    conn = sqlite3.connect(config.DATABASE_URL)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS buckets (
            name TEXT PRIMARY KEY,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS objects (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bucket_name TEXT NOT NULL,
            object_key TEXT NOT NULL,
            internal_storage_id TEXT NOT NULL UNIQUE, -- Could be UUID or path fragment
            size_bytes INTEGER,
            etag TEXT,
            content_type TEXT,
            storage_path TEXT NOT NULL,
            last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (bucket_name) REFERENCES buckets(name) ON DELETE CASCADE,
            UNIQUE (bucket_name, object_key) -- Ensures object keys are unique within a bucket
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_objects_bucket_key ON objects (bucket_name, object_key)")

    conn.commit()
    conn.close()
    print(f"Database initialized/checked at {config.DATABASE_URL}")

def get_db():
    """FastAPI dependency to get a database connection."""
    db = sqlite3.connect(config.DATABASE_URL,check_same_thread=False)
    db.row_factory = sqlite3.Row 
    try:
        yield db
    finally:
        db.close()
