import os
import uuid
import sqlite3
import shutil
from fastapi import FastAPI, HTTPException, status, Depends, Response,UploadFile, File,Header
from contextlib import asynccontextmanager 
import uvicorn
import hashlib 
import mimetypes
import config 
from fastapi.responses import StreamingResponse

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

@asynccontextmanager
async def lifespan(app_instance: FastAPI): 
    print("Application startup...")
    os.makedirs(config.DATA_DIR_BASE, exist_ok=True) 
    init_db() 
    print("Toy S3 Service (SQLite Backend & Config File) started.")
    print(f"Data will be stored in: {os.path.abspath(config.DATA_DIR_BASE)}")
    print(f"Object data in: {os.path.abspath(config.OBJECT_STORAGE_DIR)}")
    print(f"Metadata database: {os.path.abspath(config.DATABASE_URL)}")
    yield
    print("Application shutdown...")

app = FastAPI(
    title="Toy S3 Service (SQLite & Config)",
    description="A simplified S3-like object storage service using SQLite for metadata and a config file.",
    version="0.0.1", # Updated version
    lifespan=lifespan # Use the lifespan context manager
)


# --- API Endpoints ---

@app.get("/", tags=["General"])
async def root_status():
    return {"message": "Toy S3 Service is running!"}


@app.put("/{bucket_name}", status_code=status.HTTP_201_CREATED, tags=["Buckets"])
async def create_bucket(bucket_name: str, db: sqlite3.Connection = Depends(get_db)):
    if not bucket_name or not bucket_name.strip(): # Basic validation
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Bucket name cannot be empty or just whitespace")

    cursor = db.cursor()
    try:
        cursor.execute("INSERT INTO buckets (name) VALUES (?)", (bucket_name,))
        db.commit()
    except sqlite3.IntegrityError: 
        db.rollback()
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Bucket '{bucket_name}' already exists")
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Database error: {e}")

    bucket_data_path = os.path.join(config.OBJECT_STORAGE_DIR, bucket_name)
    try:
        os.makedirs(bucket_data_path, exist_ok=True)
        print(f"Created data directory for bucket: {bucket_data_path}")
    except OSError as e:
        print(f"Error creating bucket data directory {bucket_data_path}: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Could not create bucket data directory: {e}")

    return {"message": f"Bucket '{bucket_name}' created successfully"}

@app.get("/buckets", tags=["Buckets"])
async def list_buckets_endpoint(db: sqlite3.Connection = Depends(get_db)):
    cursor = db.cursor()
    try:
        cursor.execute("SELECT name, created_at FROM buckets ORDER BY created_at DESC")
        buckets_data = cursor.fetchall() # Returns a list of Row objects
        buckets_list = [{"name": row["name"], "created_at": row["created_at"]} for row in buckets_data]
        return {"buckets": buckets_list}
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Database error: {e}")


@app.head("/{bucket_name}", status_code=status.HTTP_200_OK, tags=["Buckets"])
async def head_bucket(bucket_name: str, db: sqlite3.Connection = Depends(get_db)):
    cursor = db.cursor()
    try:
        cursor.execute("SELECT name FROM buckets WHERE name = ?", (bucket_name,))
        bucket = cursor.fetchone()
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Database error: {e}")

    if not bucket:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Bucket '{bucket_name}' not found")
    return Response(status_code=status.HTTP_200_OK) # HEAD expects no body


@app.delete("/{bucket_name}", status_code=status.HTTP_204_NO_CONTENT, tags=["Buckets"])
async def delete_bucket_endpoint(bucket_name: str, db: sqlite3.Connection = Depends(get_db)):
    cursor = db.cursor()
    try:
        # Check if bucket exists
        cursor.execute("SELECT name FROM buckets WHERE name = ?", (bucket_name,))
        bucket_exists = cursor.fetchone()
        if not bucket_exists:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Bucket '{bucket_name}' not found")

        bucket_data_path = os.path.join(config.OBJECT_STORAGE_DIR, bucket_name)
        if os.path.exists(bucket_data_path) and os.path.isdir(bucket_data_path):
            try:
                shutil.rmtree(bucket_data_path)
                print(f"Deleted data directory for bucket: {bucket_data_path}")
            except OSError as e:
                print(f"Error deleting bucket data directory {bucket_data_path}: {e}")
                raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                                    detail=f"Could not delete bucket data directory: {e}. Bucket metadata not deleted.")

        cursor.execute("DELETE FROM buckets WHERE name = ?", (bucket_name,))
        db.commit()

    except HTTPException: 
        db.rollback() # Rollback any partial DB changes if an HTTPException occurred earlier
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Database error or unexpected issue: {e}")

    return Response(status_code=status.HTTP_204_NO_CONTENT) 

def construct_storage_path(bucket_name: str, object_key: str) -> str:
    return os.path.join(config.OBJECT_STORAGE_DIR, bucket_name, object_key)

@app.put("/{bucket_name}/{object_key:path}", status_code=status.HTTP_201_CREATED, tags=["Objects"])
async def put_object(
    bucket_name: str,    
    object_key: str,     
    file: UploadFile = File(...),
    client_content_type: str = Header(None, alias="Content-Type"),
    db: sqlite3.Connection = Depends(get_db)
):
    cursor = db.cursor()

    #Validate Bucket Existence 
    cursor.execute("SELECT name FROM buckets WHERE name = ?", (bucket_name,))
    bucket = cursor.fetchone()
    if not bucket:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Bucket '{bucket_name}' not found")
    
    if not object_key: 
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Object key cannot be empty")

    generated_internal_storage_id = str(uuid.uuid4().hex) # NOT NULL
    derived_storage_path = construct_storage_path(bucket_name, object_key) # NOT NULL

    object_target_dir = os.path.dirname(derived_storage_path)
    try:
        os.makedirs(object_target_dir, exist_ok=True)
    except OSError as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Could not create object storage directory: {e}")

    md5_hash = hashlib.md5()
    calculated_size_bytes = 0
    try:
        with open(derived_storage_path, "wb") as f_disk: 
            while chunk := await file.read(8192):
                f_disk.write(chunk)
                md5_hash.update(chunk)
                calculated_size_bytes += len(chunk)
    except Exception as e:
        if os.path.exists(derived_storage_path): #to handle atomicity, if something happens during write and partial file is created, we will remove everything that was written
            os.remove(derived_storage_path)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Failed to store object: {e}")
    finally:
        await file.close()

    calculated_etag = md5_hash.hexdigest()

    final_content_type = client_content_type
    if not final_content_type:
        if file.content_type and file.content_type != "application/octet-stream":
            final_content_type = file.content_type
        else:
            guessed_type, _ = mimetypes.guess_type(object_key)
            if guessed_type:
                final_content_type = guessed_type
            else:
                final_content_type = "application/octet-stream"

    try:
        cursor.execute("""
            INSERT INTO objects (
                bucket_name, object_key, internal_storage_id, storage_path,
                size_bytes, etag, content_type, last_modified
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(bucket_name, object_key) DO UPDATE SET
                internal_storage_id = excluded.internal_storage_id,
                storage_path = excluded.storage_path, -- Ensure this is updated too
                size_bytes = excluded.size_bytes,
                etag = excluded.etag,
                content_type = excluded.content_type,
                last_modified = CURRENT_TIMESTAMP
        """, (
            bucket_name,                      # NOT NULL
            object_key,                       # NOT NULL
            generated_internal_storage_id,    # NOT NULL
            derived_storage_path,             # NOT NULL
            calculated_size_bytes,
            calculated_etag,
            final_content_type
        ))
        db.commit()
    except Exception as e:
        db.rollback()
        if os.path.exists(derived_storage_path):
            os.remove(derived_storage_path)
        print(f"DB Error during object metadata storage: {type(e).__name__} - {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Database error storing object metadata: {type(e).__name__} - {str(e)}")

    response_headers = {"ETag": f'"{calculated_etag}"'}
    return Response(status_code=status.HTTP_201_CREATED, headers=response_headers,
                    content=f"Object '{object_key}' uploaded successfully to bucket '{bucket_name}'.")

@app.get("/{bucket_name}/{object_key:path}", tags=["Objects"])
async def get_object(
    bucket_name: str,
    object_key: str,
    db: sqlite3.Connection = Depends(get_db)
):
    cursor = db.cursor()
    try:
        cursor.execute("""
            SELECT storage_path, content_type, etag, size_bytes
            FROM objects
            WHERE bucket_name = ? AND object_key = ?
        """, (bucket_name, object_key))
        object_meta = cursor.fetchone()
    except Exception as e:
        print(f"DB Error during get_object metadata lookup: {type(e).__name__} - {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Database error retrieving object metadata: {str(e)}")
    if not object_meta:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Object '{object_key}' not found in bucket '{bucket_name}'")

    storage_path = object_meta["storage_path"]
    content_type = object_meta["content_type"]
    etag = object_meta["etag"]
    size_bytes = object_meta["size_bytes"] # We'll use this for Content-Length

    if not os.path.exists(storage_path):
        
        print(f"CRITICAL INCONSISTENCY: Object metadata found for '{bucket_name}/{object_key}' but file missing at '{storage_path}'")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail="Object data not found on server (inconsistency)")

    async def file_iterator(file_path: str, chunk_size: int = 8192):
        """Asynchronous generator to stream the file in chunks."""
        try:
            with open(file_path, "rb") as f:
                while chunk := f.read(chunk_size):
                    yield chunk
        except Exception as e:
            print(f"Error reading file chunk for {file_path}: {e}")
            
    response_headers = {
        "Content-Type": content_type,
        "ETag": f'"{etag}"', # ETags are typically quoted
        "Content-Length": str(size_bytes) # Must be a string
    }
    # S3 also often includes "Last-Modified", "x-amz-version-id" (if versioning) etc.

    return StreamingResponse(
        file_iterator(storage_path),
        media_type=content_type, # Redundant if Content-Type is in headers, but good practice
        headers=response_headers
    )
if __name__ == "__main__":
    print("Starting server with uvicorn directly from script...")
    uvicorn.run("app:app", host="0.0.0.0", port=3000, reload=True) 