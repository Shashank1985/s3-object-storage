import os
import uuid
import sqlite3
import shutil
from fastapi import FastAPI, HTTPException, status, Depends, Response,UploadFile, File,Header, APIRouter
from contextlib import asynccontextmanager 
import uvicorn
import hashlib 
import mimetypes
import config 
from fastapi.responses import StreamingResponse
from db import get_db

router = APIRouter(
    prefix="/objects",  # Optional: prefix for all routes in this router
    tags=["Buckets"]    # Tag for OpenAPI docs
)
def construct_storage_path(bucket_name: str, object_key: str) -> str:
    return os.path.join(config.OBJECT_STORAGE_DIR, bucket_name, object_key)

@router.put("/{bucket_name}/{object_key:path}", status_code=status.HTTP_201_CREATED, tags=["Objects"])
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

@router.get("/{bucket_name}/{object_key:path}", tags=["Objects"])
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

    return StreamingResponse(
        file_iterator(storage_path),
        media_type=content_type, 
        headers=response_headers
    )