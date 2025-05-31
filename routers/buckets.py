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
    prefix="/buckets",  # Optional: prefix for all routes in this router
    tags=["Buckets"]    # Tag for OpenAPI docs
)

@router.put("/{bucket_name}", status_code=status.HTTP_201_CREATED, tags=["Buckets"])
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

@router.get("/buckets", tags=["Buckets"])
async def list_buckets_endpoint(db: sqlite3.Connection = Depends(get_db)):
    cursor = db.cursor()
    try:
        cursor.execute("SELECT name, created_at FROM buckets ORDER BY created_at DESC")
        buckets_data = cursor.fetchall() # Returns a list of Row objects
        buckets_list = [{"name": row["name"], "created_at": row["created_at"]} for row in buckets_data]
        return {"buckets": buckets_list}
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Database error: {e}")


@router.head("/{bucket_name}", status_code=status.HTTP_200_OK, tags=["Buckets"])
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


@router.delete("/{bucket_name}", status_code=status.HTTP_204_NO_CONTENT, tags=["Buckets"])
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
