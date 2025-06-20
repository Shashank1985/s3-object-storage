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
from cachetools import LRUCache
from fastapi.responses import StreamingResponse
from shared import init_db, get_db
from routers import buckets as buckets_router # Import the bucket router
from routers import objects as objects_router

async def verify_api_key(api_key_from_header: str = Header(None, alias=config.API_KEY_HEADER_NAME)):
    """
    FastAPI Dependency to verify the API key.
    It expects the API key in the header defined by config.API_KEY_HEADER_NAME.
    """
    if api_key_from_header is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"API Key required in '{config.API_KEY_HEADER_NAME}' header"
        )
    if api_key_from_header not in config.ALLOWED_API_KEYS:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid API Key provided"
        )
    return api_key_from_header 

@asynccontextmanager
async def lifespan(app_instance: FastAPI): 
    print("Application startup...")
    os.makedirs(config.DATA_DIR_BASE, exist_ok=True) 
    init_db() 
    print("Toy S3 Service (SQLite Backend & Config File) started.")
    yield
    print("Application shutdown...")

app = FastAPI(
    title="Toy S3 Service (SQLite & Config)",
    description="A simplified S3-like object storage service using SQLite for metadata and a config file.",
    version="0.0.1", # Updated version
    lifespan=lifespan # Use the lifespan context manager
)

app.include_router(buckets_router.router,dependencies=[Depends(verify_api_key)]) # This uses the prefix from buckets.py
app.include_router(objects_router.router,dependencies=[Depends(verify_api_key)]) # This uses paths as defined in objects.py


# --- API Endpoints ---

@app.get("/", tags=["General"])
async def root_status():
    return {"message": "Toy S3 Service is running!"}




if __name__ == "__main__":
    print("Starting server with uvicorn directly from script...")
    uvicorn.run("app:app", host="0.0.0.0", port=3000, reload=True) 