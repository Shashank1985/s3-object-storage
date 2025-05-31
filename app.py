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
from db import init_db, get_db
from routers import buckets as buckets_router # Import the bucket router
from routers import objects as objects_router

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

app.include_router(buckets_router.router) # This uses the prefix from buckets.py
app.include_router(objects_router.router) # This uses paths as defined in objects.py


# --- API Endpoints ---

@app.get("/", tags=["General"])
async def root_status():
    return {"message": "Toy S3 Service is running!"}




if __name__ == "__main__":
    print("Starting server with uvicorn directly from script...")
    uvicorn.run("app:app", host="0.0.0.0", port=3000, reload=True) 