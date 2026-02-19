import os
import uuid
import boto3
from datetime import datetime
from fastapi import FastAPI, Depends, UploadFile, File, Form, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import update
from typing import Optional
from . import models, database, schemas
from .kafka_producer import kafka_producer

app = FastAPI(title="GourmetGram API")

# S3 / MinIO Client
s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("S3_ENDPOINT_URL"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
)
BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

@app.on_event("startup")
async def startup_event():
    # Ensure S3 bucket exists
    try:
        s3.create_bucket(Bucket=BUCKET_NAME)
    except Exception:
        pass # Bucket likely exists

    # Start Kafka producer
    await kafka_producer.start()

@app.on_event("shutdown")
async def shutdown_event():
    # Stop Kafka producer gracefully
    await kafka_producer.stop()

# --- Health Check ---
@app.get("/health")
async def health_check():
    """Health check endpoint for container orchestration"""
    return {
        "status": "healthy",
        "kafka_enabled": kafka_producer.enabled,
        "kafka_stats": kafka_producer.get_stats()
    }

# --- Users ---
@app.post("/users/", response_model=schemas.UserResponse)
def create_user(user: schemas.UserCreate, db: Session = Depends(database.get_db)):
    # Check if username exists
    existing = db.query(models.User).filter(models.User.username == user.username).first()
    if existing:
        return existing
        
    db_user = models.User(
        username=user.username,
        tos_version_accepted=user.tos_version_accepted,
        tos_accepted_at=user.tos_accepted_at,
        country_of_residence=user.country_of_residence,
        year_of_birth=user.year_of_birth,
        is_test_account=user.is_test_account,
    )
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    return db_user

# --- Images ---
@app.post("/upload/", response_model=schemas.ImageResponse)
async def upload_image(
    user_id: uuid.UUID = Form(...),
    caption: str = Form(None),
    category: str = Form(None),
    file: UploadFile = File(...),
    db: Session = Depends(database.get_db)
):
    # 1. Upload file to MinIO/S3
    file_ext = file.filename.split('.')[-1]
    file_key = f"{uuid.uuid4()}.{file_ext}"

    try:
        s3.upload_fileobj(file.file, BUCKET_NAME, file_key)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"S3 Upload failed: {str(e)}")

    # 2. Construct S3 URL (Mock URL structure for local dev)
    s3_url = f"{os.getenv('S3_ENDPOINT_URL')}/{BUCKET_NAME}/{file_key}"

    # 3. Save to DB
    db_image = models.Image(
        user_id=user_id,
        s3_url=s3_url,
        caption=caption,
        category=category
    )
    db.add(db_image)
    db.commit()
    db.refresh(db_image)

    # 4. Publish to Kafka
    upload_event = {
        "event_type": "upload",
        "image_id": str(db_image.id),
        "user_id": str(db_image.user_id),
        "category": db_image.category,
        "caption": db_image.caption,
        "created_at": db_image.created_at.isoformat(),
        "s3_url": db_image.s3_url
    }
    await kafka_producer.publish_event(
        topic=os.getenv("KAFKA_TOPIC_UPLOADS", "gourmetgram.uploads"),
        event_data=upload_event
    )

    return db_image

# --- Comments ---
@app.post("/comments/", response_model=schemas.CommentResponse)
async def add_comment(comment: schemas.CommentCreate, db: Session = Depends(database.get_db)):
    # 1. Save to DB
    db_comment = models.Comment(
        image_id=comment.image_id,
        user_id=comment.user_id,
        content=comment.content
    )
    db.add(db_comment)
    db.commit()
    db.refresh(db_comment)

    # 2. Publish to Kafka
    comment_event = {
        "event_type": "comment",
        "comment_id": str(db_comment.id),
        "image_id": str(db_comment.image_id),
        "user_id": str(db_comment.user_id),
        "content": db_comment.content,
        "created_at": db_comment.created_at.isoformat()
    }
    await kafka_producer.publish_event(
        topic=os.getenv("KAFKA_TOPIC_COMMENTS", "gourmetgram.comments"),
        event_data=comment_event
    )

    return db_comment

# --- Flags ---
@app.post("/flags/", response_model=schemas.FlagResponse)
async def create_flag(flag: schemas.FlagCreate, db: Session = Depends(database.get_db)):
    if not flag.image_id and not flag.comment_id:
        raise HTTPException(status_code=400, detail="Must flag either an image or a comment")

    # 1. Save to DB
    db_flag = models.Flag(
        user_id=flag.user_id,
        image_id=flag.image_id,
        comment_id=flag.comment_id,
        reason=flag.reason
    )
    db.add(db_flag)
    db.commit()
    db.refresh(db_flag)

    # 2. Publish to Kafka
    flag_event = {
        "event_type": "flag",
        "flag_id": str(db_flag.id),
        "user_id": str(db_flag.user_id),
        "image_id": str(db_flag.image_id) if db_flag.image_id else None,
        "comment_id": str(db_flag.comment_id) if db_flag.comment_id else None,
        "reason": db_flag.reason,
        "created_at": db_flag.created_at.isoformat()
    }
    await kafka_producer.publish_event(
        topic=os.getenv("KAFKA_TOPIC_FLAGS", "gourmetgram.flags"),
        event_data=flag_event
    )

    return db_flag

# --- Views (Helper for Generator) ---
@app.post("/images/{image_id}/view")
async def record_view(image_id: uuid.UUID, db: Session = Depends(database.get_db)):
    stmt = (
        update(models.Image)
        .where(models.Image.id == image_id)
        .values(views=models.Image.views + 1)
        .returning(models.Image.views)
    )
    new_views = db.execute(stmt).scalar_one_or_none()
    if new_views is None:
        raise HTTPException(status_code=404, detail="Image not found")

    db.commit()

    # 3. Publish to Kafka
    view_event = {
        "event_type": "view",
        "image_id": str(image_id),
        "viewed_at": datetime.utcnow().isoformat()
    }
    await kafka_producer.publish_event(
        topic=os.getenv("KAFKA_TOPIC_VIEWS", "gourmetgram.views"),
        event_data=view_event
    )

    return {"status": "ok", "views": new_views}
