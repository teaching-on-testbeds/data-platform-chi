import uuid
from sqlalchemy import Column, String, Integer, Text, DateTime, ForeignKey, Boolean
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship 
from .database import Base

class User(Base):
    __tablename__ = "users"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    username = Column(String, unique=True, index=True, nullable=False)
    tos_version_accepted = Column(Integer, nullable=False, default=1)
    tos_accepted_at = Column(DateTime(timezone=True), nullable=True)
    country_of_residence = Column(String(2), nullable=True)
    year_of_birth = Column(Integer, nullable=True)
    is_test_account = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    images = relationship("Image", back_populates="user")
    comments = relationship("Comment", back_populates="user")
    flags = relationship("Flag", back_populates="user")

class Image(Base):
    __tablename__ = "images"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    s3_url = Column(String, nullable=False)
    source = Column(String, nullable=False, default="user_upload")
    caption = Column(Text, nullable=True)
    category = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    deleted_at = Column(DateTime(timezone=True), nullable=True)
    views = Column(Integer, default=0)

    user = relationship("User", back_populates="images")
    comments = relationship("Comment", back_populates="image", cascade="all, delete-orphan")
    flags = relationship("Flag", back_populates="image")

class Comment(Base):
    __tablename__ = "comments"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    image_id = Column(UUID(as_uuid=True), ForeignKey("images.id"), nullable=False)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    content = Column(Text, nullable=False)
    moderator_hidden = Column(Boolean, nullable=False, default=False)
    moderator_action_id = Column(UUID(as_uuid=True), nullable=True)
    deleted_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    image = relationship("Image", back_populates="comments")
    user = relationship("User", back_populates="comments")
    flags = relationship("Flag", back_populates="comment")

class Flag(Base):
    __tablename__ = "flags"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    image_id = Column(UUID(as_uuid=True), ForeignKey("images.id"), nullable=True)
    comment_id = Column(UUID(as_uuid=True), ForeignKey("comments.id"), nullable=True)
    reason = Column(Text, nullable=False)
    moderation_action_id = Column(UUID(as_uuid=True), nullable=True)
    review_status = Column(String, nullable=False, default="pending")
    resolved_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    user = relationship("User", back_populates="flags")
    image = relationship("Image", back_populates="flags")
    comment = relationship("Comment", back_populates="flags")
