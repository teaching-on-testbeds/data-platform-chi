from pydantic import BaseModel
from typing import Optional
from uuid import UUID
from datetime import datetime

# --- Users ---
class UserCreate(BaseModel):
    username: str
    tos_version_accepted: int = 1
    tos_accepted_at: Optional[datetime] = None
    country_of_residence: Optional[str] = None
    year_of_birth: Optional[int] = None
    is_test_account: bool = False

class UserResponse(BaseModel):
    id: UUID
    username: str
    tos_version_accepted: int
    tos_accepted_at: Optional[datetime] = None
    country_of_residence: Optional[str] = None
    year_of_birth: Optional[int] = None
    is_test_account: bool
    created_at: datetime
    class Config:
        from_attributes = True

class ImageResponse(BaseModel):
    id: UUID
    user_id: UUID
    s3_url: str
    source: str
    caption: Optional[str] = None
    category: Optional[str] = None
    views: int
    created_at: datetime
    deleted_at: Optional[datetime] = None
    class Config:
        from_attributes = True

# --- Comments ---
class CommentCreate(BaseModel):
    image_id: UUID
    user_id: UUID
    content: str

class CommentResponse(BaseModel):
    id: UUID
    image_id: UUID
    user_id: UUID
    content: str
    moderator_hidden: bool
    moderator_action_id: Optional[UUID] = None
    deleted_at: Optional[datetime] = None
    created_at: datetime
    class Config:
        from_attributes = True

# --- Flags ---
class FlagCreate(BaseModel):
    user_id: UUID
    reason: str
    image_id: Optional[UUID] = None
    comment_id: Optional[UUID] = None

class FlagResponse(BaseModel):
    id: UUID
    user_id: UUID
    image_id: Optional[UUID] = None
    comment_id: Optional[UUID] = None
    reason: str
    moderation_action_id: Optional[UUID] = None
    review_status: str
    resolved_at: Optional[datetime] = None
    created_at: datetime
    class Config:
        from_attributes = True
