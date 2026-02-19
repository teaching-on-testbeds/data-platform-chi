# FastAPI Service

The central REST API for GourmetGram. Every other service in the platform interacts through this API — the data generator sends events here, and all user/image/comment/flag data flows through it into Postgres, MinIO, and Kafka.

## What It Does

Handles all write operations for the platform:
- Creates users and serves as the auth/identity layer
- Accepts image uploads, stores files in MinIO, and saves metadata in Postgres
- Records views, comments, and flags
- Publishes every event to the appropriate Kafka topic so downstream consumers (stream consumer, risk scoring service) can react in real time

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Health check — returns status and Kafka stats |
| `POST` | `/users/` | Create a user (returns existing if username taken) |
| `POST` | `/upload/` | Upload a food image with caption and category |
| `POST` | `/images/{image_id}/view` | Record a view on an image |
| `POST` | `/comments/` | Add a comment to an image |
| `POST` | `/flags/` | Flag an image or comment for moderation |

## Input

- HTTP requests from the data generator, test scripts, or any client
- Image files (multipart form upload)
- JSON payloads for users, comments, flags

## Output / What Gets Stored

| Data | Storage |
|------|---------|
| User records | PostgreSQL → `users` table |
| Image metadata (caption, category, view count) | PostgreSQL → `images` table |
| Image files | MinIO → `gourmetgram-images` bucket (UUID-based filenames) |
| Comments | PostgreSQL → `comments` table |
| Flags | PostgreSQL → `flags` table |
| All events (uploads, views, comments, flags) | Kafka topics (see below) |

**Kafka topics published to:**
- `gourmetgram.uploads` — new image uploads
- `gourmetgram.views` — image view events
- `gourmetgram.comments` — new comments
- `gourmetgram.flags` — flagged content

## Dependencies

```
fastapi          # web framework
sqlalchemy       # ORM for PostgreSQL
aiokafka         # async Kafka producer
boto3            # S3/MinIO file uploads
pydantic         # request/response validation
uvicorn          # ASGI server
```

## Configuration

| Env Var | Description |
|---------|-------------|
| `DATABASE_URL` | PostgreSQL connection string |
| `S3_ENDPOINT_URL` | MinIO endpoint (e.g. `http://minio:9000`) |
| `AWS_ACCESS_KEY_ID` | MinIO access key |
| `AWS_SECRET_ACCESS_KEY` | MinIO secret key |
| `S3_BUCKET_NAME` | Bucket for image uploads |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker address |
| `KAFKA_TOPIC_UPLOADS/VIEWS/COMMENTS/FLAGS` | Topic names (have defaults) |

## Files

```
fastapi_service/
  Dockerfile
  requirements.txt
  app/
    main.py           # all API endpoints and startup/shutdown hooks
    models.py         # SQLAlchemy ORM table definitions
    schemas.py        # Pydantic request/response schemas
    database.py       # DB session and connection setup
    kafka_producer.py # async Kafka producer (fire-and-forget per event)
```
