# Data Generator

Simulates realistic user activity on GourmetGram by continuously sending events to the FastAPI service. This populates the platform with data so the rest of the pipeline (stream consumer, risk scoring service, ETL jobs) has something to work with.

## What It Does

Runs a **Poisson process** that fires random events at a configurable rate. Each event hits a real API endpoint, which in turn stores data in Postgres, uploads images to MinIO, and publishes messages to Kafka topics.

**Event types (equal 20% probability each):**
- `view` — a user views an image
- `comment` — a user comments on an image
- `upload` — a user uploads a new food image (from the Food-11 dataset)
- `user` — a new user is created
- `flag` — a user flags an image as inappropriate

## Input

- **Food-11 dataset** — real food images mounted at `/data/Food-11` inside the container (11 categories: Bread, Dairy, Dessert, Egg, Fried food, Meat, Noodles/Pasta, Rice, Seafood, Soup, Vegetable/Fruit)
- **FastAPI** running at `http://fastapi:8000` (configured via `GENERATOR_API_URL`)

## Output

All events are sent through the API, which handles storage:

| What | Where |
|------|-------|
| User records | PostgreSQL → `users` table |
| Image metadata | PostgreSQL → `images` table |
| Image files | MinIO → `gourmetgram-images` bucket |
| View / comment / flag events | Kafka → `gourmetgram.views`, `gourmetgram.comments`, `gourmetgram.flags` topics |
| Upload events | Kafka → `gourmetgram.uploads` topic |

## Configuration

| Env Var | Default | Description |
|---------|---------|-------------|
| `GENERATOR_API_URL` | `http://localhost:8000` | FastAPI base URL |
| `GENERATOR_DATASET_PATH` | `/data/Food-11` | Path to Food-11 images |
| `GENERATOR_ARRIVAL_RATE` | `150.0` | Total events per hour |
| `GENERATOR_INITIAL_USERS` | `0` | Users to create on startup |
| `GENERATOR_INITIAL_IMAGES` | `0` | Images to upload on startup |

## Dependencies

```
httpx      # async HTTP client for API calls
numpy      # Poisson/exponential distribution sampling
```

## Files

- `main.py` — entry point, Poisson event loop, config
- `event_generators.py` — one async function per event type (`generate_view`, `generate_upload`, etc.)
- `Dockerfile` — builds the container image
- `requirements.txt` — Python dependencies
