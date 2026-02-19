# Stream Consumer

Consumes events from Kafka in real time and maintains rolling aggregations in Redis. When activity on an image crosses a threshold (e.g. goes viral), it publishes a moderation request to Kafka so the risk scoring service can evaluate it.

## What It Does

1. **Consumes** view, comment, upload, and flag events from Kafka topics
2. **Aggregates** per-image counts into sliding time windows (1 min, 5 min, 1 hr) stored as Redis sorted sets
3. **Detects** threshold crossings and fires alerts:
   - Viral content: 100+ views in 5 minutes
   - Suspicious activity: 10+ comments in 1 minute
   - Popular post: 50+ views in 1 hour
4. **Persists** view milestones (10, 100, 1000 views) to PostgreSQL
5. **Publishes** a moderation request to Kafka (`gourmetgram.moderation_requests`) for inference when viral or suspicious alerts fire

## Input

Kafka topics consumed:
- `gourmetgram.views`
- `gourmetgram.comments`
- `gourmetgram.uploads`
- `gourmetgram.flags`

## Output / What Gets Stored

| Data | Storage | Key Pattern |
|------|---------|-------------|
| Rolling view counts (1min/5min/1hr) | Redis sorted set | `image:{id}:views:5min` |
| Rolling comment counts (1min/5min/1hr) | Redis sorted set | `image:{id}:comments:5min` |
| Total views / comments / flags | Redis counter | `image:{id}:total_views` |
| Image metadata (category, caption length) | Redis hash | `image:{id}:metadata` |
| View milestones (10/100/1000) | PostgreSQL → `image_milestones` table | — |
| Moderation requests (viral/suspicious) | Kafka → `gourmetgram.moderation_requests` | — |

Redis keys for sorted sets use **timestamps as scores**, so counts within any time window are computed with `ZCOUNT key (now - window) now`. Expired entries are cleaned up on each event.

## Alert Thresholds

| Alert | Condition | Triggers moderation? (inference) |
|-------|-----------|----------------------|
| Viral | 100+ views in 5 min | Yes |
| Suspicious | 10+ comments in 1 min | Yes |
| Popular | 50+ views in 1 hr | No (log only) |
| Milestone | 10 / 100 / 1000 total views | No (persisted to Postgres) |

Each alert fires **once per image** — duplicate suppression is done in memory with an LRU cache (max 1000 images tracked per alert type).

## Dependencies

```
kafka-python    # Kafka consumer and producer
redis           # sorted sets for rolling windows
psycopg2-binary # milestone persistence to PostgreSQL
```

## Configuration

| Env Var | Default | Description |
|---------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker |
| `KAFKA_TOPICS` | `gourmetgram.views,gourmetgram.comments` | Topics to consume |
| `MODERATION_TOPIC` | `gourmetgram.moderation_requests` | Topic to publish alerts to |
| `REDIS_HOST` / `REDIS_PORT` | `localhost:6379` | Redis connection |
| `DATABASE_URL` | `postgresql://...` | PostgreSQL for milestones |
| `VIRAL_THRESHOLD_VIEWS_5MIN` | `100` | Views/5min to trigger viral alert |
| `SUSPICIOUS_THRESHOLD_COMMENTS_1MIN` | `10` | Comments/1min to trigger suspicious alert |
| `POPULAR_THRESHOLD_VIEWS_1HR` | `50` | Views/1hr for popular alert |

## Files

- `main.py` — entry point, wires everything together, graceful shutdown
- `aggregators.py` — processes each event type, updates Redis windows, checks alerts
- `alert_manager.py` — threshold checks, alert deduplication, publishes to Kafka
- `kafka_consumer.py` — Kafka consumer wrapper
- `redis_client.py` — Redis client wrapper with `zadd_with_ttl`, `zcount` helpers
- `db_client.py` — PostgreSQL client for milestone persistence
- `config.py` — all configuration via env vars
