import hmac, hashlib, time
from fastapi import FastAPI, Request, Header, HTTPException
from pydantic import BaseModel, Field
from typing import Optional
from app.config import WEBHOOK_SECRET
from app.models import init_db
from app.storage import insert_message, query_messages, stats
from app.logging_utils import log
from app.metrics import http_requests, webhook_requests, latency, generate_latest

app = FastAPI()

@app.on_event("startup")
def startup():
    init_db()

class WebhookMsg(BaseModel):
    message_id: str
    from_: str = Field(alias="from")
    to: str
    ts: str
    text: Optional[str] = Field(default=None, max_length=4096)

@app.get("/health/live")
def live():
    return {"status": "ok"}

@app.get("/health/ready")
def ready():
    return {"status": "ok"}

@app.post("/webhook")
async def webhook(
    request: Request,
    x_signature: str = Header(None)
):
    start = time.time()
    body = await request.body()
    expected = hmac.new(
        WEBHOOK_SECRET.encode(), body, hashlib.sha256
    ).hexdigest()

    if not x_signature or not hmac.compare_digest(expected, x_signature):
        webhook_requests.labels("invalid_signature").inc()
        raise HTTPException(401, detail="invalid signature")

    data = await request.json()
    result = insert_message(data)
    webhook_requests.labels(result).inc()

    latency.observe((time.time() - start) * 1000)
    http_requests.labels("/webhook", "200").inc()

    log(request, 200, int((time.time() - start) * 1000),
        message_id=data.get("message_id"),
        dup=result == "duplicate",
        result=result)

    return {"status": "ok"}

@app.get("/messages")
def messages(
    limit: int = 50,
    offset: int = 0,
    from_: Optional[str] = None,
    since: Optional[str] = None,
    q: Optional[str] = None,
):
    total, rows = query_messages(limit, offset, from_, since, q)
    return {
        "data": [dict(r) for r in rows],
        "total": total,
        "limit": limit,
        "offset": offset,
    }

@app.get("/stats")
def get_stats():
    total, senders, first, last = stats()
    return {
        "total_messages": total,
        "senders_count": len(senders),
        "messages_per_sender": [dict(s) for s in senders],
        "first_message_ts": first,
        "last_message_ts": last,
    }

@app.get("/metrics")
def metrics():
    return generate_latest()
