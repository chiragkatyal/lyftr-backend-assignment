import json
import time
import uuid
from datetime import datetime

def log(request, status, latency, **extra):
    payload = {
        "ts": datetime.utcnow().isoformat() + "Z",
        "level": "INFO",
        "request_id": str(uuid.uuid4()),
        "method": request.method,
        "path": request.url.path,
        "status": status,
        "latency_ms": latency,
    }
    payload.update(extra)
    print(json.dumps(payload))
