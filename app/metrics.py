from prometheus_client import Counter, Histogram, generate_latest

http_requests = Counter(
    "http_requests_total", "HTTP requests", ["path", "status"]
)

webhook_requests = Counter(
    "webhook_requests_total", "Webhook outcomes", ["result"]
)

latency = Histogram("request_latency_ms", "Latency", buckets=(100, 500, 1000))
