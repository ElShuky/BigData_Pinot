import sys
import time
import random
from kafka import KafkaProducer

rate = int(sys.argv[1]) if len(sys.argv) > 1 else 2500
duration = int(sys.argv[2]) if len(sys.argv) > 2 else 25

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    linger_ms=5,
    batch_size=65536,
    acks=0
)

EVENT_TYPES = [
    ("login_fail", "DENY", "HIGH", "203.0.113.45", 22, 120),
    ("port_scan", "BLOCKED", "CRITICAL", "198.51.100.77", 80, 75),
    ("login_success", "ALLOW", "LOW", "10.0.1.15", 443, 7600),
    ("api_request", "ALLOW", "LOW", "10.0.1.22", 443, 8100),
    ("file_access", "ALLOW", "LOW", "10.0.1.30", 445, 7300),
]

templates = []
for i in range(1000):
    ev, act, sev, src, port, b_base = random.choice(EVENT_TYPES)
    templates.append(
        f',"src_ip":"{src}","dst_ip":"10.0.0.5","dst_port":{port},'
        f'"protocol":"TCP","username":"user_{i%50}","event_type":"{ev}",'
        f'"action":"{act}","severity":"{sev}","bytes_transferred":{b_base + random.randint(0, 500)}}}'
    )

batch_interval = 0.05
events_per_batch = max(1, int(rate * batch_interval))
end_time = time.time() + duration
sent = 0

print(f"Enviando {rate} ev/s durante {duration}s...")
while time.time() < end_time:
    t0 = time.perf_counter()
    now_ms = int(time.time() * 1000)
    for i in range(events_per_batch):
        msg = f'{{"timestamp_ms":{now_ms}{templates[(sent + i) % 1000]}'.encode("utf-8")
        producer.send("network-events", value=msg)
    sent += events_per_batch
    elapsed = time.perf_counter() - t0
    if batch_interval - elapsed > 0:
        time.sleep(batch_interval - elapsed)

producer.flush()
print(f"Completado: {sent} eventos enviados.")