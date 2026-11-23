import time
import urllib.request
import json
import subprocess

SQL_QUERY = """
SELECT
  event_type,
  severity,
  count(*) AS total_eventos,
  sum(bytes_transferred) AS total_bytes,
  avg(bytes_transferred) AS promedio_bytes,
  max(timestamp_ms) AS ultimo_ts
FROM security_events
GROUP BY event_type, severity
ORDER BY total_eventos DESC
"""

def run_query():
    req = urllib.request.Request(
        "http://localhost:8099/query/sql",
        data=json.dumps({"sql": SQL_QUERY}).encode("utf-8"),
        headers={"Content-Type": "application/json"}
    )
    t0 = time.perf_counter()
    with urllib.request.urlopen(req) as resp:
        res = json.loads(resp.read().decode("utf-8"))
    query_ms = (time.perf_counter() - t0) * 1000
    now_ms = int(time.time() * 1000)

    rows = res.get("resultTable", {}).get("rows", [])
    total_rows = int(sum(float(r[2]) for r in rows))
    max_ts = int(max((float(r[5]) for r in rows), default=now_ms))
    freshness_ms = max(0, now_ms - max_ts) if total_rows > 0 else 0
    pinot_time_ms = int(float(res.get("timeUsedMs", query_ms)))
    return total_rows, freshness_ms, pinot_time_ms, round(query_ms, 2)

def get_segments_and_ram():
    with urllib.request.urlopen("http://localhost:9000/segments/security_events") as resp:
        segs = json.loads(resp.read().decode("utf-8"))
    num_segs = int(sum(len(item.get("REALTIME", [])) for item in segs))
    
    out = subprocess.check_output(
        ["docker", "stats", "--no-stream", "--format", "{{.MemUsage}}", "pinot-server"]
    ).decode("utf-8").strip()
    return num_segs, out

print("Iter | Total_Filas | Freshness_Lag(ms) | Pinot_SQL(ms) | End2End_SQL(ms) | Segmentos | RAM_Server")
lags, sqls, e2es = [], [], []

for i in range(1, 11):
    rows, fresh, p_ms, e2e_ms = run_query()
    segs, ram = get_segments_and_ram()
    print(f"{i:4d} | {rows:11d} | {fresh:17d} | {p_ms:13d} | {e2e_ms:15.2f} | {segs:9d} | {ram}")
    if rows > 0:
        lags.append(fresh)
        sqls.append(p_ms)
        e2es.append(e2e_ms)
    time.sleep(2)

if lags:
    print("-" * 92)
    print(f"PROMEDIO (con ingesta activa) -> Freshness: {sum(lags)/len(lags):.1f} ms | Pinot_SQL: {sum(sqls)/len(sqls):.1f} ms | End2End: {sum(e2es)/len(e2es):.2f} ms")