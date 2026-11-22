#!/usr/bin/env python3
"""Generador paralelo de dataset común de ciberseguridad en múltiples shards JSONL."""

from __future__ import annotations

import argparse
import os
import random
import time
from multiprocessing import Pool
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
RAW_DATASET_DIR = REPO_ROOT / "workspace" / "rawdata" / "exp_dataset"
TARGET_RARE_IP = "172.20.45.99"

NORMAL_EVENT_TYPES = ("login_success", "session_start", "file_access", "api_request")
NORMAL_PORTS = (80, 443, 8080)
SCAN_PORTS = (21, 22, 23, 25, 53, 80, 110, 443, 1433, 3306, 3389, 8080)
USERS = (
    "admin",
    "usuario1",
    "dev_ops",
    "finance_user",
    "guest",
    "root",
    "analyst",
    "sec_bot",
    "backup_svc",
    "auditor",
)
DST_IP = "172.16.0.10"


def _build_zipf_pool(items: list[str], table_size: int = 8192) -> tuple[str, ...]:
    """Construye una tabla de búsqueda O(1) con frecuencias Zipf aproximadas."""
    weights = [1.0 / (rank + 1) for rank in range(len(items))]
    total_w = sum(weights)
    bucket: list[str] = []
    for item, weight in zip(items, weights):
        count = max(1, int(round((weight / total_w) * table_size)))
        bucket.extend([item] * count)
    return tuple(bucket)


def _build_normal_ip_pool(rare_ip: str) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Genera 5.000 IPs corporativas (sin coincidir con rare_ip) divididas en calientes/cola."""
    pool: list[str] = []
    # IPs en rango 10.x (siempre < 172.20.45.99 lexicográficamente)
    for third in range(20):
        for fourth in range(1, 201):
            ip = f"10.0.{third}.{fourth}"
            if ip != rare_ip:
                pool.append(ip)
    # IPs en rango 192.168.x (siempre > 172.20.45.99 lexicográficamente)
    for third in range(10):
        for fourth in range(1, 101):
            ip = f"192.168.{third}.{fourth}"
            if ip != rare_ip:
                pool.append(ip)

    hot_ips = _build_zipf_pool(pool[:40], table_size=4096)
    tail_ips = tuple(pool[40:])
    return hot_ips, tail_ips


def _generate_shard(task: tuple) -> tuple[int, str, int, int, int]:
    (
        shard_idx,
        num_rows,
        out_file_str,
        base_ts_ms,
        span_ms,
        total_shards,
        seed,
        rare_ip,
        rare_target,
    ) = task

    rng = random.Random(seed + shard_idx)
    hot_ips, tail_ips = _build_normal_ip_pool(rare_ip)
    user_pool = _build_zipf_pool(list(USERS), table_size=2048)

    hot_len = len(hot_ips)
    tail_len = len(tail_ips)
    user_len = len(user_pool)

    # Ventana temporal asignada a este shard dentro del rango de 24 horas
    shard_start_ts = base_ts_ms + int((shard_idx / total_shards) * span_ms)
    shard_span = max(1, int(span_ms / total_shards))

    rare_emitted = 0
    lines: list[str] = []
    flush_threshold = 10000
    bytes_written = 0

    with open(out_file_str, "w", encoding="utf-8", buffering=1024 * 1024) as fh:
        for i in range(num_rows):
            ts_ms = shard_start_ts + (i * shard_span) // max(1, num_rows)
            r = rng.random()

            # Si quedan filas por cubrir al final del shard y faltaron eventos rare_ip
            remaining = num_rows - i
            need_force_rare = rare_target > rare_emitted and remaining <= (rare_target - rare_emitted)

            if not need_force_rare and r < 0.60:
                event_type = NORMAL_EVENT_TYPES[rng.randrange(4)]
                if rng.random() < 0.70:
                    src_ip = hot_ips[rng.randrange(hot_len)]
                else:
                    src_ip = tail_ips[rng.randrange(tail_len)]
                dst_port = NORMAL_PORTS[rng.randrange(3)]
                protocol = "HTTPS" if dst_port != 80 else "TCP"
                username = user_pool[rng.randrange(user_len)]
                action = "ALLOW"
                severity = "LOW"
                bytes_tx = rng.randint(200, 15000)
            elif need_force_rare or r < 0.85:
                event_type = "login_fail"
                if rare_emitted < rare_target:
                    src_ip = rare_ip
                    rare_emitted += 1
                else:
                    src_ip = "203.0.113.45"
                dst_port = 22
                protocol = "SSH"
                username = "admin" if rng.random() < 0.70 else "root"
                action = "DENY"
                severity = "HIGH"
                bytes_tx = rng.randint(50, 200)
            else:
                event_type = "port_scan"
                src_ip = "198.51.100.77"
                dst_port = SCAN_PORTS[rng.randrange(12)]
                protocol = "TCP"
                username = "anonymous"
                action = "BLOCKED"
                severity = "CRITICAL"
                bytes_tx = rng.randint(40, 120)

            lines.append(
                f'{{"timestamp_ms":{ts_ms},"src_ip":"{src_ip}","dst_ip":"{DST_IP}",'
                f'"protocol":"{protocol}","username":"{username}","event_type":"{event_type}",'
                f'"action":"{action}","severity":"{severity}","dst_port":{dst_port},'
                f'"bytes_transferred":{bytes_tx}}}\n'
            )

            if len(lines) >= flush_threshold:
                chunk = "".join(lines)
                fh.write(chunk)
                bytes_written += len(chunk)
                lines.clear()

        if lines:
            chunk = "".join(lines)
            fh.write(chunk)
            bytes_written += len(chunk)
            lines.clear()

    return shard_idx, out_file_str, num_rows, bytes_written, rare_emitted


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Genera un dataset común de eventos de ciberseguridad particionado en shards JSONL."
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=5_000_000,
        help="Cantidad total de eventos a generar (por defecto: 5000000, ~1.1 GB).",
    )
    parser.add_argument(
        "--shards",
        type=int,
        default=20,
        help="Cantidad de archivos JSONL a generar (cada uno produce 1 segmento Pinot).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=RAW_DATASET_DIR,
        help=f"Directorio de salida (por defecto: {RAW_DATASET_DIR}).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Semilla base para reproducibilidad (por defecto: 42).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=min(8, os.cpu_count() or 4),
        help="Procesos paralelos para generación de shards.",
    )
    parser.add_argument(
        "--rare-ip",
        type=str,
        default=TARGET_RARE_IP,
        help=f"IP aislada en un único shard para probar Bloom Filter (por defecto: {TARGET_RARE_IP}).",
    )
    parser.add_argument(
        "--rare-shard",
        type=int,
        default=None,
        help="Índice del shard que contendrá la IP rara (por defecto: shards // 2).",
    )
    parser.add_argument(
        "--rare-count",
        type=int,
        default=500,
        help="Cantidad de eventos con la IP rara dentro del shard objetivo (por defecto: 500).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.rows <= 0:
        raise SystemExit("ERROR: --rows debe ser mayor que 0.")
    if args.shards <= 0:
        raise SystemExit("ERROR: --shards debe ser mayor que 0.")

    rare_shard = args.rare_shard if args.rare_shard is not None else (args.shards // 2)
    if not (0 <= rare_shard < args.shards):
        raise SystemExit(f"ERROR: --rare-shard debe estar entre 0 y {args.shards - 1}.")

    out_dir: Path = args.output
    out_dir.mkdir(parents=True, exist_ok=True)
    for old_file in out_dir.glob("part_*.json"):
        old_file.unlink()

    base_rows = args.rows // args.shards
    remainder = args.rows % args.shards
    pad = max(2, len(str(args.shards - 1)))

    now_ms = int(time.time() * 1000)
    span_ms = 24 * 3600 * 1000  # 24 horas de ventana temporal simulada
    base_ts_ms = now_ms - span_ms

    tasks = []
    for idx in range(args.shards):
        shard_rows = base_rows + (1 if idx < remainder else 0)
        rare_target = min(args.rare_count, shard_rows) if idx == rare_shard else 0
        out_file = out_dir / f"part_{idx:0{pad}d}.json"
        tasks.append(
            (
                idx,
                shard_rows,
                str(out_file),
                base_ts_ms,
                span_ms,
                args.shards,
                args.seed,
                args.rare_ip,
                rare_target,
            )
        )

    workers = max(1, min(args.workers, args.shards))
    print(f"Generando {args.rows:,} eventos en {args.shards} shards usando {workers} proceso(s)...")
    print(f"Directorio de salida: {out_dir}")
    t0 = time.perf_counter()

    if workers == 1:
        results = [_generate_shard(t) for t in tasks]
    else:
        with Pool(processes=workers) as pool:
            results = pool.map(_generate_shard, tasks)

    elapsed = time.perf_counter() - t0
    total_rows = sum(r[2] for r in results)
    total_bytes = sum(r[3] for r in results)
    total_rare = sum(r[4] for r in results)
    size_mb = total_bytes / (1024 * 1024)
    size_gb = total_bytes / (1024 * 1024 * 1024)

    print("=" * 65)
    print(f"Dataset generado en {elapsed:.2f} s")
    print(f"Total de registros : {total_rows:,}")
    print(f"Total de shards    : {len(results)}")
    print(f"Tamaño total       : {size_mb:.2f} MiB ({size_gb:.3f} GiB)")
    print(
        f"IP rara (Bloom)    : {args.rare_ip} -> {total_rare} eventos "
        f"en part_{rare_shard:0{pad}d}.json"
    )
    print("=" * 65)


if __name__ == "__main__":
    main()
