#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Scan an HBase table and report row count / basic column stats."""

#from __future__ import annotations

import argparse
import json
import sys
import time
from collections import Counter
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple


import happybase


DEFAULT_HBASE_TABLE = "recommend:video_search_recomm_poms_type1_embedding_multimode"
DEFAULT_HBASE_HOSTS = (
    "h1003.dm.migu.cn,h1004.dm.migu.cn,h1005.dm.migu.cn,h1006.dm.migu.cn,"
    "h1007.dm.migu.cn,h1008.dm.migu.cn,h1009.dm.migu.cn,h1010.dm.migu.cn,"
    "h1011.dm.migu.cn,h1012.dm.migu.cn"
)


def to_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def to_bytes(value: Any) -> bytes:
    if isinstance(value, bytes):
        return value
    return str(value).encode("utf-8")


def is_not_blank(value: Any) -> bool:
    return value is not None and str(value).strip() != ""


def parse_csv(value: Optional[str]) -> List[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def short_value(value: Any, max_len: int = 80) -> str:
    if value is None:
        return "<missing>"
    text = str(value)
    if len(text) <= max_len:
        return text
    return "%s...<len=%s>" % (text[:max_len], len(text))


def connect_hbase(hosts: Iterable[str], port: int, timeout_ms: int):
    last_error = None
    for host in hosts:
        host = host.strip()
        if not host:
            continue
        try:
            connection = happybase.Connection(
                host=host,
                port=port,
                timeout=timeout_ms,
                autoconnect=True,
            )
            print(
                "Connected to HBase thrift %s:%s" % (host, port),
                file=sys.stderr,
                flush=True,
            )
            return connection
        except Exception as exc:
            last_error = exc
            print(
                "Could not connect to HBase thrift %s:%s: %s" % (host, port, exc),
                file=sys.stderr,
            )
    raise RuntimeError("Could not connect to any HBase thrift host") from last_error


def decode_hbase_row(row: Mapping[Any, Any], column_family: str) -> Dict[str, str]:
    prefix = "%s:" % column_family
    result: Dict[str, str] = {}
    for column, value in row.items():
        column_text = to_text(column) or ""
        qualifier = (
            column_text[len(prefix) :]
            if column_text.startswith(prefix)
            else column_text
        )
        value_text = to_text(value)
        if value_text is not None:
            result[qualifier] = value_text
    return result


def load_hbase_settings_from_config(path: str) -> Dict[str, Any]:
    try:
        import yaml
    except ImportError as exc:
        raise RuntimeError(
            "PyYAML is required when using -c/--config. Install pyyaml first."
        ) from exc

    with open(path, "r", encoding="utf-8") as fp:
        config = yaml.safe_load(fp) or {}

    hbase = config.get("hbase") or {}
    tables = config.get("tables") or {}
    one_shot = config.get("one_shot") or {}

    hosts = hbase.get("thrift_hosts")
    if isinstance(hosts, list):
        hosts_text = ",".join(str(item) for item in hosts if is_not_blank(item))
    else:
        hosts_text = str(hosts or "").strip()

    table = (
        one_shot.get("write_table")
        or tables.get("poms_type140_embedding")
        or DEFAULT_HBASE_TABLE
    )
    return {
        "hosts": hosts_text or DEFAULT_HBASE_HOSTS,
        "port": int(hbase.get("thrift_port", 9090)),
        "timeout_ms": int(hbase.get("timeout_ms", 30000)),
        "column_family": str(hbase.get("column_family", "cf")),
        "table": str(table),
    }


def get_row(
    table: Any, column_family: str, row_key: str
) -> Tuple[str, Dict[str, str]]:
    row = table.row(to_bytes(row_key))
    decoded = decode_hbase_row(row, column_family)
    if not decoded:
        raise RuntimeError("HBase row not found: %s" % row_key)
    return row_key, decoded


def scan_table_stats(
    table: Any,
    column_family: str,
    scan_batch_size: int,
    limit: int,
    progress_every: int,
    sample_size: int,
    columns: Optional[List[str]],
) -> Dict[str, Any]:
    scan_kwargs: Dict[str, Any] = {"batch_size": scan_batch_size}
    if columns:
        scan_kwargs["columns"] = [
            to_bytes("%s:%s" % (column_family, column)) for column in columns
        ]

    row_count = 0
    empty_row_count = 0
    column_presence = Counter()
    samples: List[Dict[str, Any]] = []
    started = time.time()
    last_progress = started

    for row_key, row in table.scan(**scan_kwargs):
        row_count += 1
        decoded = decode_hbase_row(row, column_family)
        if not decoded:
            empty_row_count += 1
        else:
            column_presence.update(decoded.keys())

        if len(samples) < sample_size:
            samples.append(
                {
                    "row_key": to_text(row_key),
                    "columns": {
                        key: short_value(value) for key, value in sorted(decoded.items())
                    },
                }
            )

        now = time.time()
        if progress_every > 0 and (
            row_count % progress_every == 0 or now - last_progress >= 30
        ):
            elapsed = max(now - started, 0.001)
            print(
                "Progress: rows=%s elapsed=%.1fs rate=%.1f rows/s"
                % (row_count, elapsed, row_count / elapsed),
                file=sys.stderr,
                flush=True,
            )
            last_progress = now

        if limit > 0 and row_count >= limit:
            break

    elapsed = time.time() - started
    return {
        "row_count": row_count,
        "empty_row_count": empty_row_count,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_second": round(row_count / elapsed, 3) if elapsed > 0 else None,
        "unique_columns": sorted(column_presence.keys()),
        "column_presence_counts": dict(sorted(column_presence.items())),
        "samples": samples,
        "scan_truncated": bool(limit > 0 and row_count >= limit),
    }


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Read an HBase table via Thrift and report row count / column presence. "
            "Full-table count requires a full scan."
        )
    )
    parser.add_argument(
        "-c",
        "--config",
        help="Optional YAML config (reuse kafka_to_hbase config.yml HBase settings).",
    )
    parser.add_argument(
        "--hbase-hosts",
        help="Comma-separated HBase thrift hosts. Default comes from config or built-in hosts.",
    )
    parser.add_argument("--hbase-port", type=int, help="HBase thrift port.")
    parser.add_argument(
        "--hbase-timeout-ms", type=int, help="HBase thrift timeout in ms."
    )
    parser.add_argument("--hbase-table", help="HBase table name.")
    parser.add_argument("--column-family", help="HBase column family. Default cf.")
    parser.add_argument(
        "--row-key",
        help="If set, only fetch this one row and print it (no full-table count).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Stop after scanning N rows. Default 0 means scan all rows.",
    )
    parser.add_argument(
        "--hbase-scan-batch-size",
        type=int,
        default=500,
        help="HBase scan batch size.",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=10000,
        help="Print progress every N rows (also every ~30s).",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=3,
        help="Include N sample rows in the JSON summary. Default 3.",
    )
    parser.add_argument(
        "--columns",
        help=(
            "Optional comma-separated qualifiers to scan "
            "(e.g. assetId,version,time). Default scans all columns."
        ),
    )
    parser.add_argument(
        "--output",
        help="Write JSON summary to this file. Always prints a short summary to stdout.",
    )
    args = parser.parse_args(argv)

    settings = {
        "hosts": DEFAULT_HBASE_HOSTS,
        "port": 9090,
        "timeout_ms": 30000,
        "column_family": "cf",
        "table": DEFAULT_HBASE_TABLE,
    }
    if args.config:
        settings.update(load_hbase_settings_from_config(args.config))
    if args.hbase_hosts:
        settings["hosts"] = args.hbase_hosts
    if args.hbase_port is not None:
        settings["port"] = args.hbase_port
    if args.hbase_timeout_ms is not None:
        settings["timeout_ms"] = args.hbase_timeout_ms
    if args.hbase_table:
        settings["table"] = args.hbase_table
    if args.column_family:
        settings["column_family"] = args.column_family

    connection = connect_hbase(
        hosts=parse_csv(str(settings["hosts"])),
        port=int(settings["port"]),
        timeout_ms=int(settings["timeout_ms"]),
    )
    try:
        table = connection.table(str(settings["table"]))
        column_family = str(settings["column_family"])

        if is_not_blank(args.row_key):
            row_key, decoded = get_row(table, column_family, str(args.row_key))
            summary = {
                "table": settings["table"],
                "column_family": column_family,
                "mode": "get",
                "row_key": row_key,
                "column_count": len(decoded),
                "columns": sorted(decoded.keys()),
                "row": {
                    key: short_value(value, max_len=200)
                    for key, value in sorted(decoded.items())
                },
            }
        else:
            print(
                "Scanning table=%s column_family=%s limit=%s ..."
                % (settings["table"], column_family, args.limit or "ALL"),
                file=sys.stderr,
                flush=True,
            )
            stats = scan_table_stats(
                table=table,
                column_family=column_family,
                scan_batch_size=args.hbase_scan_batch_size,
                limit=args.limit,
                progress_every=args.progress_every,
                sample_size=max(0, args.sample_size),
                columns=parse_csv(args.columns),
            )
            summary = {
                "table": settings["table"],
                "column_family": column_family,
                "mode": "scan",
                **stats,
            }
    finally:
        connection.close()

    text = json.dumps(summary, ensure_ascii=False, indent=2)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as fp:
            fp.write(text)
            fp.write("\n")
        print("Wrote JSON summary to %s" % args.output)

    if summary.get("mode") == "scan":
        print("table=%s" % summary["table"])
        print("row_count=%s" % summary["row_count"])
        print("empty_row_count=%s" % summary["empty_row_count"])
        print("elapsed_seconds=%s" % summary["elapsed_seconds"])
        print("unique_column_count=%s" % len(summary.get("unique_columns") or []))
        print("scan_truncated=%s" % summary["scan_truncated"])
    else:
        print("table=%s" % summary["table"])
        print("row_key=%s" % summary["row_key"])
        print("column_count=%s" % summary["column_count"])
        print("columns=%s" % ",".join(summary["columns"]))

    if not args.output:
        print(text)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("Stopped", file=sys.stderr)
        sys.exit(130)
    except Exception as exc:
        print("hbase_table_stats failed: %s" % exc, file=sys.stderr)
        sys.exit(1)
