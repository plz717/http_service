#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Scan an HBase table and report row count / basic column stats.

Copy of hbase_table_stats.py that additionally prints the newest and the
oldest samples still retained in the Kafka topic.
"""

#from __future__ import annotations

import argparse
import json
import sys
import time
from collections import Counter
from datetime import datetime
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple


import happybase


DEFAULT_HBASE_TABLE = "recommend:video_search_recomm_poms_type1_embedding_multimode"
DEFAULT_HBASE_HOSTS = (
    "h1003.dm.migu.cn,h1004.dm.migu.cn,h1005.dm.migu.cn,h1006.dm.migu.cn,"
    "h1007.dm.migu.cn,h1008.dm.migu.cn,h1009.dm.migu.cn,h1010.dm.migu.cn,"
    "h1011.dm.migu.cn,h1012.dm.migu.cn"
)
DEFAULT_KAFKA_BOOTSTRAP_SERVERS = (
    "kafka101.dm.migu.cn:9092,kafka102.dm.migu.cn:9092,kafka103.dm.migu.cn:9092,"
    "kafka104.dm.migu.cn:9092,kafka105.dm.migu.cn:9092,kafka106.dm.migu.cn:9092,"
    "kafka107.dm.migu.cn:9092"
)
DEFAULT_KAFKA_TOPIC = "video-search-recomm-poms-content"


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


def load_config_file(path: str) -> Dict[str, Any]:
    try:
        import yaml
    except ImportError as exc:
        raise RuntimeError(
            "PyYAML is required when using -c/--config. Install pyyaml first."
        ) from exc

    with open(path, "r", encoding="utf-8") as fp:
        return yaml.safe_load(fp) or {}


def load_hbase_settings_from_config(path: str) -> Dict[str, Any]:
    config = load_config_file(path)

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


def load_kafka_settings_from_config(path: str) -> Dict[str, Any]:
    config = load_config_file(path)
    kafka = config.get("kafka") or {}

    servers = kafka.get("bootstrap_servers")
    if isinstance(servers, list):
        servers_text = ",".join(str(item) for item in servers if is_not_blank(item))
    else:
        servers_text = str(servers or "").strip()

    settings: Dict[str, Any] = {}
    if servers_text:
        settings["bootstrap_servers"] = servers_text
    if is_not_blank(kafka.get("topic")):
        settings["topic"] = str(kafka["topic"]).strip()
    if is_not_blank(kafka.get("api_version")):
        settings["api_version"] = str(kafka["api_version"]).strip()
    if kafka.get("poll_timeout_seconds") is not None:
        settings["poll_timeout_seconds"] = float(kafka["poll_timeout_seconds"])
    if kafka.get("max_poll_records") is not None:
        settings["max_poll_records"] = int(kafka["max_poll_records"])
    return settings


def parse_kafka_api_version(value: Any) -> Optional[Tuple[int, ...]]:
    if not is_not_blank(value):
        return None
    if isinstance(value, (list, tuple)):
        parts = value
    else:
        parts = str(value).strip().split(".")
    try:
        return tuple(int(part) for part in parts)
    except ValueError:
        raise ValueError("kafka api_version must look like 2.0.0")


def wait_topic_partitions(
    consumer: Any, topic: str, timeout_seconds: float
) -> List[Any]:
    from kafka import TopicPartition

    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        partitions = consumer.partitions_for_topic(topic)
        if partitions:
            return [
                TopicPartition(topic, partition) for partition in sorted(partitions)
            ]
        time.sleep(0.5)
    raise RuntimeError("Could not load Kafka partitions for topic %s" % topic)


def format_kafka_timestamp(timestamp_ms: Any) -> Optional[str]:
    if timestamp_ms is None:
        return None
    try:
        value = int(timestamp_ms)
    except (TypeError, ValueError):
        return None
    if value <= 0:
        return None
    return datetime.fromtimestamp(value / 1000.0).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def parse_kafka_payload(text: Optional[str]) -> Any:
    if text is None:
        return None
    stripped = text.strip()
    if not stripped or stripped[0] not in "[{\"":
        return None
    try:
        payload = json.loads(stripped)
    except ValueError:
        return None
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except ValueError:
            return None
    return payload


def find_payload_field(payload: Any, names: Iterable[str], depth: int = 4) -> Any:
    if depth < 0:
        return None
    wanted = list(names)
    if isinstance(payload, Mapping):
        for name in wanted:
            if name in payload and is_not_blank(payload[name]):
                return payload[name]
        for item in payload.values():
            found = find_payload_field(item, wanted, depth - 1)
            if found is not None:
                return found
    elif isinstance(payload, list):
        for item in payload:
            found = find_payload_field(item, wanted, depth - 1)
            if found is not None:
                return found
    return None


def summarize_kafka_message(message: Any, value_max_len: int) -> Dict[str, Any]:
    value_text = to_text(message.value)
    sample: Dict[str, Any] = {
        "topic": message.topic,
        "partition": message.partition,
        "offset": message.offset,
        "kafka_timestamp": message.timestamp,
        "kafka_time": format_kafka_timestamp(message.timestamp),
        "key": to_text(message.key),
        "value_length": len(value_text) if value_text is not None else None,
        "value_preview": short_value(value_text, max_len=value_max_len),
    }
    payload = parse_kafka_payload(value_text)
    if isinstance(payload, Mapping):
        sample["payload_keys"] = sorted(str(key) for key in payload.keys())
        asset_id = find_payload_field(payload, ["assetId", "asset_id", "assert_id"])
        if asset_id is not None:
            sample["assetId"] = str(asset_id)
        message_type = payload.get("type")
        if message_type is None:
            message_type = find_payload_field(payload, ["type"])
        if message_type is not None:
            sample["type"] = str(message_type)
    return sample


def sort_key_by_time(message: Any) -> Tuple[int, int, int]:
    timestamp = message.timestamp if message.timestamp is not None else -1
    return (int(timestamp), int(message.partition), int(message.offset))


def collect_partition_records(
    consumer: Any,
    partitions: List[Any],
    start_offsets: Mapping[Any, int],
    end_offsets: Mapping[Any, int],
    per_partition_limit: int,
    poll_timeout_ms: int,
    max_poll_records: int,
    timeout_seconds: float,
) -> List[Any]:
    """Read up to per_partition_limit records per partition from start_offsets."""

    consumer.resume(*partitions)
    pending = []
    for partition in partitions:
        start = start_offsets[partition]
        if start >= end_offsets[partition]:
            consumer.pause(partition)
            continue
        consumer.seek(partition, start)
        pending.append(partition)
    if not pending:
        return []

    collected: Dict[Any, List[Any]] = {partition: [] for partition in pending}
    remaining = set(pending)
    deadline = time.time() + timeout_seconds
    while remaining and time.time() < deadline:
        records_by_partition = consumer.poll(
            timeout_ms=poll_timeout_ms, max_records=max_poll_records
        )
        if not records_by_partition:
            continue
        for partition, records in records_by_partition.items():
            if partition not in collected:
                continue
            for message in records:
                if message.offset >= end_offsets[partition]:
                    continue
                if len(collected[partition]) >= per_partition_limit:
                    break
                collected[partition].append(message)
            if (
                len(collected[partition]) >= per_partition_limit
                or consumer.position(partition) >= end_offsets[partition]
            ):
                consumer.pause(partition)
                remaining.discard(partition)

    messages: List[Any] = []
    for partition_messages in collected.values():
        messages.extend(partition_messages)
    return messages


def collect_kafka_samples(
    bootstrap_servers: List[str],
    topic: str,
    api_version: Optional[Tuple[int, ...]],
    latest_sample_size: int,
    oldest_sample_size: int,
    poll_timeout_ms: int,
    max_poll_records: int,
    fetch_timeout_seconds: float,
    metadata_timeout_seconds: float,
    value_max_len: int,
) -> Dict[str, Any]:
    from kafka import KafkaConsumer

    consumer_kwargs: Dict[str, Any] = {
        "bootstrap_servers": bootstrap_servers,
        "enable_auto_commit": False,
        "auto_offset_reset": "earliest",
        "max_poll_records": max_poll_records,
    }
    if api_version is not None:
        consumer_kwargs["api_version"] = api_version

    print(
        "Reading Kafka samples topic=%s latest=%s oldest=%s ..."
        % (topic, latest_sample_size, oldest_sample_size),
        file=sys.stderr,
        flush=True,
    )
    started = time.time()
    consumer = KafkaConsumer(**consumer_kwargs)
    try:
        partitions = wait_topic_partitions(consumer, topic, metadata_timeout_seconds)
        consumer.assign(partitions)
        beginning_offsets = consumer.beginning_offsets(partitions)
        end_offsets = consumer.end_offsets(partitions)
        retained = sum(
            end_offsets[partition] - beginning_offsets[partition]
            for partition in partitions
        )

        oldest_messages: List[Any] = []
        if oldest_sample_size > 0:
            oldest_messages = collect_partition_records(
                consumer=consumer,
                partitions=partitions,
                start_offsets=beginning_offsets,
                end_offsets=end_offsets,
                per_partition_limit=oldest_sample_size,
                poll_timeout_ms=poll_timeout_ms,
                max_poll_records=max_poll_records,
                timeout_seconds=fetch_timeout_seconds,
            )

        latest_messages: List[Any] = []
        if latest_sample_size > 0:
            latest_start_offsets = {
                partition: max(
                    beginning_offsets[partition],
                    end_offsets[partition] - latest_sample_size,
                )
                for partition in partitions
            }
            latest_messages = collect_partition_records(
                consumer=consumer,
                partitions=partitions,
                start_offsets=latest_start_offsets,
                end_offsets=end_offsets,
                per_partition_limit=latest_sample_size,
                poll_timeout_ms=poll_timeout_ms,
                max_poll_records=max_poll_records,
                timeout_seconds=fetch_timeout_seconds,
            )
    finally:
        consumer.close()

    oldest_sorted = sorted(oldest_messages, key=sort_key_by_time)[:oldest_sample_size]
    latest_sorted = sorted(latest_messages, key=sort_key_by_time, reverse=True)[
        :latest_sample_size
    ]
    return {
        "enabled": True,
        "topic": topic,
        "bootstrap_servers": bootstrap_servers,
        "partition_count": len(partitions),
        "retained_messages": retained,
        "partition_offsets": {
            str(partition.partition): {
                "beginning_offset": beginning_offsets[partition],
                "end_offset": end_offsets[partition],
            }
            for partition in partitions
        },
        "read_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "elapsed_seconds": round(time.time() - started, 3),
        "latest_samples": [
            summarize_kafka_message(message, value_max_len) for message in latest_sorted
        ],
        "oldest_samples": [
            summarize_kafka_message(message, value_max_len) for message in oldest_sorted
        ],
    }


def print_kafka_samples(kafka_summary: Mapping[str, Any]) -> None:
    if not kafka_summary.get("enabled"):
        return
    print("kafka_topic=%s" % kafka_summary.get("topic"))
    if kafka_summary.get("error"):
        print("kafka_error=%s" % kafka_summary["error"])
        return
    print("kafka_partition_count=%s" % kafka_summary.get("partition_count"))
    print("kafka_retained_messages=%s" % kafka_summary.get("retained_messages"))

    for title, key in (
        ("=== kafka latest samples ===", "latest_samples"),
        ("=== kafka oldest samples ===", "oldest_samples"),
    ):
        print(title)
        samples = kafka_summary.get(key) or []
        if not samples:
            print("no samples")
            continue
        for index, sample in enumerate(samples, start=1):
            print(
                "#%s partition=%s offset=%s kafka_time=%s assetId=%s type=%s"
                % (
                    index,
                    sample.get("partition"),
                    sample.get("offset"),
                    sample.get("kafka_time"),
                    sample.get("assetId", "<missing>"),
                    sample.get("type", "<missing>"),
                )
            )
            print("   value=%s" % sample.get("value_preview"))


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
            "Read an HBase table via Thrift and report row count / column presence, "
            "plus the newest and oldest samples retained in the Kafka topic. "
            "Full-table count requires a full scan."
        )
    )
    parser.add_argument(
        "-c",
        "--config",
        help="Optional YAML config (reuse kafka_to_hbase config.yml HBase/Kafka settings).",
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
    parser.add_argument(
        "--no-kafka-samples",
        action="store_true",
        help="Skip reading Kafka samples and only report HBase stats.",
    )
    parser.add_argument(
        "--kafka-bootstrap-servers",
        help="Comma-separated Kafka bootstrap servers. Default comes from config or built-in servers.",
    )
    parser.add_argument("--kafka-topic", help="Kafka topic to sample.")
    parser.add_argument(
        "--kafka-api-version", help="Kafka api_version, e.g. 2.0.0. Default from config."
    )
    parser.add_argument(
        "--kafka-sample-size",
        type=int,
        default=3,
        help="Kafka samples to print for both newest and oldest messages. Default 3.",
    )
    parser.add_argument(
        "--kafka-latest-sample-size",
        type=int,
        help="Override the number of newest Kafka samples.",
    )
    parser.add_argument(
        "--kafka-oldest-sample-size",
        type=int,
        help="Override the number of oldest Kafka samples.",
    )
    parser.add_argument(
        "--kafka-poll-timeout-seconds",
        type=float,
        help="Kafka poll timeout in seconds. Default from config or 1.0.",
    )
    parser.add_argument(
        "--kafka-fetch-timeout-seconds",
        type=float,
        default=30.0,
        help="Give up reading Kafka samples after N seconds per direction. Default 30.",
    )
    parser.add_argument(
        "--kafka-metadata-timeout-seconds",
        type=float,
        default=30.0,
        help="Kafka metadata wait timeout in seconds. Default 30.",
    )
    parser.add_argument(
        "--kafka-value-max-len",
        type=int,
        default=200,
        help="Truncate each Kafka sample payload to N characters. Default 200.",
    )
    args = parser.parse_args(argv)

    settings = {
        "hosts": DEFAULT_HBASE_HOSTS,
        "port": 9090,
        "timeout_ms": 30000,
        "column_family": "cf",
        "table": DEFAULT_HBASE_TABLE,
    }
    kafka_settings: Dict[str, Any] = {
        "bootstrap_servers": DEFAULT_KAFKA_BOOTSTRAP_SERVERS,
        "topic": DEFAULT_KAFKA_TOPIC,
        "api_version": None,
        "poll_timeout_seconds": 1.0,
        "max_poll_records": 500,
    }
    if args.config:
        settings.update(load_hbase_settings_from_config(args.config))
        kafka_settings.update(load_kafka_settings_from_config(args.config))
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
    if args.kafka_bootstrap_servers:
        kafka_settings["bootstrap_servers"] = args.kafka_bootstrap_servers
    if args.kafka_topic:
        kafka_settings["topic"] = args.kafka_topic
    if args.kafka_api_version:
        kafka_settings["api_version"] = args.kafka_api_version
    if args.kafka_poll_timeout_seconds is not None:
        kafka_settings["poll_timeout_seconds"] = args.kafka_poll_timeout_seconds

    latest_sample_size = max(
        0,
        args.kafka_latest_sample_size
        if args.kafka_latest_sample_size is not None
        else args.kafka_sample_size,
    )
    oldest_sample_size = max(
        0,
        args.kafka_oldest_sample_size
        if args.kafka_oldest_sample_size is not None
        else args.kafka_sample_size,
    )

    kafka_summary: Dict[str, Any] = {"enabled": False}
    if not args.no_kafka_samples and (latest_sample_size > 0 or oldest_sample_size > 0):
        try:
            kafka_summary = collect_kafka_samples(
                bootstrap_servers=parse_csv(str(kafka_settings["bootstrap_servers"])),
                topic=str(kafka_settings["topic"]),
                api_version=parse_kafka_api_version(kafka_settings.get("api_version")),
                latest_sample_size=latest_sample_size,
                oldest_sample_size=oldest_sample_size,
                poll_timeout_ms=int(
                    float(kafka_settings["poll_timeout_seconds"]) * 1000
                ),
                max_poll_records=int(kafka_settings["max_poll_records"]),
                fetch_timeout_seconds=args.kafka_fetch_timeout_seconds,
                metadata_timeout_seconds=args.kafka_metadata_timeout_seconds,
                value_max_len=args.kafka_value_max_len,
            )
        except Exception as exc:
            # HBase stats stay useful even when the Kafka topic is unreachable.
            print("Could not read Kafka samples: %s" % exc, file=sys.stderr)
            kafka_summary = {
                "enabled": True,
                "topic": kafka_settings.get("topic"),
                "error": str(exc),
                "latest_samples": [],
                "oldest_samples": [],
            }

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

    summary["kafka"] = kafka_summary

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

    print_kafka_samples(kafka_summary)

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
        print("hbase_table_stats_with_kafka_samples failed: %s" % exc, file=sys.stderr)
        sys.exit(1)
