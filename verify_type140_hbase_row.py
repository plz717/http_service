import argparse
import json
import logging
import signal
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple


from kafka_to_hbase import (
    HBaseWriter,
    as_text,
    build_poms_type140_document,
    configure_logging,
    decode_kafka_value,
    filter_document_fields,
    find_field,
    is_not_blank,
    load_config,
    message_matches_filter,
    normalize_json_strings,
    parse_bootstrap_servers,
    parse_kafka_api_version,
)


LOG = logging.getLogger("verify_type140_hbase_row")


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


def payload_asset_id(payload: Mapping[str, Any]) -> Any:
    data = payload.get("data")
    if isinstance(data, Mapping) and is_not_blank(data.get("assetId")):
        return data.get("assetId")
    return find_field(payload, "assetId")


def selected_fields_from_config(config: Mapping[str, Any]) -> List[str]:
    one_shot_config = config.get("one_shot", {})
    fields = one_shot_config.get("fields") or []
    return [str(field) for field in fields if is_not_blank(field)]


def table_from_config(config: Mapping[str, Any]) -> str:
    one_shot_config = config.get("one_shot", {})
    table = one_shot_config.get("write_table") or config.get("tables", {}).get(
        "poms_type140_embedding"
    )
    if not is_not_blank(table):
        raise ValueError(
            "Missing one_shot.write_table or tables.poms_type140_embedding"
        )
    return str(table)


def compare_values(expected: Any, actual: Any) -> bool:
    if expected is None and actual is None:
        return True
    return str(expected) == str(actual)


def short_value(value: Any, max_len: int = 120) -> str:
    if value is None:
        return "<missing>"
    text = str(value)
    if len(text) <= max_len:
        return text
    return "%s...<len=%s>" % (text[:max_len], len(text))


def compare_documents(
    mapped: Mapping[str, Any], hbase_row: Mapping[str, Any], fields: Iterable[str]
) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    for field in fields:
        expected = mapped.get(field)
        actual = hbase_row.get(field)
        results.append(
            {
                "field": field,
                "match": compare_values(expected, actual),
                "kafka_value": expected,
                "hbase_value": actual,
                "kafka_len": len(str(expected)) if expected is not None else None,
                "hbase_len": len(str(actual)) if actual is not None else None,
            }
        )
    return results


def fetch_hbase_row(
    config: Mapping[str, Any], table_name: str, row_key: str, columns: Iterable[str]
) -> Dict[str, str]:
    writer = HBaseWriter(config)
    try:
        return writer.get_columns(table_name, row_key, columns)
    finally:
        writer.close()


def scan_kafka_for_asset_id(
    config: Mapping[str, Any],
    asset_id: str,
    expected_type: Optional[str],
    max_scan: int,
    progress_records: int,
    metadata_timeout_seconds: float,
) -> Tuple[Any, Mapping[str, Any], str]:
    from kafka import KafkaConsumer

    kafka_config = config["kafka"]
    topic = kafka_config["topic"]
    consumer_options: Dict[str, Any] = {
        "bootstrap_servers": parse_bootstrap_servers(kafka_config["bootstrap_servers"]),
        "enable_auto_commit": False,
        "auto_offset_reset": "earliest",
        "max_poll_records": int(kafka_config.get("max_poll_records", 500)),
    }
    api_version = parse_kafka_api_version(kafka_config.get("api_version"))
    if api_version is not None:
        consumer_options["api_version"] = api_version

    stop = {"requested": False}

    def request_stop(signum, frame):
        stop["requested"] = True
        LOG.info("Stop requested by signal %s", signum)

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    consumer = KafkaConsumer(**consumer_options)
    try:
        partitions = wait_topic_partitions(consumer, topic, metadata_timeout_seconds)
        consumer.assign(partitions)
        beginning_offsets = consumer.beginning_offsets(partitions)
        end_offsets = consumer.end_offsets(partitions)
        for partition in partitions:
            consumer.seek(partition, beginning_offsets[partition])

        total_retained = sum(
            end_offsets[tp] - beginning_offsets[tp] for tp in partitions
        )
        LOG.info(
            "Scanning topic=%s partitions=%s retained_messages_at_start=%s target_assetId=%s expected_type=%s",
            topic,
            len(partitions),
            total_retained,
            asset_id,
            expected_type,
        )

        scanned = 0
        poll_timeout_ms = int(
            float(kafka_config.get("poll_timeout_seconds", 1.0)) * 1000
        )
        while not stop["requested"]:
            if max_scan > 0 and scanned >= max_scan:
                break

            all_reached_end = True
            for partition in partitions:
                if consumer.position(partition) < end_offsets[partition]:
                    all_reached_end = False
                    break
            if all_reached_end:
                break

            records_by_partition = consumer.poll(
                timeout_ms=poll_timeout_ms,
                max_records=int(kafka_config.get("max_poll_records", 500)),
            )
            if not records_by_partition:
                continue

            for records in records_by_partition.values():
                for message in records:
                    if max_scan > 0 and scanned >= max_scan:
                        break
                    scanned += 1
                    payload = normalize_json_strings(decode_kafka_value(message.value))
                    if not message_matches_filter(payload, expected_type):
                        continue
                    actual_asset_id = payload_asset_id(payload)
                    if as_text(actual_asset_id) != asset_id:
                        continue

                    raw_text = message.value.decode("utf-8", errors="replace")
                    LOG.info(
                        "Found target assetId=%s partition=%s offset=%s scanned=%s",
                        asset_id,
                        message.partition,
                        message.offset,
                        scanned,
                    )
                    return message, payload, raw_text

                if (
                    progress_records > 0
                    and scanned > 0
                    and scanned % progress_records == 0
                ):
                    LOG.info("Progress: scanned=%s", scanned)
    finally:
        consumer.close()

    raise RuntimeError(
        "Could not find Kafka message for assetId=%s after scanning %s messages"
        % (asset_id, scanned)
    )


def write_report(
    output_path: Path,
    message: Any,
    raw_text: str,
    payload: Mapping[str, Any],
    mapped_document: Mapping[str, Any],
    hbase_row: Mapping[str, Any],
    comparisons: List[Mapping[str, Any]],
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as fp:
        fp.write("=== Kafka metadata ===\n")
        fp.write("topic=%s\n" % message.topic)
        fp.write("partition=%s\n" % message.partition)
        fp.write("offset=%s\n" % message.offset)
        fp.write("kafka_timestamp=%s\n" % message.timestamp)
        fp.write("\n=== Kafka raw value ===\n")
        fp.write(raw_text)
        fp.write("\n\n=== Kafka normalized JSON ===\n")
        json.dump(payload, fp, ensure_ascii=False, indent=2)
        fp.write("\n\n=== Mapped HBase document from Kafka ===\n")
        json.dump(mapped_document, fp, ensure_ascii=False, indent=2)
        fp.write("\n\n=== Current HBase row decoded as UTF-8 ===\n")
        json.dump(hbase_row, fp, ensure_ascii=False, indent=2)
        fp.write("\n\n=== Field comparison ===\n")
        json.dump(comparisons, fp, ensure_ascii=False, indent=2)
        fp.write("\n")


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Find one Kafka type=140 assetId and compare it with the HBase row."
    )
    parser.add_argument(
        "-c", "--config", required=True, help="Path to YAML config file."
    )
    parser.add_argument(
        "--asset-id", required=True, help="Target assetId / HBase row key."
    )
    parser.add_argument("--table", help="HBase table name. Default comes from config.")
    parser.add_argument(
        "--filter-type", help="Outer Kafka message type. Default comes from config."
    )
    parser.add_argument(
        "--vector-list-type",
        help="data.vectorList item type. Default comes from config, usually 1.",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Path to write the raw Kafka and comparison report txt.",
    )
    parser.add_argument(
        "--max-scan",
        type=int,
        default=0,
        help="Stop after scanning N messages. Default 0 means all retained.",
    )
    parser.add_argument(
        "--progress-records",
        type=int,
        default=50000,
        help="Print progress every N scanned messages.",
    )
    parser.add_argument(
        "--metadata-timeout-seconds",
        type=float,
        default=30.0,
        help="Kafka metadata wait timeout.",
    )
    args = parser.parse_args(argv)

    try:
        config = load_config(args.config)
        configure_logging(config)
        expected_type = args.filter_type or config.get("message_filter", {}).get("type")
        vector_list_type = args.vector_list_type or config.get("one_shot", {}).get(
            "vector_list_type", "1"
        )
        table_name = args.table or table_from_config(config)
        selected_fields = selected_fields_from_config(config)

        message, payload, raw_text = scan_kafka_for_asset_id(
            config=config,
            asset_id=str(args.asset_id),
            expected_type=expected_type,
            max_scan=args.max_scan,
            progress_records=args.progress_records,
            metadata_timeout_seconds=args.metadata_timeout_seconds,
        )
        row_key, document = build_poms_type140_document(payload, vector_list_type)
        mapped_document = filter_document_fields(document, selected_fields)
        for required_field in ("assetId", "version"):
            if required_field in document:
                mapped_document[required_field] = document[required_field]

        hbase_columns = sorted(set(mapped_document.keys()) | {"operate_time"})
        hbase_row = fetch_hbase_row(config, table_name, row_key, hbase_columns)
        comparisons = compare_documents(
            mapped_document, hbase_row, sorted(mapped_document.keys())
        )
        write_report(
            Path(args.output),
            message,
            raw_text,
            payload,
            mapped_document,
            hbase_row,
            comparisons,
        )

        matched = sum(1 for item in comparisons if item["match"])
        mismatched = [item for item in comparisons if not item["match"]]
        print("asset_id=%s" % args.asset_id)
        print("kafka_partition=%s" % message.partition)
        print("kafka_offset=%s" % message.offset)
        print("hbase_table=%s" % table_name)
        print("hbase_row_key=%s" % row_key)
        print("compare_matched_fields=%s" % matched)
        print("compare_mismatched_fields=%s" % len(mismatched))
        for item in mismatched:
            print(
                "DIFF field=%s kafka=%s hbase=%s"
                % (
                    item["field"],
                    short_value(item["kafka_value"]),
                    short_value(item["hbase_value"]),
                )
            )
        print("Wrote report to %s" % args.output)
    except Exception as exc:
        LOG.error("verify_type140_hbase_row failed: %s", exc, exc_info=True)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
