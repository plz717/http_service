import argparse
import base64
import binascii
import json
import logging
import signal
import sys
import time
from collections import Counter
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional


try:
    import yaml
except ImportError:  # pragma: no cover - runtime dependency guard
    yaml = None


LOG = logging.getLogger("kafka_type140_stats")


DEFAULT_CONFIG: Dict[str, Any] = {
    "kafka": {
        "auto_offset_reset": "earliest",
        "poll_timeout_seconds": 1.0,
        "max_poll_records": 500,
    },
    "message_filter": {
        "type": "140",
    },
    "logging": {
        "level": "INFO",
    },
    "vector": {
        "expected_dimension": 1024,
    },
}


def deep_merge(
    base: MutableMapping[str, Any], override: Mapping[str, Any]
) -> MutableMapping[str, Any]:
    for key, value in override.items():
        if isinstance(value, Mapping) and isinstance(base.get(key), MutableMapping):
            deep_merge(base[key], value)
        else:
            base[key] = value
    return base


def load_config(path: str) -> Dict[str, Any]:
    if yaml is None:
        raise RuntimeError(
            "PyYAML is required. Install dependencies from requirements.txt."
        )
    with open(path, "r", encoding="utf-8") as fp:
        user_config = yaml.safe_load(fp) or {}
    config = deepcopy(DEFAULT_CONFIG)
    deep_merge(config, user_config)
    validate_config(config)
    return config


def validate_config(config: Mapping[str, Any]) -> None:
    kafka = config["kafka"]
    required = [
        ("kafka.bootstrap_servers", kafka.get("bootstrap_servers")),
        ("kafka.topic", kafka.get("topic")),
    ]
    missing = [name for name, value in required if not value]
    if missing:
        raise ValueError("Missing required config: " + ", ".join(missing))


def configure_logging(config: Mapping[str, Any]) -> None:
    level_name = str(config.get("logging", {}).get("level", "INFO")).upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )


def parse_bootstrap_servers(value: Any) -> List[str]:
    if isinstance(value, str):
        return [server.strip() for server in value.split(",") if server.strip()]
    if isinstance(value, Iterable):
        return [str(server).strip() for server in value if str(server).strip()]
    raise ValueError("kafka.bootstrap_servers must be a string or list")


def parse_api_version(value: Any) -> Optional[tuple]:
    if value is None or str(value).strip() == "":
        return None
    if isinstance(value, (list, tuple)):
        return tuple(int(item) for item in value)
    return tuple(
        int(item) for item in str(value).strip().split(".") if item.strip() != ""
    )


def is_not_blank(value: Any) -> bool:
    return value is not None and str(value).strip() != ""


def normalize_json_strings(value: Any) -> Any:
    if isinstance(value, str):
        stripped = value.strip()
        if stripped and stripped[0] in "[{":
            try:
                return normalize_json_strings(json.loads(stripped))
            except ValueError:
                return value
        return value
    if isinstance(value, Mapping):
        return {key: normalize_json_strings(item) for key, item in value.items()}
    if isinstance(value, list):
        return [normalize_json_strings(item) for item in value]
    return value


def flatten_fields(value: Any, prefix: str = "") -> Dict[str, Any]:
    fields: Dict[str, Any] = {}
    if isinstance(value, Mapping):
        for key, item in value.items():
            path = "%s.%s" % (prefix, key) if prefix else str(key)
            fields.update(flatten_fields(item, path))
    elif isinstance(value, list):
        fields[prefix] = value
        if value and isinstance(value[0], Mapping):
            fields.update(flatten_fields(value[0], "%s[0]" % prefix))
    else:
        fields[prefix] = value
    return fields


def find_field(payload: Mapping[str, Any], field_name: str) -> Any:
    aliases = {
        "assetId": ["assetId", "asset_id", "assert_id"],
        "asset_id": ["asset_id", "assetId", "assert_id"],
        "assert_id": ["assert_id", "assetId", "asset_id"],
        "type": ["type"],
        "vector": ["vector", "embedding", "emb", "embed"],
    }
    candidate_names = aliases.get(field_name, [field_name])

    for name in candidate_names:
        if name in payload:
            return payload[name]

    flattened = flatten_fields(payload)
    for name in candidate_names:
        if name in flattened:
            return flattened[name]
        suffix = "." + name
        for path, value in flattened.items():
            if path.endswith(suffix):
                return value
    return None


def find_mapping_by_key(
    value: Any, candidate_names: Iterable[str]
) -> Optional[Mapping[str, Any]]:
    names = set(candidate_names)
    if isinstance(value, Mapping):
        for key, item in value.items():
            if str(key) in names and isinstance(item, Mapping):
                return item
        for item in value.values():
            found = find_mapping_by_key(item, names)
            if found is not None:
                return found
    elif isinstance(value, list):
        for item in value:
            found = find_mapping_by_key(item, names)
            if found is not None:
                return found
    return None


def find_list_by_key(value: Any, candidate_names: Iterable[str]) -> Optional[List[Any]]:
    names = set(candidate_names)
    if isinstance(value, Mapping):
        for key, item in value.items():
            if str(key) in names and isinstance(item, list):
                return item
        for item in value.values():
            found = find_list_by_key(item, names)
            if found is not None:
                return found
    elif isinstance(value, list):
        for item in value:
            found = find_list_by_key(item, names)
            if found is not None:
                return found
    return None


def find_outer_type(payload: Mapping[str, Any]) -> Any:
    if "type" in payload:
        return payload.get("type")
    return find_field(payload, "type")


def find_vector_item(payload: Mapping[str, Any]) -> Optional[Mapping[str, Any]]:
    return find_mapping_by_key(payload, ["vectorItem", "vector_item", "vectoritem"])


def find_vector_list(payload: Mapping[str, Any]) -> Optional[List[Any]]:
    return find_list_by_key(payload, ["vectorList", "vector_list", "vectorlist"])


def find_vector_item_field(
    payload: Mapping[str, Any],
    vector_item: Optional[Mapping[str, Any]],
    field_name: str,
) -> Any:
    if vector_item is not None and field_name in vector_item:
        return vector_item.get(field_name)
    if field_name == "type":
        return None
    return find_field(payload, field_name)


def validate_vector_string(value: Any, expected_dimension: int) -> Dict[str, Any]:
    if value is None:
        return {"status": "missing", "bytes": None, "dimension": None}
    if not isinstance(value, str):
        return {"status": "not_string", "bytes": None, "dimension": None}

    text = value.strip()
    if not text:
        return {"status": "blank", "bytes": 0, "dimension": 0}

    try:
        decoded = base64.b64decode(text, validate=True)
    except (TypeError, binascii.Error):
        return {"status": "base64_invalid", "bytes": None, "dimension": None}

    byte_length = len(decoded)
    dimension = byte_length // 4 if byte_length % 4 == 0 else None
    if byte_length != expected_dimension * 4:
        return {
            "status": "length_invalid",
            "bytes": byte_length,
            "dimension": dimension,
        }
    return {"status": "valid", "bytes": byte_length, "dimension": expected_dimension}


def parse_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(str(value).strip())
    except ValueError:
        return None


def increment_nested_counter(
    counters: MutableMapping[str, Counter], key: str, value: str
) -> None:
    if key not in counters:
        counters[key] = Counter()
    counters[key][value] += 1


def update_vector_stats(
    stats: MutableMapping[str, Any], payload: Mapping[str, Any], expected_dimension: int
) -> None:
    vector_item = find_vector_item(payload)
    if vector_item is None:
        stats["vector_item_missing"] += 1
    else:
        stats["vector_item_present"] += 1
        vector_item_type = vector_item.get("type")
        stats["vector_item_type_counter"][
            str(vector_item_type) if vector_item_type is not None else "<missing>"
        ] += 1

    vector_value = find_vector_item_field(payload, vector_item, "vector")
    result = validate_vector_string(vector_value, expected_dimension)
    stats["vector_status_counter"][result["status"]] += 1
    if result["bytes"] is not None:
        stats["vector_bytes_counter"][str(result["bytes"])] += 1
    if result["dimension"] is not None:
        stats["vector_dimension_counter"][str(result["dimension"])] += 1


def update_vector_list_stats(
    stats: MutableMapping[str, Any], payload: Mapping[str, Any], fallback_dimension: int
) -> None:
    vector_list = find_vector_list(payload)
    if vector_list is None:
        stats["vector_list_missing"] += 1
        return

    stats["vector_list_present"] += 1
    stats["vector_list_item_total"] += len(vector_list)
    asset_id = find_field(payload, "assetId")
    asset_id_text = str(asset_id) if is_not_blank(asset_id) else None
    seen_types = set()

    for item in vector_list:
        if not isinstance(item, Mapping):
            stats["vector_list_item_not_object"] += 1
            continue

        vector_type = item.get("type")
        vector_type_text = str(vector_type) if vector_type is not None else "<missing>"
        seen_types.add(vector_type_text)
        stats["vector_list_type_item_counter"][vector_type_text] += 1

        vector_dimension = parse_int(item.get("vectorDimension"))
        expected_dimension = vector_dimension or fallback_dimension
        if vector_dimension is None:
            increment_nested_counter(
                stats["vector_list_type_dimension_counter"],
                vector_type_text,
                "<missing>",
            )
        else:
            increment_nested_counter(
                stats["vector_list_type_dimension_counter"],
                vector_type_text,
                str(vector_dimension),
            )

        result = validate_vector_string(item.get("vector"), expected_dimension)
        increment_nested_counter(
            stats["vector_list_type_status_counter"],
            vector_type_text,
            str(result["status"]),
        )
        if result["bytes"] is not None:
            increment_nested_counter(
                stats["vector_list_type_bytes_counter"],
                vector_type_text,
                str(result["bytes"]),
            )
        if result["dimension"] is not None:
            increment_nested_counter(
                stats["vector_list_type_decoded_dimension_counter"],
                vector_type_text,
                str(result["dimension"]),
            )

        if asset_id_text is not None:
            stats["vector_list_type_asset_ids"].setdefault(vector_type_text, set()).add(
                asset_id_text
            )

    for vector_type_text in seen_types:
        stats["vector_list_type_message_counter"][vector_type_text] += 1


def decode_kafka_value(value: Any) -> Mapping[str, Any]:
    if isinstance(value, bytes):
        text = value.decode("utf-8")
    else:
        text = str(value)
    payload = json.loads(text)
    if isinstance(payload, str):
        payload = json.loads(payload)
    if not isinstance(payload, Mapping):
        raise ValueError("Kafka value must be a JSON object")
    return payload


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def append_sample(
    save_path: Path, payload: Mapping[str, Any], metadata: Mapping[str, Any]
) -> None:
    save_path.parent.mkdir(parents=True, exist_ok=True)
    with save_path.open("a", encoding="utf-8") as fp:
        fp.write("=" * 100)
        fp.write("\n")
        fp.write("received_time=%s\n" % now_text())
        for key, value in metadata.items():
            fp.write("%s=%s\n" % (key, value))
        fp.write(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
        fp.write("\n")


def print_progress(stats: Mapping[str, Any], prefix: str = "Progress") -> None:
    matched = int(stats["matched_messages"])
    unique_asset_ids = len(stats["asset_id_counter"])
    duplicate_messages = max(
        0, matched - unique_asset_ids - int(stats["missing_asset_id"])
    )
    LOG.info(
        "%s: scanned=%s matched=%s unique_asset_id=%s duplicate_messages=%s "
        "missing_asset_id=%s vector_type1_items=%s vector_type1_valid=%s parse_errors=%s",
        prefix,
        stats["scanned_messages"],
        matched,
        unique_asset_ids,
        duplicate_messages,
        stats["missing_asset_id"],
        stats["vector_list_type_item_counter"].get("1", 0),
        stats["vector_list_type_status_counter"].get("1", Counter()).get("valid", 0),
        stats["parse_errors"],
    )


def print_summary(stats: Mapping[str, Any], top_n: int) -> None:
    asset_id_counter = stats["asset_id_counter"]
    matched = int(stats["matched_messages"])
    unique_asset_ids = len(asset_id_counter)
    duplicate_messages = max(
        0, matched - unique_asset_ids - int(stats["missing_asset_id"])
    )
    duplicated_asset_ids = sum(1 for _, count in asset_id_counter.items() if count > 1)

    print("=== Kafka type=140 stats summary ===")
    print("scanned_messages=%s" % stats["scanned_messages"])
    print("matched_type_140_messages=%s" % matched)
    print("unique_asset_id=%s" % unique_asset_ids)
    print("duplicate_type_140_messages=%s" % duplicate_messages)
    print("duplicated_asset_id_count=%s" % duplicated_asset_ids)
    print("missing_asset_id=%s" % stats["missing_asset_id"])
    print("parse_errors=%s" % stats["parse_errors"])
    print("vector_item_present=%s" % stats["vector_item_present"])
    print("vector_item_missing=%s" % stats["vector_item_missing"])
    print("vector_list_present=%s" % stats["vector_list_present"])
    print("vector_list_missing=%s" % stats["vector_list_missing"])
    print("vector_list_item_total=%s" % stats["vector_list_item_total"])
    print("vector_list_item_not_object=%s" % stats["vector_list_item_not_object"])
    print("sample_saved=%s" % stats.get("sample_saved", 0))

    print("=== type counts ===")
    for message_type, count in stats["type_counter"].most_common():
        print("%s=%s" % (message_type, count))

    if asset_id_counter:
        print("=== top duplicated assetId ===")
        printed = 0
        for asset_id, count in asset_id_counter.most_common():
            if count <= 1:
                break
            print("%s=%s" % (asset_id, count))
            printed += 1
            if printed >= top_n:
                break
        if printed == 0:
            print("No duplicated assetId found.")

    print("=== vectorItem.type counts ===")
    if stats["vector_item_type_counter"]:
        for vector_type, count in stats["vector_item_type_counter"].most_common():
            print("%s=%s" % (vector_type, count))
    else:
        print("No vectorItem.type found.")

    print("=== vector status counts ===")
    for status, count in stats["vector_status_counter"].most_common():
        print("%s=%s" % (status, count))

    print("=== vector decoded byte length counts ===")
    for byte_length, count in stats["vector_bytes_counter"].most_common():
        print("%s=%s" % (byte_length, count))

    print("=== vector decoded dimension counts ===")
    for dimension, count in stats["vector_dimension_counter"].most_common():
        print("%s=%s" % (dimension, count))

    print("=== vectorList.type item counts ===")
    if stats["vector_list_type_item_counter"]:
        for vector_type, count in stats["vector_list_type_item_counter"].most_common():
            print("%s=%s" % (vector_type, count))
    else:
        print("No vectorList type found.")

    print("=== vectorList.type message counts ===")
    for vector_type, count in stats["vector_list_type_message_counter"].most_common():
        print("%s=%s" % (vector_type, count))

    print("=== vectorList.type unique assetId counts ===")
    for vector_type, asset_ids in sorted(stats["vector_list_type_asset_ids"].items()):
        print("%s=%s" % (vector_type, len(asset_ids)))

    print("=== vectorList.type vectorDimension counts ===")
    for vector_type, counter in sorted(
        stats["vector_list_type_dimension_counter"].items()
    ):
        for dimension, count in counter.most_common():
            print("type=%s dimension=%s count=%s" % (vector_type, dimension, count))

    print("=== vectorList.type vector status counts ===")
    for vector_type, counter in sorted(
        stats["vector_list_type_status_counter"].items()
    ):
        for status, count in counter.most_common():
            print("type=%s status=%s count=%s" % (vector_type, status, count))

    print("=== vectorList.type decoded byte length counts ===")
    for vector_type, counter in sorted(stats["vector_list_type_bytes_counter"].items()):
        for byte_length, count in counter.most_common():
            print("type=%s bytes=%s count=%s" % (vector_type, byte_length, count))

    print("=== vectorList.type decoded dimension counts ===")
    for vector_type, counter in sorted(
        stats["vector_list_type_decoded_dimension_counter"].items()
    ):
        for dimension, count in counter.most_common():
            print("type=%s dimension=%s count=%s" % (vector_type, dimension, count))

    if stats["missing_asset_id_examples"]:
        print("=== missing assetId examples ===")
        for example in stats["missing_asset_id_examples"][:top_n]:
            print(json.dumps(example, ensure_ascii=False, separators=(",", ":")))


def wait_topic_partitions(
    consumer: Any, topic: str, timeout_seconds: float
) -> List[Any]:
    from kafka import TopicPartition

    deadline = time.time() + timeout_seconds
    partitions = None
    while time.time() < deadline:
        partitions = consumer.partitions_for_topic(topic)
        if partitions:
            return [
                TopicPartition(topic, partition) for partition in sorted(partitions)
            ]
        time.sleep(0.5)
    raise RuntimeError("Could not load Kafka partitions for topic %s" % topic)


def scan_topic(config: Mapping[str, Any], args: argparse.Namespace) -> None:
    from kafka import KafkaConsumer

    kafka_config = config["kafka"]
    topic = kafka_config["topic"]
    consumer_kwargs: Dict[str, Any] = {
        "bootstrap_servers": parse_bootstrap_servers(kafka_config["bootstrap_servers"]),
        "enable_auto_commit": False,
        "auto_offset_reset": kafka_config.get("auto_offset_reset", "earliest"),
        "max_poll_records": int(kafka_config.get("max_poll_records", 500)),
    }
    api_version = parse_api_version(kafka_config.get("api_version"))
    if api_version is not None:
        consumer_kwargs["api_version"] = api_version

    expected_type = (
        args.filter_type or config.get("message_filter", {}).get("type") or "140"
    )
    expected_dimension = int(
        args.expected_vector_dimension
        or config.get("vector", {}).get("expected_dimension", 1024)
    )
    poll_timeout_ms = int(float(kafka_config.get("poll_timeout_seconds", 1.0)) * 1000)
    stats: Dict[str, Any] = {
        "scanned_messages": 0,
        "matched_messages": 0,
        "missing_asset_id": 0,
        "parse_errors": 0,
        "type_counter": Counter(),
        "asset_id_counter": Counter(),
        "missing_asset_id_examples": [],
        "vector_item_present": 0,
        "vector_item_missing": 0,
        "vector_item_type_counter": Counter(),
        "vector_status_counter": Counter(),
        "vector_bytes_counter": Counter(),
        "vector_dimension_counter": Counter(),
        "vector_list_present": 0,
        "vector_list_missing": 0,
        "vector_list_item_total": 0,
        "vector_list_item_not_object": 0,
        "vector_list_type_item_counter": Counter(),
        "vector_list_type_message_counter": Counter(),
        "vector_list_type_asset_ids": {},
        "vector_list_type_dimension_counter": {},
        "vector_list_type_status_counter": {},
        "vector_list_type_bytes_counter": {},
        "vector_list_type_decoded_dimension_counter": {},
        "sample_saved": 0,
    }
    stop = {"requested": False}

    def request_stop(signum, frame):
        stop["requested"] = True
        LOG.info("Stop requested by signal %s", signum)

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    consumer = KafkaConsumer(**consumer_kwargs)
    try:
        partitions = wait_topic_partitions(
            consumer, topic, args.metadata_timeout_seconds
        )
        consumer.assign(partitions)
        beginning_offsets = consumer.beginning_offsets(partitions)
        end_offsets = consumer.end_offsets(partitions)
        for partition in partitions:
            consumer.seek(partition, beginning_offsets[partition])

        total_retained = sum(
            end_offsets[tp] - beginning_offsets[tp] for tp in partitions
        )
        LOG.info(
            "Scanning topic=%s partitions=%s retained_messages_at_start=%s",
            topic,
            len(partitions),
            total_retained,
        )
        LOG.info(
            "Expected type=%s; expected vector dimension=%s; no consumer group offset will be committed",
            expected_type,
            expected_dimension,
        )

        last_progress_time = time.time()
        while not stop["requested"]:
            if args.max_scan > 0 and stats["scanned_messages"] >= args.max_scan:
                LOG.info("Reached --max-scan=%s", args.max_scan)
                break
            if args.max_matched > 0 and stats["matched_messages"] >= args.max_matched:
                LOG.info("Reached --max-matched=%s", args.max_matched)
                break

            all_reached_end = True
            for partition in partitions:
                if consumer.position(partition) < end_offsets[partition]:
                    all_reached_end = False
                    break
            if all_reached_end:
                LOG.info("Reached topic end offsets captured at startup")
                break

            records_by_partition = consumer.poll(
                timeout_ms=poll_timeout_ms, max_records=args.poll_records
            )
            if not records_by_partition:
                now = time.time()
                if now - last_progress_time >= args.progress_seconds:
                    print_progress(stats)
                    last_progress_time = now
                continue

            for records in records_by_partition.values():
                for message in records:
                    if args.max_scan > 0 and stats["scanned_messages"] >= args.max_scan:
                        break
                    stats["scanned_messages"] += 1
                    try:
                        payload = normalize_json_strings(
                            decode_kafka_value(message.value)
                        )
                    except Exception as exc:
                        stats["parse_errors"] += 1
                        LOG.warning(
                            "Could not parse Kafka message partition=%s offset=%s: %s",
                            message.partition,
                            message.offset,
                            exc,
                        )
                        continue

                    message_type = find_outer_type(payload)
                    message_type_text = (
                        str(message_type) if message_type is not None else "<missing>"
                    )
                    stats["type_counter"][message_type_text] += 1
                    if message_type_text != str(expected_type):
                        continue

                    stats["matched_messages"] += 1
                    if args.save_samples and stats["sample_saved"] < args.sample_count:
                        append_sample(
                            Path(args.save_samples),
                            payload,
                            {
                                "topic": message.topic,
                                "partition": message.partition,
                                "offset": message.offset,
                                "kafka_timestamp": message.timestamp,
                            },
                        )
                        stats["sample_saved"] += 1
                    update_vector_stats(stats, payload, expected_dimension)
                    update_vector_list_stats(stats, payload, expected_dimension)
                    asset_id = find_field(payload, "assetId")
                    if not is_not_blank(asset_id):
                        stats["missing_asset_id"] += 1
                        if len(stats["missing_asset_id_examples"]) < args.top_n:
                            stats["missing_asset_id_examples"].append(payload)
                        continue
                    stats["asset_id_counter"][str(asset_id)] += 1

                if args.max_scan > 0 and stats["scanned_messages"] >= args.max_scan:
                    break

            now = time.time()
            if (
                args.progress_records > 0
                and stats["scanned_messages"] > 0
                and stats["scanned_messages"] % args.progress_records == 0
            ) or now - last_progress_time >= args.progress_seconds:
                print_progress(stats)
                last_progress_time = now

    finally:
        consumer.close()

    print_progress(stats, prefix="Final progress")
    print_summary(stats, args.top_n)


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Count Kafka type=140 messages and unique assetId values."
    )
    parser.add_argument(
        "-c", "--config", required=True, help="Path to YAML config file."
    )
    parser.add_argument(
        "--filter-type",
        default=None,
        help="Message type to count. Default comes from config, usually 140.",
    )
    parser.add_argument(
        "--max-scan",
        type=int,
        default=0,
        help="Stop after scanning N Kafka messages. Default 0 means no limit.",
    )
    parser.add_argument(
        "--max-matched",
        type=int,
        default=0,
        help="Stop after matching N type messages. Default 0 means no limit.",
    )
    parser.add_argument(
        "--poll-records", type=int, default=500, help="Max records per poll."
    )
    parser.add_argument(
        "--progress-records",
        type=int,
        default=10000,
        help="Print progress every N scanned messages.",
    )
    parser.add_argument(
        "--progress-seconds",
        type=float,
        default=30.0,
        help="Print progress every N seconds.",
    )
    parser.add_argument(
        "--metadata-timeout-seconds",
        type=float,
        default=30.0,
        help="Kafka metadata wait timeout.",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=20,
        help="Top duplicated assetId and missing examples to print.",
    )
    parser.add_argument(
        "--expected-vector-dimension",
        type=int,
        default=1024,
        help="Expected float32 vector dimension. Default 1024 means decoded vector should be 4096 bytes.",
    )
    parser.add_argument(
        "--save-samples", help="Save matched Kafka payload samples to this txt file."
    )
    parser.add_argument(
        "--sample-count",
        type=int,
        default=5,
        help="Max matched payload samples to save.",
    )
    args = parser.parse_args(argv)

    try:
        config = load_config(args.config)
        configure_logging(config)
        scan_topic(config, args)
    except KeyboardInterrupt:
        LOG.info("Stopped")
    except Exception as exc:
        LOG.error("kafka_type140_stats failed: %s", exc, exc_info=True)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
