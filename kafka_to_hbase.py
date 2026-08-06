import argparse
import base64
import binascii
import json
import logging
import signal
import struct
import sys
import time
from copy import deepcopy
from datetime import datetime
from itertools import cycle
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Tuple


try:
    import yaml
except ImportError:  # pragma: no cover - runtime dependency guard
    yaml = None


LOG = logging.getLogger("kafka_to_hbase")


DEFAULT_CONFIG: Dict[str, Any] = {
    "kafka": {
        "api_version": None,
        "auto_offset_reset": "latest",
        "enable_auto_commit": False,
        "idle_log_interval_seconds": 30.0,
        "log_each_message": True,
        "log_skipped_messages": True,
        "poll_timeout_seconds": 1.0,
        "progress_log_interval_records": 10000,
        "progress_log_interval_seconds": 60.0,
        "stop_after_idle_seconds": 0.0,
        "max_poll_records": 500,
    },
    "operations": {
        "release": ["release", "released", "publish", "online", "up"],
        "withdraw": ["withdraw", "offline", "down", "delete", "deleted"],
    },
    "message_filter": {
        "type": None,
    },
    "hbase": {
        "thrift_port": 9090,
        "timeout_ms": 30000,
        "column_family": "cf",
        "batch_size": 100,
    },
    "tables": {
        "use_test_when_table_name_present": True,
        "description_prod": "recommend:video_list_description_base",
        "description_test": "recommend:video_list_description_base_test",
        "content_prod": "recommend:video_list_content_base",
        "content_test": "recommend:video_list_content_base_test",
    },
    "content_index": {
        "enabled": False,
        "table": "",
        "row_prefix": "__chart_index__",
        "column": "content_row_keys",
    },
    "one_shot": {
        "mode": "poms_type140",
        "write_table": None,
        "row_key_field": "assetId",
        "vector_list_type": "1",
        "fields": [
            "id",
            "assetId",
            "version",
            "time",
            "modelName",
            "source",
            "summary",
            "type",
            "vector",
            "vectorDimension",
            "vectorVersion",
        ],
    },
    "logging": {
        "level": "INFO",
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
    hbase = config["hbase"]
    required = [
        ("kafka.bootstrap_servers", kafka.get("bootstrap_servers")),
        ("kafka.topic", kafka.get("topic")),
        ("kafka.group_id", kafka.get("group_id")),
        ("hbase.thrift_hosts", hbase.get("thrift_hosts")),
    ]
    missing = [name for name, value in required if not value]
    if missing:
        raise ValueError("Missing required config: " + ", ".join(missing))


def parse_bootstrap_servers(value: Any) -> List[str]:
    if isinstance(value, str):
        return [server.strip() for server in value.split(",") if server.strip()]
    if isinstance(value, Iterable):
        return [str(server).strip() for server in value if str(server).strip()]
    raise ValueError("kafka.bootstrap_servers must be a string or list")


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
        raise ValueError("kafka.api_version must look like 2.0.0")


def configure_logging(config: Mapping[str, Any]) -> None:
    level_name = str(config.get("logging", {}).get("level", "INFO")).upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )


def is_not_blank(value: Any) -> bool:
    return value is not None and str(value).strip() != ""


def as_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return str(value)


def normalize_operation(operation: Any) -> str:
    return str(operation or "").strip().lower()


def normalized_set(values: Iterable[Any]) -> set:
    return {normalize_operation(value) for value in values if is_not_blank(value)}


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def parse_message_datetime(value: Any) -> Optional[datetime]:
    if not is_not_blank(value):
        return None
    text = str(value).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(text[:19], fmt)
        except ValueError:
            pass
    LOG.warning("Could not parse message time: %s", text)
    return None


def parse_message_time(value: Any) -> Optional[str]:
    parsed = parse_message_datetime(value)
    if parsed is None:
        return None
    return parsed.strftime("%Y-%m-%d %H:%M:%S")


def to_bytes(value: Any) -> bytes:
    if isinstance(value, bytes):
        return value
    return str(value).encode("utf-8")


def parse_csv(value: Optional[str]) -> List[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


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


def normalize_json_strings(value: Any) -> Any:
    if isinstance(value, str):
        stripped = value.strip()
        if stripped and stripped[0] in "[{":
            try:
                return normalize_json_strings(json.loads(stripped))
            except json.JSONDecodeError:
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
            path = f"{prefix}.{key}" if prefix else str(key)
            fields.update(flatten_fields(item, path))
    elif isinstance(value, list):
        fields[prefix] = value
        if value and isinstance(value[0], Mapping):
            fields.update(flatten_fields(value[0], f"{prefix}[0]"))
    else:
        fields[prefix] = value
    return fields


def find_field(payload: Mapping[str, Any], field_name: str) -> Any:
    aliases = {
        "assert_id": ["assert_id", "asset_id", "assetId"],
        "asset_id": ["asset_id", "assert_id", "assetId"],
        "assetId": ["assetId", "asset_id", "assert_id"],
        "embedding": ["embedding", "vector", "emb", "embed"],
    }
    candidate_names = aliases.get(field_name, [field_name])

    for name in candidate_names:
        if name in payload:
            return payload[name]

    flattened = flatten_fields(payload)
    for name in candidate_names:
        if name in flattened:
            return flattened[name]
        suffix = f".{name}"
        for path, value in flattened.items():
            if path.endswith(suffix):
                return value
    return None


def log_payload_fields(payload: Mapping[str, Any]) -> None:
    flattened = flatten_fields(payload)
    if not flattened:
        LOG.info("Message has no parseable fields")
        return
    LOG.info("Parsed %s fields:", len(flattened))
    for path, value in sorted(flattened.items()):
        if isinstance(value, list):
            value_type = f"list[{len(value)}]"
        else:
            value_type = type(value).__name__
        LOG.info("  %s (%s)", path, value_type)


def message_matches_filter(
    payload: Mapping[str, Any], expected_type: Optional[str]
) -> bool:
    if not is_not_blank(expected_type):
        return True
    actual_type = find_field(payload, "type")
    return as_text(actual_type) == str(expected_type)


def write_selected_fields_to_hbase(
    writer: "HBaseWriter",
    table_name: str,
    payload: Mapping[str, Any],
    fields: Iterable[str],
    row_key_field: str,
) -> str:
    document: Dict[str, Any] = {}
    for field in fields:
        value = find_field(payload, field)
        if value is not None:
            document[field] = value

    row_key = find_field(payload, row_key_field)
    if row_key is None and row_key_field == "assert_id":
        row_key = find_field(payload, "asset_id")
    if row_key is None:
        raise ValueError(
            f"Cannot write HBase row: missing row key field {row_key_field}"
        )
    if not document:
        raise ValueError("Cannot write HBase row: selected fields were not found")

    document["operate_time"] = now_text()
    writer.put(table_name, str(row_key), document)
    return str(row_key)


def parse_version_number(value: Any) -> Optional[int]:
    if not is_not_blank(value):
        return None
    text = str(value).strip()
    try:
        return int(text)
    except ValueError:
        LOG.warning("Could not parse version as numeric timestamp: %s", text)
        return None


def is_incoming_poms_row_older(
    incoming_version: Optional[int],
    existing_version: Optional[int],
    incoming_time: Optional[datetime],
    existing_time: Optional[datetime],
) -> Tuple[bool, str]:
    if incoming_version is not None and existing_version is not None:
        if incoming_version < existing_version:
            return True, "version"
        if incoming_version > existing_version:
            return False, ""
        if (
            incoming_time is not None
            and existing_time is not None
            and incoming_time < existing_time
        ):
            return True, "time"
        return False, ""

    if (
        incoming_time is not None
        and existing_time is not None
        and incoming_time < existing_time
    ):
        return True, "time"
    return False, ""


def first_mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    if isinstance(value, list):
        for item in value:
            if isinstance(item, Mapping):
                return item
    return {}


def select_vector_list_item(value: Any, expected_type: Any) -> Mapping[str, Any]:
    if not is_not_blank(expected_type):
        return first_mapping(value)

    expected_type_text = str(expected_type)
    if isinstance(value, Mapping):
        return value if as_text(value.get("type")) == expected_type_text else {}
    if isinstance(value, list):
        for item in value:
            if (
                isinstance(item, Mapping)
                and as_text(item.get("type")) == expected_type_text
            ):
                return item
    return {}


def put_if_present(document: Dict[str, Any], key: str, value: Any) -> None:
    if is_not_blank(value):
        document[key] = value


def parse_positive_int(value: Any, default: int) -> int:
    if not is_not_blank(value):
        return default
    try:
        parsed = int(str(value).strip())
    except ValueError:
        LOG.warning(
            "Could not parse positive integer value: %s, use default=%s", value, default
        )
        return default
    return parsed if parsed > 0 else default


def validate_vector_base64(
    vector_text: Any, vector_dimension: Any = None
) -> Tuple[bool, str]:
    if not is_not_blank(vector_text):
        return False, "vector is blank"

    expected_dimension = parse_positive_int(vector_dimension, 1024)
    expected_bytes = struct.calcsize("<%sf" % expected_dimension)
    try:
        decoded = base64.b64decode(str(vector_text).strip(), validate=True)
    except (TypeError, binascii.Error) as exc:
        return False, "vector is not valid base64: %s" % exc

    actual_bytes = len(decoded)
    if actual_bytes != expected_bytes:
        return (
            False,
            "decoded vector byte length is %s, expected %s for dimension %s"
            % (actual_bytes, expected_bytes, expected_dimension),
        )
    return True, "decoded vector dimension=%s bytes=%s" % (
        expected_dimension,
        actual_bytes,
    )


def build_poms_type140_document(
    payload: Mapping[str, Any], vector_list_type: Any = "1"
) -> Tuple[str, Dict[str, Any]]:
    data = payload.get("data") or {}
    if not isinstance(data, Mapping):
        data = {}

    vector_item = select_vector_list_item(data.get("vectorList"), vector_list_type)
    asset_id = data.get("assetId") or find_field(payload, "assetId")
    if not is_not_blank(asset_id):
        raise ValueError("Cannot write POMS type=140 row: missing data.assetId")
    if not vector_item:
        raise ValueError(
            "Cannot write POMS type=140 row: missing data.vectorList item with type=%s for assetId=%s"
            % (vector_list_type, asset_id)
        )

    vector_text = vector_item.get("vector")
    vector_valid, vector_reason = validate_vector_base64(
        vector_text, vector_item.get("vectorDimension")
    )
    if not vector_valid:
        raise ValueError(
            "Cannot write POMS type=140 row: invalid vector for assetId=%s: %s"
            % (asset_id, vector_reason)
        )
    LOG.debug(
        "Validated POMS type=140 vector for assetId=%s vectorList.type=%s: %s",
        asset_id,
        vector_item.get("type"),
        vector_reason,
    )

    document: Dict[str, Any] = {}
    put_if_present(document, "id", payload.get("id"))
    put_if_present(document, "operation", payload.get("operation"))
    put_if_present(document, "assetId", asset_id)
    put_if_present(document, "version", payload.get("version"))
    put_if_present(document, "time", payload.get("time"))
    put_if_present(
        document,
        "modelName",
        vector_item.get("modelName") or find_field(payload, "modelName"),
    )
    put_if_present(document, "source", vector_item.get("source"))
    put_if_present(
        document,
        "summary",
        vector_item.get("summary") or find_field(payload, "summary"),
    )
    put_if_present(document, "type", vector_item.get("type"))
    put_if_present(document, "vector", vector_text)
    put_if_present(document, "vectorDimension", vector_item.get("vectorDimension"))
    put_if_present(document, "vectorVersion", vector_item.get("version"))
    put_if_present(document, "modelVersion", vector_item.get("version"))
    return str(asset_id), document


def filter_document_fields(
    document: Mapping[str, Any], fields: Iterable[str]
) -> Dict[str, Any]:
    selected = {str(field) for field in fields if is_not_blank(field)}
    if not selected:
        return dict(document)
    return {key: value for key, value in document.items() if key in selected}


def write_poms_type140_to_hbase(
    writer: "HBaseWriter",
    table_name: str,
    payload: Mapping[str, Any],
    fields: Iterable[str],
    vector_list_type: Any = "1",
) -> Tuple[str, str]:
    try:
        row_key, document = build_poms_type140_document(payload, vector_list_type)
    except ValueError as exc:
        LOG.warning("Skip POMS type=140 row: %s", exc)
        return "<invalid>", "skipped_invalid_payload"

    incoming_version = parse_version_number(document.get("version"))
    incoming_time = parse_message_datetime(document.get("time"))
    existing_row = writer.get_columns(table_name, row_key, ["version", "time"])
    existing_version = parse_version_number(existing_row.get("version"))
    existing_time = parse_message_datetime(existing_row.get("time"))

    if existing_row and existing_version is not None and incoming_version is None:
        LOG.warning(
            "Skip POMS type=140 row because incoming version is blank or invalid: table=%s row_key=%s existing_version=%s",
            table_name,
            row_key,
            existing_row.get("version"),
        )
        return row_key, "skipped_missing_incoming_version"

    is_older, older_reason = is_incoming_poms_row_older(
        incoming_version=incoming_version,
        existing_version=existing_version,
        incoming_time=incoming_time,
        existing_time=existing_time,
    )
    if is_older and older_reason == "version":
        LOG.info(
            "Skip POMS type=140 row because incoming version is older: table=%s row_key=%s incoming=%s existing=%s",
            table_name,
            row_key,
            incoming_version,
            existing_version,
        )
        return row_key, "skipped_old_version"

    if is_older and older_reason == "time":
        LOG.info(
            "Skip POMS type=140 row because incoming time is older: table=%s row_key=%s incoming=%s existing=%s incoming_version=%s existing_version=%s",
            table_name,
            row_key,
            document.get("time"),
            existing_row.get("time"),
            document.get("version"),
            existing_row.get("version"),
        )
        return row_key, "skipped_old_time"

    filtered_document = filter_document_fields(document, fields)
    for required_field in ("assetId", "version"):
        if required_field in document:
            filtered_document[required_field] = document[required_field]
    filtered_document["operate_time"] = now_text()
    writer.put(table_name, row_key, filtered_document)
    return row_key, "written"


def append_sample(save_path: Path, payload: Mapping[str, Any]) -> None:
    save_path.parent.mkdir(parents=True, exist_ok=True)
    with save_path.open("a", encoding="utf-8") as fp:
        fp.write("=" * 80)
        fp.write("\n")
        fp.write(f"received_time={now_text()}\n")
        fp.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
        fp.write("\n")


class HBaseWriter:
    def __init__(self, config: Mapping[str, Any]):
        import happybase

        hbase_config = config["hbase"]
        self._happybase = happybase
        self._hosts = list(hbase_config["thrift_hosts"])
        self._host_cycle = cycle(self._hosts)
        self._port = int(hbase_config.get("thrift_port", 9090))
        self._timeout_ms = int(hbase_config.get("timeout_ms", 30000))
        self._column_family = str(hbase_config.get("column_family", "cf"))
        self._batch_size = int(hbase_config.get("batch_size", 100))
        self._connection = None
        self._current_host = None

    @property
    def column_family(self) -> str:
        return self._column_family

    def close(self) -> None:
        if self._connection is not None:
            try:
                self._connection.close()
            except Exception:
                LOG.debug("HBase connection close failed", exc_info=True)
        self._connection = None
        self._current_host = None

    def _connect(self):
        last_error = None
        for _ in range(len(self._hosts)):
            host = next(self._host_cycle)
            try:
                connection = self._happybase.Connection(
                    host=host,
                    port=self._port,
                    timeout=self._timeout_ms,
                    autoconnect=True,
                )
                self._connection = connection
                self._current_host = host
                LOG.info("Connected to HBase thrift %s:%s", host, self._port)
                return connection
            except Exception as exc:
                last_error = exc
                LOG.warning(
                    "Could not connect to HBase thrift %s:%s",
                    host,
                    self._port,
                    exc_info=True,
                )
        raise RuntimeError("Could not connect to any HBase thrift host") from last_error

    def _get_connection(self):
        if self._connection is None:
            return self._connect()
        return self._connection

    def _with_retry(self, action):
        last_error = None
        attempts = max(1, len(self._hosts))
        for attempt in range(attempts):
            try:
                return action(self._get_connection())
            except Exception as exc:
                last_error = exc
                LOG.warning(
                    "HBase operation failed on attempt %s/%s",
                    attempt + 1,
                    attempts,
                    exc_info=True,
                )
                self.close()
        raise RuntimeError("HBase operation failed after retries") from last_error

    def put(self, table_name: str, row_key: str, document: Mapping[str, Any]) -> None:
        columns = self._encode_columns(document)
        if not columns:
            return

        def action(connection):
            table = connection.table(table_name)
            table.put(to_bytes(row_key), columns)

        self._with_retry(action)

    def batch_put(self, table_name: str, rows: Mapping[str, Mapping[str, Any]]) -> None:
        if not rows:
            return

        def action(connection):
            table = connection.table(table_name)
            with table.batch(batch_size=self._batch_size, transaction=True) as batch:
                for row_key, document in rows.items():
                    columns = self._encode_columns(document)
                    if columns:
                        batch.put(to_bytes(row_key), columns)

        self._with_retry(action)

    def get_columns(
        self, table_name: str, row_key: str, columns: Iterable[str]
    ) -> Dict[str, str]:
        full_columns = [
            to_bytes(f"{self._column_family}:{column}") for column in columns
        ]

        def action(connection):
            table = connection.table(table_name)
            row = table.row(to_bytes(row_key), columns=full_columns)
            result: Dict[str, str] = {}
            for column, value in row.items():
                column_text = (
                    column.decode("utf-8") if isinstance(column, bytes) else str(column)
                )
                value_text = (
                    value.decode("utf-8") if isinstance(value, bytes) else str(value)
                )
                qualifier = (
                    column_text.split(":", 1)[1] if ":" in column_text else column_text
                )
                result[qualifier] = value_text
            return result

        return self._with_retry(action)

    def get_json_list(self, table_name: str, row_key: str, column: str) -> List[str]:
        full_column = to_bytes(f"{self._column_family}:{column}")

        def action(connection):
            table = connection.table(table_name)
            row = table.row(to_bytes(row_key), columns=[full_column])
            raw = row.get(full_column)
            if raw is None:
                return []
            try:
                value = json.loads(raw.decode("utf-8"))
            except json.JSONDecodeError:
                LOG.warning(
                    "Invalid JSON list in HBase row %s column %s", row_key, column
                )
                return []
            if not isinstance(value, list):
                return []
            return [str(item) for item in value]

        return self._with_retry(action)

    def _encode_columns(self, document: Mapping[str, Any]) -> Dict[bytes, bytes]:
        columns: Dict[bytes, bytes] = {}
        for key, value in document.items():
            if value is None:
                continue
            if isinstance(value, (dict, list)):
                text_value = json.dumps(
                    value, ensure_ascii=False, separators=(",", ":")
                )
            else:
                text_value = str(value)
            columns[to_bytes(f"{self._column_family}:{key}")] = to_bytes(text_value)
        return columns


class ChartMessageProcessor:
    def __init__(self, hbase_writer: HBaseWriter, config: Mapping[str, Any]):
        self._hbase = hbase_writer
        self._tables = config["tables"]
        self._content_index = config["content_index"]
        self._release_operations = normalized_set(
            config.get("operations", {}).get("release", [])
        )
        self._withdraw_operations = normalized_set(
            config.get("operations", {}).get("withdraw", [])
        )

    def process(self, message: Mapping[str, Any]) -> None:
        message_type = as_text(message.get("type"))
        if message_type == "chart":
            self._process_description(message)
        elif message_type == "chart_content":
            self._process_content(message)
        else:
            LOG.warning(
                "Unsupported message type: %s, id: %s", message_type, message.get("id")
            )

    def _table_name(self, message: Mapping[str, Any], kind: str) -> str:
        table_name_present = is_not_blank(message.get("tableName"))
        use_test = (
            bool(self._tables.get("use_test_when_table_name_present", True))
            and table_name_present
        )
        env_name = "test" if use_test else "prod"
        return self._tables[f"{kind}_{env_name}"]

    def _process_description(self, message: Mapping[str, Any]) -> None:
        row_key = as_text(message.get("id"))
        if not is_not_blank(row_key):
            LOG.warning("Description message id is blank")
            return

        table_name = self._table_name(message, "description")
        operation = normalize_operation(message.get("operation"))
        if operation in self._release_operations:
            self._upsert_description(table_name, row_key, message)
        elif operation in self._withdraw_operations:
            self._mark_description_offline(
                table_name, row_key, message.get("operation")
            )
        else:
            LOG.warning(
                "Unsupported description operation: %s, id: %s",
                message.get("operation"),
                row_key,
            )

    def _upsert_description(
        self, table_name: str, row_key: str, message: Mapping[str, Any]
    ) -> None:
        data = message.get("data") or {}
        if not isinstance(data, Mapping):
            LOG.warning("Description data is not an object, id: %s", row_key)
            return
        if not is_not_blank(data.get("chartId")):
            LOG.warning("Description chartId is blank, id: %s", row_key)
            return

        document: Dict[str, Any] = {
            "id": row_key,
            "type": "chart",
            "chart_id": data.get("chartId"),
            "offline_flg": message.get("operation"),
            "operate_time": now_text(),
        }
        self._put_if_present(document, "message_time", message.get("version"))
        self._put_if_present(document, "time", parse_message_time(message.get("time")))
        self._put_if_present(document, "chart_name", data.get("chartName"))
        self._put_if_present(document, "desc", data.get("desc"))
        self._put_if_present(document, "chart_category", data.get("chartCategory"))
        self._put_if_present(document, "chart_type", data.get("chartType"))
        self._put_if_present(document, "length", data.get("length"))
        self._put_if_present(document, "content_time_span", data.get("contentTimeSpan"))
        self._put_if_present(document, "metric_time_span", data.get("metricTimeSpan"))
        self._put_if_present(document, "execution_cycle", data.get("executionCycle"))
        self._put_if_present(document, "execution_time", data.get("executionTime"))
        self._put_if_present(document, "is_recommend", data.get("isRecommend"))
        self._put_if_present(document, "is_valid", data.get("isValid"))
        self._fill_content_classes(document, data.get("contentClasses"))

        self._hbase.put(table_name, row_key, document)
        LOG.info(
            "Wrote description row to HBase table=%s row_key=%s", table_name, row_key
        )

    def _mark_description_offline(
        self, table_name: str, row_key: str, operation: Any
    ) -> None:
        document = {
            "offline_flg": operation,
            "operate_time": now_text(),
        }
        self._hbase.put(table_name, row_key, document)
        LOG.info(
            "Marked description offline in HBase table=%s row_key=%s",
            table_name,
            row_key,
        )

    def _process_content(self, message: Mapping[str, Any]) -> None:
        chart_id = as_text(message.get("id"))
        if not is_not_blank(chart_id):
            LOG.warning("Content message id/chart_id is blank")
            return

        table_name = self._table_name(message, "content")
        operation = normalize_operation(message.get("operation"))
        if operation in self._release_operations:
            self._mark_old_content_offline(
                table_name, chart_id, message.get("operation")
            )
            self._upsert_content_batch(table_name, message)
        elif operation in self._withdraw_operations:
            self._mark_old_content_offline(
                table_name, chart_id, message.get("operation")
            )
        else:
            LOG.warning(
                "Unsupported content operation: %s, chart_id: %s",
                message.get("operation"),
                chart_id,
            )

    def _upsert_content_batch(
        self, table_name: str, message: Mapping[str, Any]
    ) -> None:
        data = message.get("data") or []
        if not isinstance(data, list):
            LOG.warning("Content data is not a list, chart_id: %s", message.get("id"))
            return

        rows: Dict[str, Dict[str, Any]] = {}
        chart_row_keys: Dict[str, List[str]] = {}
        for item in data:
            if not isinstance(item, Mapping):
                continue
            content_id = as_text(item.get("contentId"))
            chart_id = as_text(item.get("chartId"))
            if not is_not_blank(content_id) or not is_not_blank(chart_id):
                LOG.warning(
                    "contentId or chartId is blank: chart_id=%s content_id=%s",
                    chart_id,
                    content_id,
                )
                continue

            row_key = f"{chart_id}_{content_id}"
            document: Dict[str, Any] = {
                "id": row_key,
                "content_id": content_id,
                "chart_id": chart_id,
                "type": "chart_content",
                "offline_flg": message.get("operation"),
                "operate_time": now_text(),
            }
            self._put_if_present(document, "message_time", message.get("version"))
            self._put_if_present(
                document, "time", parse_message_time(message.get("time"))
            )
            self._put_if_present(document, "content_type", item.get("contentType"))
            self._put_if_present(document, "rank", item.get("rank"))
            self._put_if_present(document, "chart_score", item.get("chartScore"))
            self._put_if_present(document, "is_shield", item.get("isShield"))
            rows[row_key] = document
            chart_row_keys.setdefault(chart_id, []).append(row_key)

        self._hbase.batch_put(table_name, rows)
        self._write_content_indexes(
            table_name, chart_row_keys, message.get("operation")
        )
        LOG.info("Wrote %s content rows to HBase table=%s", len(rows), table_name)

    def _mark_old_content_offline(
        self, content_table: str, chart_id: str, operation: Any
    ) -> None:
        if not self._content_index.get("enabled"):
            LOG.info(
                "Content index is disabled; cannot mark previous content rows offline by chart_id=%s without ES",
                chart_id,
            )
            return

        index_table = self._index_table(content_table)
        index_row_key = self._index_row_key(chart_id)
        index_column = str(self._content_index.get("column", "content_row_keys"))
        old_row_keys = self._hbase.get_json_list(
            index_table, index_row_key, index_column
        )
        if not old_row_keys:
            LOG.info("No content index row found for chart_id=%s", chart_id)
            return

        document = {
            "offline_flg": operation,
            "operate_time": now_text(),
        }
        self._hbase.batch_put(
            content_table, {row_key: document for row_key in old_row_keys}
        )
        self._hbase.put(index_table, index_row_key, document)
        LOG.info(
            "Marked %s old content rows offline for chart_id=%s",
            len(old_row_keys),
            chart_id,
        )

    def _write_content_indexes(
        self,
        content_table: str,
        chart_row_keys: Mapping[str, List[str]],
        operation: Any,
    ) -> None:
        if not self._content_index.get("enabled"):
            return
        index_table = self._index_table(content_table)
        index_column = str(self._content_index.get("column", "content_row_keys"))
        for chart_id, row_keys in chart_row_keys.items():
            self._hbase.put(
                index_table,
                self._index_row_key(chart_id),
                {
                    "chart_id": chart_id,
                    index_column: row_keys,
                    "offline_flg": operation,
                    "operate_time": now_text(),
                },
            )

    def _index_table(self, content_table: str) -> str:
        configured_table = str(self._content_index.get("table") or "").strip()
        return configured_table or content_table

    def _index_row_key(self, chart_id: str) -> str:
        return f"{self._content_index.get('row_prefix', '__chart_index__')}{chart_id}"

    def _fill_content_classes(
        self, document: Dict[str, Any], content_classes: Any
    ) -> None:
        if isinstance(content_classes, str) and content_classes.strip():
            try:
                content_classes = json.loads(content_classes)
            except json.JSONDecodeError:
                document["content_classes"] = content_classes
                return
        if not isinstance(content_classes, list) or not content_classes:
            return

        document["content_classes"] = json.dumps(
            content_classes, ensure_ascii=False, separators=(",", ":")
        )
        first_names, first_codes, second_names, second_codes = (
            set(),
            set(),
            set(),
            set(),
        )
        for item in content_classes:
            if not isinstance(item, Mapping):
                continue
            self._add_if_present(first_names, item.get("firstClassName"))
            self._add_if_present(first_codes, item.get("firstClassCode"))
            self._add_if_present(second_names, item.get("secondClassName"))
            self._add_if_present(second_codes, item.get("secondClassCode"))

        if first_names:
            document["firstclass_name"] = ",".join(sorted(first_names))
        if first_codes:
            document["firstclass_code"] = ",".join(sorted(first_codes))
        if second_names:
            document["secondclass_name"] = ",".join(sorted(second_names))
        if second_codes:
            document["secondclass_code"] = ",".join(sorted(second_codes))

    @staticmethod
    def _put_if_present(document: Dict[str, Any], key: str, value: Any) -> None:
        if is_not_blank(value):
            document[key] = value

    @staticmethod
    def _add_if_present(values: set, value: Any) -> None:
        if is_not_blank(value):
            values.add(str(value))


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


def run_consumer(
    config: Mapping[str, Any],
    dry_run: bool = False,
    once: bool = False,
    count: int = 0,
    write_count: int = 0,
    max_scan: int = 0,
    full_load: bool = False,
    stop_after_idle_seconds: Optional[float] = None,
    inspect_fields: bool = False,
    filter_type: Optional[str] = None,
    save_samples: Optional[str] = None,
    write_table: Optional[str] = None,
    write_fields: Optional[List[str]] = None,
    row_key_field: Optional[str] = None,
    group_id: Optional[str] = None,
    vector_list_type_override: Optional[str] = None,
    stop_at_start_end_offsets: bool = False,
    metadata_timeout_seconds: float = 30.0,
) -> None:
    from kafka import KafkaConsumer

    kafka_config = config["kafka"]
    if full_load:
        once = False
        count = 0
        write_count = 0
        max_scan = 0

    one_shot_config = config.get("one_shot", {})
    one_shot_mode = str(one_shot_config.get("mode", "poms_type140")).strip()
    configured_write_table = one_shot_config.get("write_table") or config.get(
        "tables", {}
    ).get("poms_type140_embedding")
    effective_write_table = write_table or (
        str(configured_write_table).strip() if configured_write_table else None
    )
    selected_fields = write_fields or list(
        one_shot_config.get(
            "fields",
            [
                "id",
                "assetId",
                "version",
                "time",
                "modelName",
                "source",
                "summary",
                "type",
                "vector",
                "vectorDimension",
                "vectorVersion",
            ],
        )
    )
    selected_row_key_field = row_key_field or str(
        one_shot_config.get("row_key_field", "assetId")
    )
    vector_list_type = vector_list_type_override or one_shot_config.get(
        "vector_list_type", "1"
    )
    expected_type = filter_type
    if expected_type is None:
        expected_type = config.get("message_filter", {}).get("type")
    sample_save_path = Path(save_samples) if save_samples else None
    auto_commit = (
        False if dry_run else bool(kafka_config.get("enable_auto_commit", False))
    )
    bootstrap_servers = parse_bootstrap_servers(kafka_config["bootstrap_servers"])
    effective_group_id = group_id or kafka_config["group_id"]
    consumer_options = {
        "bootstrap_servers": bootstrap_servers,
        "group_id": effective_group_id,
        "auto_offset_reset": kafka_config.get("auto_offset_reset", "latest"),
        "enable_auto_commit": auto_commit,
        "max_poll_records": int(kafka_config.get("max_poll_records", 500)),
    }
    api_version = parse_kafka_api_version(kafka_config.get("api_version"))
    if api_version is not None:
        consumer_options["api_version"] = api_version
    LOG.info(
        "Initializing Kafka consumer topic=%s group_id=%s auto_offset_reset=%s bootstrap_servers=%s api_version=%s",
        kafka_config["topic"],
        effective_group_id,
        kafka_config.get("auto_offset_reset", "latest"),
        ",".join(bootstrap_servers),
        api_version,
    )
    topic = kafka_config["topic"]
    consumer = KafkaConsumer(**consumer_options)
    snapshot_end_offsets = None
    if stop_at_start_end_offsets:
        partitions = wait_topic_partitions(consumer, topic, metadata_timeout_seconds)
        consumer.assign(partitions)
        beginning_offsets = consumer.beginning_offsets(partitions)
        snapshot_end_offsets = consumer.end_offsets(partitions)
        for partition in partitions:
            consumer.seek(partition, beginning_offsets[partition])
        total_retained = sum(
            snapshot_end_offsets[tp] - beginning_offsets[tp] for tp in partitions
        )
        LOG.info(
            "Snapshot load assigned topic=%s partitions=%s retained_messages_at_start=%s",
            topic,
            len(partitions),
            total_retained,
        )
    else:
        consumer.subscribe([topic])

    poll_timeout_ms = int(float(kafka_config.get("poll_timeout_seconds", 1.0)) * 1000)
    max_poll_records = 1 if once else int(kafka_config.get("max_poll_records", 500))
    idle_log_interval_seconds = float(
        kafka_config.get("idle_log_interval_seconds", 30.0)
    )
    log_each_message = bool(kafka_config.get("log_each_message", True))
    log_skipped_messages = bool(kafka_config.get("log_skipped_messages", True))
    if full_load:
        log_each_message = False
        log_skipped_messages = False
    progress_log_interval_records = int(
        kafka_config.get("progress_log_interval_records", 10000)
    )
    progress_log_interval_seconds = float(
        kafka_config.get("progress_log_interval_seconds", 60.0)
    )
    effective_stop_after_idle_seconds = (
        float(stop_after_idle_seconds)
        if stop_after_idle_seconds is not None
        else float(kafka_config.get("stop_after_idle_seconds", 0.0))
    )
    stop = {"requested": False}

    def request_stop(signum, frame):
        stop["requested"] = True
        LOG.info("Stop requested by signal %s", signum)

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    writer = None if dry_run else HBaseWriter(config)
    processor = (
        None
        if dry_run or effective_write_table
        else ChartMessageProcessor(writer, config)
    )

    LOG.info(
        "Kafka consumer initialized and subscribed to topic=%s group_id=%s full_load=%s",
        topic,
        effective_group_id,
        full_load,
    )
    try:
        total_scanned = 0
        total_matched = 0
        total_written = 0
        type_counts: Dict[str, int] = {}
        last_idle_log_time = time.time()
        last_record_time = time.time()
        last_progress_log_time = time.time()
        last_progress_log_scanned = 0
        while not stop["requested"]:
            if snapshot_end_offsets is not None:
                all_reached_start_end = True
                for partition, end_offset in snapshot_end_offsets.items():
                    if consumer.position(partition) < end_offset:
                        all_reached_start_end = False
                        break
                if all_reached_start_end:
                    LOG.info(
                        "Reached Kafka end offsets captured at startup; stopping. scanned=%s matched=%s written=%s",
                        total_scanned,
                        total_matched,
                        total_written,
                    )
                    stop["requested"] = True
                    continue

            records_by_partition = consumer.poll(
                timeout_ms=poll_timeout_ms, max_records=max_poll_records
            )
            if not records_by_partition:
                now = time.time()
                if (
                    effective_stop_after_idle_seconds > 0
                    and total_scanned > 0
                    and now - last_record_time >= effective_stop_after_idle_seconds
                ):
                    LOG.info(
                        "No Kafka records for %.2f seconds; stopping. scanned=%s matched=%s written=%s",
                        now - last_record_time,
                        total_scanned,
                        total_matched,
                        total_written,
                    )
                    stop["requested"] = True
                    continue
                if (
                    idle_log_interval_seconds > 0
                    and now - last_idle_log_time >= idle_log_interval_seconds
                ):
                    LOG.info(
                        "Waiting for Kafka records... scanned=%s matched=%s written=%s",
                        total_scanned,
                        total_matched,
                        total_written,
                    )
                    last_idle_log_time = now
                continue
            last_idle_log_time = time.time()
            last_record_time = last_idle_log_time
            processed = 0
            skipped = 0
            start = time.time()
            for records in records_by_partition.values():
                for message in records:
                    if log_each_message:
                        LOG.info(
                            "Received Kafka message topic=%s partition=%s offset=%s",
                            message.topic,
                            message.partition,
                            message.offset,
                        )
                    total_scanned += 1
                    payload = normalize_json_strings(decode_kafka_value(message.value))
                    actual_type = as_text(find_field(payload, "type"))
                    type_counts[actual_type or "<missing>"] = (
                        type_counts.get(actual_type or "<missing>", 0) + 1
                    )
                    if not message_matches_filter(payload, expected_type):
                        skipped += 1
                        if log_skipped_messages:
                            LOG.info(
                                "Skip Kafka message because type=%s does not match expected type=%s",
                                actual_type,
                                expected_type,
                            )
                        if max_scan > 0 and total_scanned >= max_scan:
                            stop["requested"] = True
                            break
                        continue
                    if log_each_message:
                        LOG.info(
                            "Kafka message matched expected type=%s", expected_type
                        )
                    if sample_save_path is not None:
                        append_sample(sample_save_path, payload)
                        LOG.info("Saved Kafka sample to %s", sample_save_path.resolve())
                    if inspect_fields:
                        log_payload_fields(payload)
                    if dry_run:
                        LOG.info(
                            "Dry-run payload: %s",
                            json.dumps(payload, ensure_ascii=False),
                        )
                    elif effective_write_table and one_shot_mode == "generic":
                        row_key = write_selected_fields_to_hbase(
                            writer,
                            effective_write_table,
                            payload,
                            selected_fields,
                            selected_row_key_field,
                        )
                        LOG.info(
                            "Wrote selected fields %s to HBase table=%s row_key=%s",
                            selected_fields,
                            effective_write_table,
                            row_key,
                        )
                        total_written += 1
                    elif effective_write_table:
                        row_key, write_status = write_poms_type140_to_hbase(
                            writer,
                            effective_write_table,
                            payload,
                            selected_fields,
                            vector_list_type,
                        )
                        if not full_load or write_status != "written":
                            LOG.info(
                                "POMS type=140 HBase write status=%s table=%s row_key=%s fields=%s",
                                write_status,
                                effective_write_table,
                                row_key,
                                selected_fields,
                            )
                        if write_status == "written":
                            total_written += 1
                    else:
                        processor.process(payload)
                    processed += 1
                    total_matched += 1
                    if write_count > 0 and total_written >= write_count:
                        stop["requested"] = True
                        break
                    if once or (count > 0 and total_matched >= count):
                        stop["requested"] = True
                        break
                    if max_scan > 0 and total_scanned >= max_scan:
                        stop["requested"] = True
                        break
                if stop["requested"]:
                    break
            if not dry_run and not auto_commit:
                consumer.commit()
            now = time.time()
            if not full_load:
                LOG.info(
                    "Processed %s matched Kafka messages, skipped %s messages in %.2f ms",
                    processed,
                    skipped,
                    (now - start) * 1000,
                )
            records_since_progress = total_scanned - last_progress_log_scanned
            should_log_progress = full_load and (
                (
                    progress_log_interval_records > 0
                    and records_since_progress >= progress_log_interval_records
                )
                or (
                    progress_log_interval_seconds > 0
                    and now - last_progress_log_time >= progress_log_interval_seconds
                )
            )
            if should_log_progress:
                LOG.info(
                    "Full-load progress: scanned=%s matched=%s written=%s last_batch_matched=%s last_batch_skipped=%s type_counts=%s",
                    total_scanned,
                    total_matched,
                    total_written,
                    processed,
                    skipped,
                    json.dumps(type_counts, ensure_ascii=False, sort_keys=True),
                )
                last_progress_log_time = now
                last_progress_log_scanned = total_scanned
            if stop["requested"]:
                LOG.info(
                    "Final scan summary: scanned=%s matched=%s written=%s type_counts=%s",
                    total_scanned,
                    total_matched,
                    total_written,
                    json.dumps(type_counts, ensure_ascii=False, sort_keys=True),
                )
    finally:
        consumer.close()
        if writer is not None:
            writer.close()


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Consume chart Kafka messages and write them to HBase."
    )
    parser.add_argument(
        "-c", "--config", required=True, help="Path to YAML config file."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Consume and parse messages without writing HBase.",
    )
    parser.add_argument(
        "--once", action="store_true", help="Consume one Kafka message and exit."
    )
    parser.add_argument(
        "--count",
        type=int,
        default=0,
        help="Exit after N matched messages. Default 0 means keep listening.",
    )
    parser.add_argument(
        "--write-count",
        type=int,
        default=0,
        help="Exit after N successful HBase writes. Default 0 means disabled.",
    )
    parser.add_argument(
        "--max-scan",
        type=int,
        default=0,
        help="Exit after scanning N total messages, even if not enough matched messages were found.",
    )
    parser.add_argument(
        "--full-load",
        action="store_true",
        help="Continuously consume and write all matched messages, ignoring count limits.",
    )
    parser.add_argument(
        "--stop-after-idle-seconds",
        type=float,
        help="Exit after no Kafka records arrive for this many seconds.",
    )
    parser.add_argument(
        "--inspect-fields",
        action="store_true",
        help="Print parsed field paths and value types.",
    )
    parser.add_argument(
        "--filter-type",
        help="Only process Kafka messages whose type field equals this value.",
    )
    parser.add_argument(
        "--save-samples", help="Append matched Kafka payloads to this txt file."
    )
    parser.add_argument(
        "--write-table",
        help="In one-shot mode, write selected fields to this HBase table.",
    )
    parser.add_argument(
        "--write-fields",
        help="Comma-separated fields to write to HBase. Default comes from one_shot.fields.",
    )
    parser.add_argument(
        "--row-key-field",
        help="Field used as the HBase row key. Default comes from one_shot.row_key_field.",
    )
    parser.add_argument("--group-id", help="Override kafka.group_id for this run.")
    parser.add_argument(
        "--vector-list-type",
        help="For POMS type=140, write this data.vectorList item type. Default comes from one_shot.vector_list_type.",
    )
    parser.add_argument(
        "--stop-at-start-end-offsets",
        action="store_true",
        help="Capture Kafka end offsets at startup and stop after scanning only that retained snapshot.",
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
        run_consumer(
            config,
            dry_run=args.dry_run,
            once=args.once,
            count=args.count,
            write_count=args.write_count,
            max_scan=args.max_scan,
            full_load=args.full_load,
            stop_after_idle_seconds=args.stop_after_idle_seconds,
            inspect_fields=args.inspect_fields,
            filter_type=args.filter_type,
            save_samples=args.save_samples,
            write_table=args.write_table,
            write_fields=parse_csv(args.write_fields),
            row_key_field=args.row_key_field,
            group_id=args.group_id,
            vector_list_type_override=args.vector_list_type,
            stop_at_start_end_offsets=args.stop_at_start_end_offsets,
            metadata_timeout_seconds=args.metadata_timeout_seconds,
        )
    except KeyboardInterrupt:
        LOG.info("Stopped")
    except Exception as exc:
        LOG.error("kafka_to_hbase failed: %s", exc, exc_info=True)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
