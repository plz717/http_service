# -*- coding: utf-8 -*-
"""
===============================================================================
【脚本用途】kafka_to_hbase_v2.py（带中文注释副本）
===============================================================================
kafka_to_hbase_v2.py 带中文注释副本；非法 JSON 跳过而不退出进程。

整体架构（生产场景：POMS type=140 多模态向量）：
    POMS Kafka
        (topic: video-search-recomm-poms-content, type=140 的多模态向量消息)
        │
        ▼  ── 过滤 type=140 ── 解析 JSON ── 挑选 vectorList[type=1] ──
        ▼  ── base64 向量强校验（维数=1024, 字节数=4096） ──
        ▼  ── 版本防倒灌（version+time 比对 HBase 现有行） ──
        ▼  ── 写入 HBase 表 recommend:video_search_recomm_poms_type1_embedding_multimode
        │
    入口函数: main() → run_consumer()
    辅助:   HBaseWriter / ChartMessageProcessor（遗留榜单链路）
    工具函数: find_field / build_poms_type140_document / validate_vector_base64 ...

依赖：kafka-python、happybase、PyYAML

典型命令行用法：
    # 摸底：只看 5 条匹配消息的字段结构，不写库
    python kafka_to_hbase_v2.py -c config.yml --dry-run --count 5 --inspect-fields
    # 灰度试写：真实写 HBase，但只写成功 2 条就停
    python kafka_to_hbase_v2.py -c config.yml --write-count 2 --group-id test-group-001
    # 全量初始化：从最早 offset 灌到启动时刻的快照点，灌完自动退出
    python kafka_to_hbase_v2.py -c config.yml --full-load --stop-at-start-end-offsets
    # 生产常驻（持续消费增量，建议 nohup/supervisor 托管）
    nohup python kafka_to_hbase_v2.py -c config.yml --full-load > kafka_to_hbase.log 2>&1 &

注意事项：
    1. 默认手动提交 offset（enable_auto_commit=false），每个 poll 批次处理完才 commit
    2. dry-run 模式既"不写 HBase"也"不提交 offset"，方便反复观察同一批消息
    3. 换 group_id 时会按 auto_offset_reset（通常 earliest）重新消费

【与 v1 (kafka_to_hbase.py) 的主要差异】
    1. 新增 preview_kafka_value()：非法 JSON 日志中打印消息预览
    2. decode_kafka_value() 增加 None/空值校验
    3. run_consumer() 内 decode 包 try/except：JSONDecodeError 等 → WARNING + continue，
       不会导致全进程退出；type_counts 会计入 <invalid_json>
    生产全量灌库建议使用本 v2 版本。

===============================================================================
"""

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


# ---------- yaml 可选依赖保护 ----------
# PyYAML 仅在生产运行时需要（读 config.yml）。如果环境里没装，也能 import 本文件
# 用于其它脚本（如 verify_type140_hbase_row.py）做局部函数导入。
try:
    import yaml
except ImportError:  # pragma: no cover - runtime dependency guard
    yaml = None


# 全局 logger，所有日志都走这个名字，方便统一按模块级别过滤。
LOG = logging.getLogger("kafka_to_hbase_v2")


# =============================================================================
# 【DEFAULT_CONFIG】 默认配置字典。
# ----------------------------------------------------------------------------
# 生产运行时会用 config.yml 中的值"递归覆盖"这里；用户没配置的项沿用此默认值。
# 结构分为几大块：
#   - kafka: Kafka 消费者参数（topic、group_id 等必须在 yml 里填）
#   - operations: 榜单/榜单内容消息的 release/withdraw 操作别名表
#   - message_filter: 只消费 type=<某值> 的消息（生产为 "140"）
#   - hbase: Thrift 连接参数（thrift_hosts 必须在 yml 里填）
#   - tables: 榜单类旧链路用到的 HBase 表名（生产 140 链路用不到）
#   - content_index: 榜单内容行的 chart_id→row_keys 索引开关
#   - one_shot: 生产 type=140 链路的"一次性写入"配置（表、字段、row key、vectorList 类型）
#   - logging: 日志级别
# =============================================================================
DEFAULT_CONFIG: Dict[str, Any] = {
    "kafka": {
        "api_version": None,            # None 表示让 kafka-python 自动探测
        "auto_offset_reset": "latest",  # 新 group 从末尾消费；生产通常用 earliest
        "enable_auto_commit": False,    # 手动 commit，保证每批次处理完才提交
        "idle_log_interval_seconds": 30.0,
        "log_each_message": True,       # 逐条日志（full_load 时自动关闭，避免刷屏）
        "log_skipped_messages": True,
        "poll_timeout_seconds": 1.0,
        "progress_log_interval_records": 10000,  # full_load 模式进度日志触发条数
        "progress_log_interval_seconds": 60.0,   # full_load 模式进度日志触发秒数
        "stop_after_idle_seconds": 0.0,          # 0 表示"空闲也不停"
        "max_poll_records": 500,         # 一次 poll 最多拉多少条
    },
    "operations": {
        # 榜单类"上线"语义的操作别名（大小写不敏感）
        "release": ["release", "released", "publish", "online", "up"],
        # 榜单类"下线/撤回"语义的操作别名
        "withdraw": ["withdraw", "offline", "down", "delete", "deleted"],
    },
    "message_filter": {
        "type": None,  # 空 = 不过滤；生产会配成 "140"
    },
    "hbase": {
        "thrift_port": 9090,
        "timeout_ms": 30000,
        "column_family": "cf",  # HBase 默认列族
        "batch_size": 100,
    },
    "tables": {
        # 旧榜单链路专用；type=140 链路用不到。
        # 规则：消息里带 tableName 字段→写 _test 表；否则写生产表。
        "use_test_when_table_name_present": True,
        "description_prod": "recommend:video_list_description_base",
        "description_test": "recommend:video_list_description_base_test",
        "content_prod": "recommend:video_list_content_base",
        "content_test": "recommend:video_list_content_base_test",
    },
    "content_index": {
        # 榜单内容的 chart_id→row_keys 索引（目前关闭）
        "enabled": False,
        "table": "",
        "row_prefix": "__chart_index__",
        "column": "content_row_keys",
    },
    "one_shot": {
        # 生产 type=140 链路：按一条消息写一行的模式
        "mode": "poms_type140",                # 仅支持 poms_type140 / generic
        "write_table": None,                   # 生产由 yml 填入实际表名
        "row_key_field": "assetId",            # HBase row key 的来源字段
        "vector_list_type": "1",               # 从 vectorList 里挑 type=1 的那一项
        "fields": [                            # 要写入 HBase 的字段清单
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


# =============================================================================
# 【deep_merge】 递归合并 dict（override 覆盖 base）
# ----------------------------------------------------------------------------
# 用法：先把 DEFAULT_CONFIG 深拷贝一份，再用 deep_merge 把用户 yml 中的覆盖项合并进来。
# 遇到 dict 嵌套就继续递归；非 dict 项直接覆盖。
# =============================================================================
def deep_merge(
    base: MutableMapping[str, Any], override: Mapping[str, Any]
) -> MutableMapping[str, Any]:
    for key, value in override.items():
        if isinstance(value, Mapping) and isinstance(base.get(key), MutableMapping):
            deep_merge(base[key], value)
        else:
            base[key] = value
    return base


# =============================================================================
# 【load_config】 读取 YAML 配置并合并默认值
# ----------------------------------------------------------------------------
# 步骤：
#   1. 打开 yml（UTF-8）
#   2. 用 yaml.safe_load 解析（避免任意代码执行）
#   3. 深拷贝 DEFAULT_CONFIG 得到 base
#   4. deep_merge(base, user_config) 用用户的值覆盖 base
#   5. validate_config 强校验 4 个必填字段
# =============================================================================
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


# =============================================================================
# 【validate_config】 配置强校验
# ----------------------------------------------------------------------------
# 以下 4 项缺一不可（任一为空/None 都会启动报错）：
#   - kafka.bootstrap_servers
#   - kafka.topic
#   - kafka.group_id
#   - hbase.thrift_hosts
# =============================================================================
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


# =============================================================================
# 【parse_bootstrap_servers】 解析 Kafka broker 列表
# ----------------------------------------------------------------------------
# 既接受逗号分隔的字符串 "h1:9092,h2:9092"，也接受 list ["h1:9092","h2:9092"]。
# 自动去除两端空白与空串。
# =============================================================================
def parse_bootstrap_servers(value: Any) -> List[str]:
    if isinstance(value, str):
        return [server.strip() for server in value.split(",") if server.strip()]
    if isinstance(value, Iterable):
        return [str(server).strip() for server in value if str(server).strip()]
    raise ValueError("kafka.bootstrap_servers must be a string or list")


# =============================================================================
# 【parse_kafka_api_version】 解析 Kafka 协议版本
# ----------------------------------------------------------------------------
# 形如 "2.0.0" → (2, 0, 0)；None/"": → None（让 kafka-python 自动探测）。
# =============================================================================
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


# =============================================================================
# 【configure_logging】 配置 logging 基础格式
# ----------------------------------------------------------------------------
# 格式：时间 级别 [logger名] 消息
# 级别从 config.logging.level 读，默认 INFO。
# =============================================================================
def configure_logging(config: Mapping[str, Any]) -> None:
    level_name = str(config.get("logging", {}).get("level", "INFO")).upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )


# =============================================================================
# 【is_not_blank】 值非空判断（None / "" / 全是空白 都算空）
# =============================================================================
def is_not_blank(value: Any) -> bool:
    return value is not None and str(value).strip() != ""


# =============================================================================
# 【as_text】 把任意值转成 str；None 保持 None
# =============================================================================
def as_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return str(value)


# =============================================================================
# 【normalize_operation / normalized_set】 操作名归一化（小写+去空格）
# ----------------------------------------------------------------------------
# 用于把配置里的 release/withdraw 别名表和实际消息 operation 做大小写不敏感比对。
# =============================================================================
def normalize_operation(operation: Any) -> str:
    return str(operation or "").strip().lower()


def normalized_set(values: Iterable[Any]) -> set:
    return {normalize_operation(value) for value in values if is_not_blank(value)}


# =============================================================================
# 【now_text】 当前时间字符串，用作 HBase 行的 operate_time
# =============================================================================
def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# =============================================================================
# 【parse_message_datetime】 解析消息中的时间字符串 → datetime
# ----------------------------------------------------------------------------
# 支持两种格式：
#   "2026-01-02 03:04:05"
#   "2026-01-02T03:04:05"   （ISO 8601 无毫秒版本）
# 只取前 19 个字符，避免后续的毫秒部分影响解析。
# =============================================================================
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


# =============================================================================
# 【parse_message_time】 解析消息时间后再格式化成统一字符串（丢弃毫秒等杂项）
# =============================================================================
def parse_message_time(value: Any) -> Optional[str]:
    parsed = parse_message_datetime(value)
    if parsed is None:
        return None
    return parsed.strftime("%Y-%m-%d %H:%M:%S")


# =============================================================================
# 【to_bytes】 字符串/bytes 统一转 bytes（HBase row key / 列值都要 bytes）
# =============================================================================
def to_bytes(value: Any) -> bytes:
    if isinstance(value, bytes):
        return value
    return str(value).encode("utf-8")


# =============================================================================
# 【parse_csv】 把逗号分隔字符串拆成列表
# =============================================================================
def parse_csv(value: Optional[str]) -> List[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


# =============================================================================
# 【wait_topic_partitions】 轮询等 Kafka topic 分区元数据
# ----------------------------------------------------------------------------
# 用途：--stop-at-start-end-offsets 模式需要 assign 之前先拿到 topic 所有分区。
# 超时内一直 poll 不到则抛 RuntimeError。
# =============================================================================
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


# =============================================================================
# 【normalize_json_strings】 递归"拆 JSON 字符串套娃"
# ----------------------------------------------------------------------------
# POMS 消息有时会出现"值是 JSON 字符串"的情况（双重 JSON 编码）。
# 例如：{"data": "{\"assetId\":\"123\"}"}
# 本函数会把这种字符串递归 parse 回 dict/list，避免后续 payload['data']['assetId']
# 因为 data 是字符串而取不到值。
# =============================================================================
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


# =============================================================================
# 【flatten_fields】 把嵌套结构拍平成 path→value 的扁平字典
# ----------------------------------------------------------------------------
# 例如：
#   {"data": {"assetId": "123", "vectorList": [{"vector":"..."}]}}
#   → {"data.assetId":"123", "data.vectorList":[...], "data.vectorList[0].vector":"..."}
# 用于 find_field 的"嵌套/别名查找"兜底。
# =============================================================================
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


# =============================================================================
# 【find_field】 带别名 + 嵌套路径兜底的字段查找
# ----------------------------------------------------------------------------
# 策略：
#   1. 顶层直接按候选名找（每个字段都有别名表）
#   2. 找不到就 flatten 后按候选名或"路径以 .<name> 结尾"再找一次
# 别名表：
#   assert_id / asset_id / assetId 三者互为别名
#   embedding / vector / emb / embed 互为别名
# 这样即使上游字段命名不稳定也能稳定取到想要的值。
# =============================================================================
def find_field(payload: Mapping[str, Any], field_name: str) -> Any:
    aliases = {
        "assert_id": ["assert_id", "asset_id", "assetId"],
        "asset_id": ["asset_id", "assert_id", "assetId"],
        "assetId": ["assetId", "asset_id", "assert_id"],
        "embedding": ["embedding", "vector", "emb", "embed"],
    }
    candidate_names = aliases.get(field_name, [field_name])

    # 策略 1：顶层直接匹配
    for name in candidate_names:
        if name in payload:
            return payload[name]

    # 策略 2：flatten 后按候选名 / 路径后缀匹配
    flattened = flatten_fields(payload)
    for name in candidate_names:
        if name in flattened:
            return flattened[name]
        suffix = f".{name}"
        for path, value in flattened.items():
            if path.endswith(suffix):
                return value
    return None


# =============================================================================
# 【log_payload_fields】 打印 payload 的所有字段路径和类型（调试用）
# ----------------------------------------------------------------------------
# 对应命令行 --inspect-fields 选项。list 字段打印长度，其它打印类型名。
# =============================================================================
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


# =============================================================================
# 【message_matches_filter】 消息类型过滤（仅保留 type=<expected_type>）
# ----------------------------------------------------------------------------
# expected_type 为空 → 不过滤，全部接受。
# 生产配置里 expected_type="140"，即只处理多模态向量消息。
# =============================================================================
def message_matches_filter(
    payload: Mapping[str, Any], expected_type: Optional[str]
) -> bool:
    if not is_not_blank(expected_type):
        return True
    actual_type = find_field(payload, "type")
    return as_text(actual_type) == str(expected_type)


# =============================================================================
# 【write_selected_fields_to_hbase】 "generic 模式"下的简单写 HBase
# ----------------------------------------------------------------------------
# 用于 one_shot.mode="generic"：按 --write-fields 从 payload 里挑字段直接写。
# 生产 140 链路不走这里（走 write_poms_type140_to_hbase）。
# =============================================================================
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
    # 兜底：如果 row_key_field="assert_id" 但消息里只有 asset_id，就用 asset_id
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


# =============================================================================
# 【parse_version_number】 解析消息的 version 字段为 int（通常是毫秒时间戳）
# ----------------------------------------------------------------------------
# POMS 的 version 是毫秒时间戳，用作版本防倒灌的主要比较键。
# 解析失败（含非数字）返回 None。
# =============================================================================
def parse_version_number(value: Any) -> Optional[int]:
    if not is_not_blank(value):
        return None
    text = str(value).strip()
    try:
        return int(text)
    except ValueError:
        LOG.warning("Could not parse version as numeric timestamp: %s", text)
        return None


# =============================================================================
# 【is_incoming_poms_row_older】 版本防倒灌的核心判断
# ----------------------------------------------------------------------------
# 比较"新消息 (incoming)"与"HBase 现有行 (existing)"的 version+time：
#
#   1. 若两边都有 version：
#       - incoming_version < existing_version  → (True, "version") 旧版本
#       - incoming_version > existing_version  → (False, "")        新版本
#       - 相等时再比 time：time 更旧 → (True, "time")
#   2. 否则（至少一边没有 version）：
#       - 有 time 就比 time：time 更旧 → (True, "time")
#
# 返回 (True, "version") 或 (True, "time") 表示新消息更旧、应丢弃；
# 返回 (False, "") 表示可以写入。
# =============================================================================
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


# =============================================================================
# 【first_mapping】 取出一个"对象或 list 中第一个 dict"（兜底用）
# =============================================================================
def first_mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    if isinstance(value, list):
        for item in value:
            if isinstance(item, Mapping):
                return item
    return {}


# =============================================================================
# 【select_vector_list_item】 从 vectorList 里挑 type=<expected_type> 的一项
# ----------------------------------------------------------------------------
# POMS 消息的 data.vectorList 是一个数组，同一 assetId 可能有多种向量
# （多模态/文本/图像等）。生产只关心 type=1（多模态向量），由 vector_list_type 控制。
# =============================================================================
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


# =============================================================================
# 【put_if_present】 非空才加入 document（避免空字符串写进 HBase 占列）
# =============================================================================
def put_if_present(document: Dict[str, Any], key: str, value: Any) -> None:
    if is_not_blank(value):
        document[key] = value


# =============================================================================
# 【parse_positive_int】 解析正整数，失败返回默认值（用于 vectorDimension）
# =============================================================================
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


# =============================================================================
# 【validate_vector_base64】 向量强校验
# ----------------------------------------------------------------------------
# 校验顺序：
#   1. 非空
#   2. 能严格 base64 解码（validate=True，多余字符会报错）
#   3. 解码后字节长度必须严格等于 expected_dimension * 4
#      （默认 1024 维 × 4 字节/float32 = 4096 字节）
# 任何一项不通过，该条消息的向量都不会入库。
# 返回 (bool, reason_text)。
# =============================================================================
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


# =============================================================================
# 【build_poms_type140_document】 从 POMS type=140 消息构造"待写 HBase 文档"
# ----------------------------------------------------------------------------
# 步骤：
#   1. 取 payload.data；非 dict 时退化成空 dict
#   2. 从 data.vectorList 挑 type=vector_list_type（默认 1）的那一项
#   3. 取 data.assetId 作为 row key；空则尝试用 find_field 别名查找
#   4. 用 validate_vector_base64 校验向量合法性（非法直接 raise，外层会跳过）
#   5. 拼 document：id/operation/assetId/version/time/modelName/source/summary/
#                    type/vector/vectorDimension/vectorVersion/modelVersion
#      vector 仍存 base64 原文（生产不需要解码保存）
# 返回 (row_key, document)。
# =============================================================================
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
    # modelName / summary：优先用 vectorItem 内部的，找不到再兜底找 payload 顶层
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
    # modelVersion 与 vectorVersion 同源（都来自 vectorList[].version）
    put_if_present(document, "modelVersion", vector_item.get("version"))
    return str(asset_id), document

# =============================================================================
# 【filter_document_fields】 按字段白名单过滤待写 HBase 的 document
# ----------------------------------------------------------------------------
# 参数：document 原始文档；fields 要保留的列名列表
# 返回：只含 fields 中列的子集；fields 为空则原样返回
# =============================================================================
def filter_document_fields(
    document: Mapping[str, Any], fields: Iterable[str]
) -> Dict[str, Any]:
    selected = {str(field) for field in fields if is_not_blank(field)}
    if not selected:
        return dict(document)
    return {key: value for key, value in document.items() if key in selected}


# =============================================================================
# 【write_poms_type140_to_hbase】 POMS type=140 写入 HBase（含版本防倒灌）
# ----------------------------------------------------------------------------
# 流程：
#   1. build_poms_type140_document 构造 row_key + document（失败则 skipped_invalid_payload）
#   2. 读 HBase 现有行的 version/time，与 incoming 比较（is_incoming_poms_row_older）
#   3. 旧版本/旧时间 → 跳过；否则 filter_document_fields 后 put
# 返回：(row_key, write_status)，status 如 written / skipped_old_version 等
# =============================================================================
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

    # 版本防倒灌：incoming version/time 不得早于 HBase 现有行
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


# =============================================================================
# 【append_sample】 将匹配到的 Kafka payload 追加写入样本文件（调试用）
# =============================================================================
def append_sample(save_path: Path, payload: Mapping[str, Any]) -> None:
    save_path.parent.mkdir(parents=True, exist_ok=True)
    with save_path.open("a", encoding="utf-8") as fp:
        fp.write("=" * 80)
        fp.write("\n")
        fp.write(f"received_time={now_text()}\n")
        fp.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
        fp.write("\n")


# =============================================================================
# 【HBaseWriter】 HBase Thrift 客户端封装
# ----------------------------------------------------------------------------
# - 多 thrift_hosts 轮询连接，失败自动换 host 重试
# - put / batch_put / get_columns / get_json_list
# - dict/list 列值 JSON 序列化后写入 cf:列名
# =============================================================================
class HBaseWriter:
# --- 初始化 HBaseWriter ---
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
# --- HBaseWriter.column_family ---
    def column_family(self) -> str:
        return self._column_family

# --- 关闭连接 ---
    def close(self) -> None:
        if self._connection is not None:
            try:
                self._connection.close()
            except Exception:
                LOG.debug("HBase connection close failed", exc_info=True)
        self._connection = None
        self._current_host = None

# --- 连接 HBase Thrift（多 host 轮询） ---
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

# --- HBaseWriter._get_connection ---
    def _get_connection(self):
        if self._connection is None:
            return self._connect()
        return self._connection

# --- 带 host 切换的重试执行 ---
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

# --- 单行写入 HBase ---
    def put(self, table_name: str, row_key: str, document: Mapping[str, Any]) -> None:
        columns = self._encode_columns(document)
        if not columns:
            return

        def action(connection):
            table = connection.table(table_name)
            table.put(to_bytes(row_key), columns)

        self._with_retry(action)

# --- 批量写入 HBase ---
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

# --- 读取指定列限定符 ---
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

# --- HBaseWriter.get_json_list ---
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

# --- document → cf:qualifier bytes 映射 ---
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


# =============================================================================
# 【ChartMessageProcessor】 榜单类 Kafka 消息处理器（遗留 chart/chart_content 链路）
# ----------------------------------------------------------------------------
# type=chart → 写 description 表；type=chart_content → 写 content 表
# operation 归一化后区分 release（上线）与 withdraw（下线）
# =============================================================================
class ChartMessageProcessor:
# --- 初始化 ChartMessageProcessor ---
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

# --- 按 message.type 分发 chart / chart_content ---
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

# --- ChartMessageProcessor._table_name ---
    def _table_name(self, message: Mapping[str, Any], kind: str) -> str:
        table_name_present = is_not_blank(message.get("tableName"))
        use_test = (
            bool(self._tables.get("use_test_when_table_name_present", True))
            and table_name_present
        )
        env_name = "test" if use_test else "prod"
        return self._tables[f"{kind}_{env_name}"]

# --- ChartMessageProcessor._process_description ---
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

# --- ChartMessageProcessor._upsert_description ---
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

# --- ChartMessageProcessor._mark_description_offline ---
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

# --- ChartMessageProcessor._process_content ---
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

# --- ChartMessageProcessor._upsert_content_batch ---
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

# --- ChartMessageProcessor._mark_old_content_offline ---
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

# --- ChartMessageProcessor._write_content_indexes ---
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

# --- ChartMessageProcessor._index_table ---
    def _index_table(self, content_table: str) -> str:
        configured_table = str(self._content_index.get("table") or "").strip()
        return configured_table or content_table

# --- ChartMessageProcessor._index_row_key ---
    def _index_row_key(self, chart_id: str) -> str:
        return f"{self._content_index.get('row_prefix', '__chart_index__')}{chart_id}"

# --- ChartMessageProcessor._fill_content_classes ---
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


# =============================================================================
# 【preview_kafka_value】 截断预览 Kafka 原始 value（v2 非法 JSON 日志用）
# ----------------------------------------------------------------------------
# 将 bytes 解码为 UTF-8（替换非法字符），换行转义，超长截断并标注总长度
# =============================================================================
def preview_kafka_value(value: Any, max_len: int = 200) -> str:
    if value is None:
        return "<None>"
    if isinstance(value, bytes):
        try:
            text = value.decode("utf-8", errors="replace")
        except Exception:
            return "<bytes len=%s>" % len(value)
    else:
        text = str(value)
    text = text.replace("\r", "\\r").replace("\n", "\\n")
    if not text:
        return "<empty>"
    if len(text) <= max_len:
        return text
    return "%s...<len=%s>" % (text[:max_len], len(text))


# =============================================================================
# 【decode_kafka_value】 解析 Kafka message.value → JSON 对象 dict
# ----------------------------------------------------------------------------
# 支持 bytes/str；双重 JSON 编码（外层 parse 后仍是 str 再 parse 一次）
# v2 额外校验 None/空 bytes/空白串，便于上层 try/except 跳过
# =============================================================================
def decode_kafka_value(value: Any) -> Mapping[str, Any]:
    if value is None:
        raise ValueError("Kafka value is None")
    if isinstance(value, bytes):
        if not value:
            raise ValueError("Kafka value is empty bytes")
        text = value.decode("utf-8")
    else:
        text = str(value)
    if not str(text).strip():
        raise ValueError("Kafka value is blank")
    payload = json.loads(text)
    if isinstance(payload, str):
        payload = json.loads(payload)
    if not isinstance(payload, Mapping):
        raise ValueError("Kafka value must be a JSON object")
    return payload


# =============================================================================
# 【run_consumer】 Kafka 消费主循环
# ----------------------------------------------------------------------------
# 核心流程：
#   poll → decode/normalize → type 过滤 → dry-run 或写 HBase 或 ChartMessageProcessor
#   每批次处理完手动 commit（非 dry-run 且 enable_auto_commit=false）
# 模式：
#   --full-load 持续灌库；--stop-at-start-end-offsets 快照灌完即停
#   --dry-run 不写库不 commit；--write-count / --count 控制退出条件
# v1：decode 异常会冒泡导致进程退出；v2：非法 JSON 打 WARNING 后 continue
# =============================================================================
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

# --- ChartMessageProcessor.request_stop ---
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
                    # v2：非法 JSON 捕获后跳过，不中断消费循环
                    try:
                        payload = normalize_json_strings(
                            decode_kafka_value(message.value)
                        )
                    except (
                        json.JSONDecodeError,
                        UnicodeDecodeError,
                        ValueError,
                        TypeError,
                    ) as exc:
                        skipped += 1
                        type_counts["<invalid_json>"] = (
                            type_counts.get("<invalid_json>", 0) + 1
                        )
                        LOG.warning(
                            "Skip Kafka message with invalid JSON payload: "
                            "topic=%s partition=%s offset=%s error=%s preview=%s",
                            message.topic,
                            message.partition,
                            message.offset,
                            exc,
                            preview_kafka_value(message.value),
                        )
                        if max_scan > 0 and total_scanned >= max_scan:
                            stop["requested"] = True
                            break
                        continue
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
            # 本批次处理完毕后再提交 offset（at-least-once）
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


# =============================================================================
# 【main】 命令行入口：解析参数 → load_config → run_consumer
# =============================================================================
def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Consume chart Kafka messages and write them to HBase "
            "(v2: skip invalid JSON payloads with WARNING instead of exiting)."
        )
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
        LOG.error("kafka_to_hbase_v2 failed: %s", exc, exc_info=True)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
