# -*- coding: utf-8 -*-
"""
===============================================================================
【脚本用途】kafka_to_hbase.py 带中文注释的"讲解版"副本
===============================================================================
本文件是 kafka_to_hbase.py 的注释增强版，**不改变任何原始逻辑**。
仅供阅读/交接/排查使用；生产运行请仍然使用原文件 kafka_to_hbase.py。

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
    工具函数: find_field（别名查找）/ build_poms_type140_document / validate_vector_base64 ...

典型命令行用法：
    # 摸底：只看 5 条匹配消息的字段结构，不写库
    python kafka_to_hbase.py -c config.yml --dry-run --count 5 --inspect-fields
    # 灰度试写：真实写 HBase，但只写成功 2 条就停
    python kafka_to_hbase.py -c config.yml --write-count 2 --group-id test-group-001
    # 全量初始化：从最早 offset 灌到启动时刻的快照点，灌完自动退出
    python kafka_to_hbase.py -c config.yml --full-load --stop-at-start-end-offsets
    # 生产常驻（持续消费增量，建议 nohup/supervisor 托管）
    nohup python kafka_to_hbase.py -c config.yml --full-load > kafka_to_hbase.log 2>&1 &

注意事项：
    1. 依赖：kafka-python / happybase / pyyaml
    2. 默认手动提交 offset（enable_auto_commit=false），每个 poll 批次处理完才 commit
    3. dry-run 模式既"不写 HBase"也"不提交 offset"，方便反复观察同一批消息
    4. 换 group_id 时会按 auto_offset_reset（通常 earliest）重新消费
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
LOG = logging.getLogger("kafka_to_hbase")


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


# 注释占位：后续部分（filter_document_fields / write_poms_type140_to_hbase / HBaseWriter /
#           ChartMessageProcessor / run_consumer / main）见下文继续。
