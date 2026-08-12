# -*- coding: utf-8 -*-
"""
verify_type140_hbase_row.py — 从 Kafka 查找 type=140 消息并与 HBase 行逐字段比对

【用途】
  在 Kafka topic 中扫描指定 assetId 的消息（可过滤外层 type 与 vectorList type），
  使用 kafka_to_hbase 同款映射逻辑生成 HBase 文档，读取 HBase 实际行并逐字段对比，
  输出差异报告 txt，用于验证 type140 embedding 写入 pipeline 的正确性。

【依赖】
  - kafka-python：KafkaConsumer
  - kafka_to_hbase 模块：HBaseWriter、build_poms_type140_document、load_config 等
  - PyYAML 配置文件（与 kafka_to_hbase 共用 config.yml）

【典型命令行用法】
  python verify_type140_hbase_row.py -c config.yml --asset-id 1234567890 --output report.txt

  # 限制扫描消息数、指定 type 过滤
  python verify_type140_hbase_row.py -c config.yml --asset-id 1234567890 --output out.txt --max-scan 100000 --filter-type 140
"""

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
    """
    等待 Kafka consumer 获取 topic 分区元数据。

    参数:
        consumer: KafkaConsumer 实例
        topic: topic 名称
        timeout_seconds: 最长等待秒数
    返回:
        TopicPartition 列表
    异常:
        RuntimeError: 超时仍无法获取分区
    """
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
    """
    从 Kafka 消息 payload 中提取 assetId。

    优先 data.assetId，否则递归 find_field。

    参数:
        payload: 解码后的 JSON 对象
    返回:
        assetId 值或 None
    """
    data = payload.get("data")
    if isinstance(data, Mapping) and is_not_blank(data.get("assetId")):
        return data.get("assetId")
    return find_field(payload, "assetId")


def selected_fields_from_config(config: Mapping[str, Any]) -> List[str]:
    """
    从 config one_shot.fields 读取需要比对/写入的字段列表。

    参数:
        config: YAML 加载后的配置 dict
    返回:
        字段名字符串列表
    """
    one_shot_config = config.get("one_shot", {})
    fields = one_shot_config.get("fields") or []
    return [str(field) for field in fields if is_not_blank(field)]


def table_from_config(config: Mapping[str, Any]) -> str:
    """
    解析 HBase 表名：one_shot.write_table 或 tables.poms_type140_embedding。

    参数:
        config: 配置 dict
    返回:
        表名字符串
    异常:
        ValueError: 配置中缺少表名
    """
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
    """
    比较 Kafka 映射值与 HBase 值是否一致（均转 str 比较）。

    参数:
        expected: Kafka 侧期望值
        actual: HBase 侧实际值
    返回:
        True 表示匹配
    """
    if expected is None and actual is None:
        return True
    return str(expected) == str(actual)


def short_value(value: Any, max_len: int = 120) -> str:
    """截断长值用于控制台 DIFF 输出。"""
    if value is None:
        return "<missing>"
    text = str(value)
    if len(text) <= max_len:
        return text
    return "%s...<len=%s>" % (text[:max_len], len(text))


def compare_documents(
    mapped: Mapping[str, Any], hbase_row: Mapping[str, Any], fields: Iterable[str]
) -> List[Dict[str, Any]]:
    """
    逐字段对比 Kafka 映射文档与 HBase 行。

    参数:
        mapped: 从 Kafka 映射得到的 HBase 列 dict
        hbase_row: HBase 实际读取的列 dict
        fields: 待比对字段名 iterable
    返回:
        每项含 field/match/kafka_value/hbase_value/长度 的列表
    """
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
    """
    通过 HBaseWriter 读取指定行的若干列。

    参数:
        config: 含 hbase 连接信息的配置
        table_name: 表名
        row_key: 行键
        columns: 列 qualifier 列表
    返回:
        列名 -> UTF-8 字符串
    """
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
    """
    从 Kafka topic 最早 offset 开始扫描，直到找到目标 assetId 的消息。

    支持 SIGINT/SIGTERM 优雅停止；按 message_filter.type 过滤消息。

    参数:
        config: 完整 YAML 配置
        asset_id: 目标 assetId（字符串）
        expected_type: 外层消息 type 过滤，None 则用 config
        max_scan: 最大扫描消息数，0 不限制
        progress_records: 每 N 条打印进度
        metadata_timeout_seconds: 等待分区元数据超时
    返回:
        (Kafka message 对象, 规范化 payload dict, 原始 JSON 字符串)
    异常:
        RuntimeError: 扫完 retained 消息仍未找到
    """
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
        # 从各分区最早 offset 开始读
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

            # 检查是否所有分区都已读到 end offset
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
                    # 按 type 等业务规则过滤
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
    """
    将 Kafka 元数据、原始消息、映射文档、HBase 行及字段对比写入报告文件。

    参数:
        output_path: 输出 txt 路径
        message: 命中的 Kafka 消息
        raw_text: 原始 value 字符串
        payload: 规范化 JSON
        mapped_document: Kafka 映射后的 HBase 文档
        hbase_row: HBase 实际行
        comparisons: compare_documents 结果
    """
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
    """
    主流程：加载配置 → 扫描 Kafka 找 assetId → 映射文档 → 读 HBase → 比对 → 写报告。

    返回:
        0 成功，1 异常
    """
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

        # 1. Kafka 扫描定位目标消息
        message, payload, raw_text = scan_kafka_for_asset_id(
            config=config,
            asset_id=str(args.asset_id),
            expected_type=expected_type,
            max_scan=args.max_scan,
            progress_records=args.progress_records,
            metadata_timeout_seconds=args.metadata_timeout_seconds,
        )
        # 2. 使用与写入 pipeline 相同的映射逻辑
        row_key, document = build_poms_type140_document(payload, vector_list_type)
        mapped_document = filter_document_fields(document, selected_fields)
        for required_field in ("assetId", "version"):
            if required_field in document:
                mapped_document[required_field] = document[required_field]

        # 3. 读取 HBase 当前行并逐字段比对
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
