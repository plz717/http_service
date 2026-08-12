#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
hbase_table_stats.py — 扫描 HBase 表并统计行数、列出现频次与样例行

【用途】
  通过 HBase Thrift 对指定表进行 get 单行或全表 scan，
  输出行总数、空行数、各列出现次数、采样行及扫描速率等 JSON 摘要。
  可用于数据量评估、列结构探查、写入 pipeline 验证。

【依赖】
  - happybase：HBase Thrift 客户端
  - 可选 PyYAML：使用 -c/--config 从 kafka_to_hbase 的 config.yml 读取 HBase 配置

【典型命令行用法】
  # 全表 scan 统计（默认表）
  python hbase_table_stats.py

  # 从 YAML 配置读取连接信息
  python hbase_table_stats.py -c config.yml --limit 1000 --output stats.json

  # 只 get 单行
  python hbase_table_stats.py --row-key 1234567890

  # 只 scan 指定列
  python hbase_table_stats.py --columns assetId,version,time --progress-every 5000
"""

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
    """HBase 单元格值转 UTF-8 字符串。"""
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def to_bytes(value: Any) -> bytes:
    """值转 bytes。"""
    if isinstance(value, bytes):
        return value
    return str(value).encode("utf-8")


def is_not_blank(value: Any) -> bool:
    """非空判断。"""
    return value is not None and str(value).strip() != ""


def parse_csv(value: Optional[str]) -> List[str]:
    """
    解析逗号分隔字符串为列表，None/空串返回 []。

    参数:
        value: 逗号分隔文本
    返回:
        非空 token 列表
    """
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def short_value(value: Any, max_len: int = 80) -> str:
    """
    截断长字符串用于样例展示。

    参数:
        value: 原始值
        max_len: 最大显示长度
    返回:
        短文本或 "<missing>"
    """
    if value is None:
        return "<missing>"
    text = str(value)
    if len(text) <= max_len:
        return text
    return "%s...<len=%s>" % (text[:max_len], len(text))


def connect_hbase(hosts: Iterable[str], port: int, timeout_ms: int):
    """
    连接 HBase Thrift，多主机轮询。

    参数:
        hosts: 主机列表
        port: Thrift 端口
        timeout_ms: 超时毫秒
    返回:
        happybase.Connection
    """
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
    """
    解码 HBase 行为 qualifier -> 字符串 字典。

    参数:
        row: 原始行
        column_family: 列族名
    返回:
        去掉列族前缀后的列映射
    """
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
    """
    从 YAML 配置文件加载 HBase 连接与表名设置。

    复用 kafka_to_hbase 项目 config.yml 中的 hbase/tables/one_shot 节点。

    参数:
        path: config.yml 路径
    返回:
        hosts/port/timeout_ms/column_family/table 字典
    异常:
        RuntimeError: 未安装 PyYAML
    """
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
    """
    按 row key get 单行并解码。

    参数:
        table: happybase Table 对象
        column_family: 列族
        row_key: 行键
    返回:
        (row_key, 解码后的列 dict)
    异常:
        RuntimeError: 行不存在或为空
    """
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
    """
    全表 scan 并累计统计信息。

    统计：行数、空行数、各列出现次数、前 N 条样例、扫描耗时与速率。

    参数:
        table: HBase 表对象
        column_family: 列族
        scan_batch_size: scan 批大小
        limit: 最大扫描行数，0 表示全表
        progress_every: 每 N 行打印进度（另每 30 秒也会打印）
        sample_size: 保留样例行数
        columns: 可选，只 scan 指定 qualifier 列表
    返回:
        统计结果 dict
    """
    scan_kwargs: Dict[str, Any] = {"batch_size": scan_batch_size}
    if columns:
        # 限定 scan 列，减少网络传输
        scan_kwargs["columns"] = [
            to_bytes("%s:%s" % (column_family, column)) for column in columns
        ]

    row_count = 0
    empty_row_count = 0
    column_presence = Counter()
    samples: List[Dict[str, Any]] = []
    started = time.time()
    last_progress = started

    # HBase 全表 scan 主循环
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
    """
    命令行入口：合并配置、连接 HBase、get 或 scan、输出 JSON 摘要。

    返回:
        0 成功；130 键盘中断；1 其他异常（由 __main__ 捕获）
    """
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

    # 合并默认、YAML 配置与命令行覆盖
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
            # 单行 get 模式
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
            # 全表/限量 scan 模式
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
