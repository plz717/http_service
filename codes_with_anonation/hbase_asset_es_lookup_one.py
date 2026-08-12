# -*- coding: utf-8 -*-
"""
hbase_asset_es_lookup_one.py — 读取单条 HBase 记录并用 assetId 反查 ES 完整文档

【用途】
  从 HBase 表读取一行（或 scan 首行）的 assetId，以 assert_id 在 ES 中查询对应文档，
  生成 HBase 列信息与 ES 字段路径的对比报告，用于单条数据联调与排查。

【依赖】
  - happybase：HBase Thrift 连接
  - es_lookup_program_id：EsClient、密码加载、ES 默认配置
  - 可选：EsManager.py / ES_PASSWORD

【典型命令行用法】
  # 指定 row key 查询
  python hbase_asset_es_lookup_one.py --row-key 1234567890

  # 不指定 row key，自动 scan 表内第一条有 assetId 的行
  python hbase_asset_es_lookup_one.py

  # 输出报告到文件
  python hbase_asset_es_lookup_one.py --row-key 1234567890 --output report.txt --print-query
"""

import argparse
import getpass
import json
import os
import sys
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple


import happybase


from es_lookup_program_id import (
    DEFAULT_INDEX,
    DEFAULT_NODES,
    EsClient,
    load_password_from_es_manager,
    parse_csv,
    parse_package_ids,
)


# 默认 HBase 表（多模态 embedding 推荐表）
DEFAULT_HBASE_TABLE = "recommend:video_search_recomm_poms_type1_embedding_multimode"
# 默认 HBase Thrift 主机列表
DEFAULT_HBASE_HOSTS = (
    "h1003.dm.migu.cn,h1004.dm.migu.cn,h1005.dm.migu.cn,h1006.dm.migu.cn,"
    "h1007.dm.migu.cn,h1008.dm.migu.cn,h1009.dm.migu.cn,h1010.dm.migu.cn,"
    "h1011.dm.migu.cn,h1012.dm.migu.cn"
)


def to_text(value: Any) -> Optional[str]:
    """将 bytes/任意值转为 UTF-8 字符串，bytes 解码失败时用 replace 策略。"""
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def to_bytes(value: Any) -> bytes:
    """将值转为 bytes，已是 bytes 则原样返回。"""
    if isinstance(value, bytes):
        return value
    return str(value).encode("utf-8")


def is_not_blank(value: Any) -> bool:
    """判断值非空（非 None 且非纯空白字符串）。"""
    return value is not None and str(value).strip() != ""


def connect_hbase(hosts: Iterable[str], port: int, timeout_ms: int):
    """
    依次尝试连接 HBase Thrift 主机，首个成功即返回连接。

    参数:
        hosts: 主机名列表
        port: Thrift 端口，通常 9090
        timeout_ms: 连接超时（毫秒）
    返回:
        happybase.Connection 实例
    异常:
        RuntimeError: 所有主机均连接失败
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
    将 HBase 行数据解码为 qualifier -> 字符串值 的字典。

    去掉列族前缀（如 cf:assetId -> assetId）。

    参数:
        row: happybase 返回的原始行 dict
        column_family: 列族名，默认 cf
    返回:
        列限定符到 UTF-8 字符串的映射
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


def get_hbase_asset_id(
    connection: Any,
    table_name: str,
    column_family: str,
    row_key: Optional[str],
    scan_batch_size: int,
) -> Tuple[str, str, Dict[str, str]]:
    """
    从 HBase 获取一条记录的 row_key 与 assetId。

    若指定 row_key 则直接 get；否则 scan 首条含 assetId 的行。

    参数:
        connection: HBase 连接
        table_name: 表名
        column_family: 列族
        row_key: 可选行键
        scan_batch_size: scan 批大小
    返回:
        (row_key, asset_id, 解码后的整行列 dict)
    """
    table = connection.table(table_name)
    asset_column = to_bytes("%s:assetId" % column_family)

    if is_not_blank(row_key):
        # 按 row key 精确读取 assetId 列
        row = table.row(to_bytes(row_key), columns=[asset_column])
        decoded = decode_hbase_row(row, column_family)
        asset_id = decoded.get("assetId") or row_key
        if not is_not_blank(asset_id):
            raise RuntimeError("HBase row %s has no assetId" % row_key)
        return str(row_key), str(asset_id), decoded

    # 未指定 row key：scan 第一条有效记录
    for key, row in table.scan(
        columns=[asset_column], batch_size=scan_batch_size, limit=1
    ):
        decoded = decode_hbase_row(row, column_family)
        key_text = to_text(key) or ""
        asset_id = decoded.get("assetId") or key_text
        if is_not_blank(asset_id):
            return key_text, str(asset_id), decoded

    raise RuntimeError("No HBase row with assetId found in table %s" % table_name)


def build_asset_id_query(
    asset_id: str, package_ids: List[Any], size: int
) -> Dict[str, Any]:
    """
    构建以 assert_id（即 HBase assetId）为条件的 ES 查询。

    可选 product_info_package_id 过滤；空 package_ids 则不过滤。

    参数:
        asset_id: HBase 中的 assetId，对应 ES assert_id
        package_ids: package 过滤列表
        size: 返回文档数上限
    返回:
        ES _search 请求体
    """
    query: Dict[str, Any] = {
        "size": size,
        "query": {
            "bool": {
                "must": [
                    {
                        "bool": {
                            # assert_id 双字段匹配
                            "should": [
                                {"term": {"assert_id": asset_id}},
                                {"term": {"assert_id.keyword": asset_id}},
                            ],
                            "minimum_should_match": 1,
                        }
                    }
                ]
            }
        },
    }
    if package_ids:
        package_id_texts = [str(item) for item in package_ids]
        query["query"]["bool"]["filter"] = [
            {
                "bool": {
                    "should": [
                        {"terms": {"product_info_package_id": package_ids}},
                        {
                            "terms": {
                                "product_info_package_id.keyword": package_id_texts
                            }
                        },
                    ],
                    "minimum_should_match": 1,
                }
            }
        ]
    return query


def total_hits(response: Mapping[str, Any]) -> Any:
    """
    从 ES 响应中提取总命中数。

    兼容 ES 7+ 的 total.value 与旧版数字 total。
    """
    total = response.get("hits", {}).get("total", 0)
    if isinstance(total, Mapping):
        return total.get("value", 0)
    return total


def flatten_source_fields(value: Any, prefix: str = "") -> Dict[str, str]:
    """
    递归展平 ES _source 文档，输出字段路径及其 Python 类型名。

    用于报告 ES 文档结构概览。

    参数:
        value: _source 或其子节点
        prefix: 当前字段路径前缀
    返回:
        路径 -> 类型名（list 显示为 list[N]）
    """
    fields: Dict[str, str] = {}
    if isinstance(value, Mapping):
        for key, item in value.items():
            path = "%s.%s" % (prefix, key) if prefix else str(key)
            fields.update(flatten_source_fields(item, path))
    elif isinstance(value, list):
        fields[prefix] = "list[%s]" % len(value)
        if value and isinstance(value[0], Mapping):
            fields.update(flatten_source_fields(value[0], "%s[0]" % prefix))
    else:
        fields[prefix] = type(value).__name__
    return fields


def load_es_password(
    username: str, password_arg: Optional[str], use_es_manager: bool
) -> str:
    """
    按优先级加载 ES 密码：参数 → EsManager → 环境变量 → 交互输入。

    参数:
        username: ES 用户名
        password_arg: 命令行传入的密码
        use_es_manager: 是否尝试 EsManager
    返回:
        非空密码字符串
    """
    password = password_arg
    if password is None and use_es_manager:
        password = load_password_from_es_manager(username)
    if password is None:
        password = os.environ.get("ES_PASSWORD")
    if password is None:
        password = getpass.getpass("ES password: ")
    return password


def print_and_optionally_save(text: str, output: Optional[str]) -> None:
    """
    打印报告文本，可选写入文件。

    参数:
        text: 报告内容
        output: 输出文件路径，None 则仅打印
    """
    print(text)
    if output:
        with open(output, "w", encoding="utf-8") as fp:
            fp.write(text)
            fp.write("\n")
        print("Wrote report to %s" % output)


def build_report(
    hbase_table: str,
    hbase_row_key: str,
    asset_id: str,
    hbase_row: Mapping[str, str],
    index: str,
    response: Mapping[str, Any],
    query: Mapping[str, Any],
    print_query: bool,
) -> str:
    """
    组装 HBase 行与 ES 命中结果的文本报告。

    参数:
        hbase_table/hbase_row_key/asset_id/hbase_row: HBase 侧信息
        index: ES 索引名
        response: ES 搜索响应
        query: 使用的查询 DSL
        print_query: 是否在报告中包含查询 JSON
    返回:
        多行文本报告
    """
    lines: List[str] = []
    hits = response.get("hits", {}).get("hits", [])

    lines.append("=== HBase row ===")
    lines.append("hbase_table=%s" % hbase_table)
    lines.append("hbase_row_key=%s" % hbase_row_key)
    lines.append("assetId=%s" % asset_id)
    if hbase_row:
        lines.append("hbase_columns=%s" % ",".join(sorted(hbase_row.keys())))

    lines.append("")
    lines.append("=== ES search ===")
    lines.append("index=%s" % index)
    lines.append("total=%s returned=%s" % (total_hits(response), len(hits)))

    if print_query:
        lines.append("")
        lines.append("=== ES query ===")
        lines.append(json.dumps(query, ensure_ascii=False, indent=2, sort_keys=True))

    if not hits:
        lines.append("No ES hit found for assert_id/assetId=%s." % asset_id)
        return "\n".join(lines)

    for index_num, hit in enumerate(hits, 1):
        source = hit.get("_source", {})
        if not isinstance(source, Mapping):
            source = {}
        field_paths = flatten_source_fields(source)
        lines.append("")
        lines.append("--- ES hit %s ---" % index_num)
        lines.append("_id=%s _score=%s" % (hit.get("_id"), hit.get("_score")))
        lines.append("program_id=%s" % source.get("program_id"))
        lines.append("assert_id=%s" % source.get("assert_id"))
        lines.append(
            "product_info_package_id=%s" % source.get("product_info_package_id")
        )
        lines.append(
            "top_level_fields=%s" % ",".join(sorted(str(key) for key in source.keys()))
        )
        lines.append("field_path_count=%s" % len(field_paths))
        lines.append("=== ES source field paths ===")
        for path, type_name in sorted(field_paths.items()):
            lines.append("%s (%s)" % (path, type_name))
        lines.append("=== ES source JSON ===")
        lines.append(json.dumps(source, ensure_ascii=False, indent=2, sort_keys=True))

    return "\n".join(lines)


def main(argv: Optional[Iterable[str]] = None) -> int:
    """
    主流程：连接 HBase → 读 assetId → 查 ES → 生成报告。

    返回:
        0 表示成功
    """
    parser = argparse.ArgumentParser(
        description="Read one HBase assetId and lookup the full ES document by assert_id."
    )
    parser.add_argument(
        "--hbase-hosts",
        default=DEFAULT_HBASE_HOSTS,
        help="Comma-separated HBase thrift hosts.",
    )
    parser.add_argument(
        "--hbase-port", type=int, default=9090, help="HBase thrift port."
    )
    parser.add_argument(
        "--hbase-timeout-ms",
        type=int,
        default=30000,
        help="HBase thrift timeout in ms.",
    )
    parser.add_argument(
        "--hbase-table", default=DEFAULT_HBASE_TABLE, help="HBase table name."
    )
    parser.add_argument("--column-family", default="cf", help="HBase column family.")
    parser.add_argument(
        "--row-key",
        help="HBase row key / assetId to query. If omitted, scan one HBase row.",
    )
    parser.add_argument(
        "--hbase-scan-batch-size", type=int, default=100, help="HBase scan batch size."
    )
    parser.add_argument("--index", default=DEFAULT_INDEX, help="ES index name.")
    parser.add_argument(
        "--nodes", default=DEFAULT_NODES, help="Comma-separated ES host:port nodes."
    )
    parser.add_argument("--scheme", default="https", help="ES scheme.")
    parser.add_argument("--username", default="elastic", help="ES username.")
    parser.add_argument("--password", help="ES password.")
    parser.add_argument(
        "--no-es-manager",
        action="store_true",
        help="Do not try to load password from EsManager.py.",
    )
    parser.add_argument(
        "--package-ids",
        default="1,141",
        help="Allowed product_info_package_id values. Use empty string for no filter.",
    )
    parser.add_argument("--size", type=int, default=1, help="Max ES hits to return.")
    parser.add_argument(
        "--timeout", type=float, default=10.0, help="ES request timeout seconds."
    )
    parser.add_argument(
        "--verify-cert", action="store_true", help="Verify ES HTTPS certificate."
    )
    parser.add_argument(
        "--print-query", action="store_true", help="Print ES query JSON."
    )
    parser.add_argument("--output", help="Write report to this txt file.")
    args = parser.parse_args(argv)

    # 1. HBase 读取
    connection = connect_hbase(
        hosts=parse_csv(args.hbase_hosts),
        port=args.hbase_port,
        timeout_ms=args.hbase_timeout_ms,
    )
    try:
        hbase_row_key, asset_id, hbase_row = get_hbase_asset_id(
            connection=connection,
            table_name=args.hbase_table,
            column_family=args.column_family,
            row_key=args.row_key,
            scan_batch_size=args.hbase_scan_batch_size,
        )
    finally:
        connection.close()

    # 2. ES 查询
    password = load_es_password(args.username, args.password, not args.no_es_manager)
    es_client = EsClient(
        nodes=parse_csv(args.nodes),
        scheme=args.scheme,
        username=args.username,
        password=password,
        timeout=args.timeout,
        verify_cert=args.verify_cert,
    )
    package_ids = parse_package_ids(args.package_ids)
    query = build_asset_id_query(
        asset_id=asset_id, package_ids=package_ids, size=args.size
    )
    response = es_client.request("POST", "/" + args.index + "/_search", query)
    report = build_report(
        hbase_table=args.hbase_table,
        hbase_row_key=hbase_row_key,
        asset_id=asset_id,
        hbase_row=hbase_row,
        index=args.index,
        response=response,
        query=query,
        print_query=args.print_query,
    )
    print_and_optionally_save(report, args.output)
    return 0


if __name__ == "__main__":
    sys.exit(main())
