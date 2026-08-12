# -*- coding: utf-8 -*-
"""
merge_es_fields_to_hbase.py — 将 ES 指定字段合并写回 HBase 行

【用途】
  扫描 HBase 表中的 assetId，批量在 ES 中按 assert_id 查询，
  将 program_id 等字段合并（多值用 | 连接）后写回 HBase 对应 row key。
  支持 dry-run 预览、CSV 报告、分批 ES 查询与 HBase batch put。

【依赖】
  - happybase：HBase 读写
  - es_lookup_program_id：EsClient、密码加载
  - csv、json 标准库

【典型命令行用法】
  # 默认合并 program_id 到 HBase
  python merge_es_fields_to_hbase.py

  # 只处理单行，dry-run 不写库
  python merge_es_fields_to_hbase.py --row-key 1234567890 --dry-run

  # 合并多个字段并导出 CSV
  python merge_es_fields_to_hbase.py --merge-fields program_id,name --output-csv merge.csv --limit 1000
"""

import argparse
import csv
import getpass
import json
import os
import sys
import time
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


DEFAULT_HBASE_TABLE = "recommend:video_search_recomm_poms_type1_embedding_multimode"
DEFAULT_HBASE_HOSTS = (
    "h1003.dm.migu.cn,h1004.dm.migu.cn,h1005.dm.migu.cn,h1006.dm.migu.cn,"
    "h1007.dm.migu.cn,h1008.dm.migu.cn,h1009.dm.migu.cn,h1010.dm.migu.cn,"
    "h1011.dm.migu.cn,h1012.dm.migu.cn"
)


def to_text(value: Any) -> Optional[str]:
    """值转 UTF-8 字符串。"""
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


def now_text() -> str:
    """返回当前时间的格式化字符串（本模块预留，供扩展 operate_time 等）。"""
    return time.strftime("%Y-%m-%d %H:%M:%S")


def connect_hbase(hosts: Iterable[str], port: int, timeout_ms: int):
    """连接 HBase Thrift，多主机故障转移。"""
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
    """解码 HBase 行为 qualifier -> 字符串。"""
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


def get_one_hbase_row(
    connection: Any, table_name: str, column_family: str, row_key: str
) -> Tuple[str, str]:
    """
    读取指定 row key 的 assetId。

    参数:
        connection: HBase 连接
        table_name: 表名
        column_family: 列族
        row_key: 行键
    返回:
        (row_key, asset_id)
    """
    table = connection.table(table_name)
    asset_column = to_bytes("%s:assetId" % column_family)
    row = table.row(to_bytes(row_key), columns=[asset_column])
    decoded = decode_hbase_row(row, column_family)
    asset_id = decoded.get("assetId") or row_key
    if not is_not_blank(asset_id):
        raise RuntimeError("HBase row %s has no assetId" % row_key)
    return str(row_key), str(asset_id)


def iter_hbase_asset_ids(
    connection: Any,
    table_name: str,
    column_family: str,
    scan_batch_size: int,
    limit: int,
) -> Iterable[Tuple[str, str]]:
    """
    扫描 HBase 表，yield (row_key, asset_id)。

    参数:
        connection: HBase 连接
        table_name: 表名
        column_family: 列族
        scan_batch_size: scan 批大小
        limit: 最大行数，0 不限制
    生成:
        (row_key_text, asset_id) 元组
    """
    table = connection.table(table_name)
    asset_column = to_bytes("%s:assetId" % column_family)
    scanned = 0
    for row_key, row in table.scan(columns=[asset_column], batch_size=scan_batch_size):
        row_key_text = to_text(row_key) or ""
        decoded = decode_hbase_row(row, column_family)
        asset_id = decoded.get("assetId") or row_key_text
        if not is_not_blank(asset_id):
            continue
        yield row_key_text, str(asset_id)
        scanned += 1
        if limit > 0 and scanned >= limit:
            break


def chunks(
    values: List[Tuple[str, str]], batch_size: int
) -> Iterable[List[Tuple[str, str]]]:
    """
    将 (row_key, asset_id) 列表按批大小切分。

    用于 ES 查询与 HBase 写入的分批处理。
    """
    for index in range(0, len(values), batch_size):
        yield values[index : index + batch_size]


def build_es_batch_query(
    asset_ids: List[str],
    package_ids: List[Any],
    source_fields: List[str],
    size: int,
) -> Dict[str, Any]:
    """
    构建批量 asset_id/assert_id ES 查询。

    参数:
        asset_ids: 一批 assetId 列表
        package_ids: package 过滤
        source_fields: 需要返回的 ES 字段
        size: 结果上限
    返回:
        _search 请求体
    """
    query: Dict[str, Any] = {
        "size": size,
        "_source": source_fields,
        "query": {
            "bool": {
                "must": [
                    {
                        "bool": {
                            "should": [
                                {"terms": {"assert_id": asset_ids}},
                                {"terms": {"assert_id.keyword": asset_ids}},
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


def query_es_by_asset_ids(
    client: EsClient,
    index: str,
    hbase_rows: List[Tuple[str, str]],
    package_ids: List[Any],
    merge_fields: List[str],
    size_per_asset: int,
) -> Dict[str, List[Mapping[str, Any]]]:
    """
    对一批 HBase assetId 执行 ES 批量查询。

    参数:
        client: ES 客户端
        index: 索引名
        hbase_rows: 本批 (row_key, asset_id)
        package_ids: package 过滤
        merge_fields: 需要 merge 的 ES 字段名
        size_per_asset: 每个 asset 预留 hit 数
    返回:
        asset_id -> _source 列表
    """
    asset_ids = [asset_id for _, asset_id in hbase_rows]
    source_fields = sorted(set(["assert_id"] + merge_fields))
    size = max(1, min(10000, len(asset_ids) * max(1, size_per_asset)))
    query = build_es_batch_query(asset_ids, package_ids, source_fields, size)
    response = client.request("POST", "/" + index + "/_search", query)
    hits = response.get("hits", {}).get("hits", [])
    result: Dict[str, List[Mapping[str, Any]]] = {}
    for hit in hits:
        source = hit.get("_source", {})
        if not isinstance(source, Mapping):
            continue
        assert_id = source.get("assert_id")
        if not is_not_blank(assert_id):
            continue
        result.setdefault(str(assert_id), []).append(source)
    return result


def merged_values(
    hits: Iterable[Mapping[str, Any]], merge_fields: Iterable[str]
) -> Dict[str, str]:
    """
    将多条 ES hit 的指定字段去重排序后用 | 合并为 HBase 列值。

    参数:
        hits: 同一 assert_id 的多条 _source
        merge_fields: 要写入 HBase 的字段名列表
    返回:
        field -> 合并后的字符串
    """
    document: Dict[str, str] = {}
    for field in merge_fields:
        values = sorted(
            {str(hit.get(field)) for hit in hits if is_not_blank(hit.get(field))}
        )
        if values:
            document[field] = "|".join(values)
    return document


def put_hbase_rows(
    connection: Any,
    table_name: str,
    column_family: str,
    rows: Mapping[str, Mapping[str, Any]],
    batch_size: int,
) -> None:
    """
    批量写入 HBase 行（happybase batch put）。

    参数:
        connection: HBase 连接
        table_name: 表名
        column_family: 列族
        rows: row_key -> {qualifier: value} 待写字典
        batch_size: batch 大小
    """
    if not rows:
        return
    table = connection.table(table_name)
    # HBase 批量写入：使用 table.batch 上下文管理器
    with table.batch(batch_size=batch_size) as batch:
        for row_key, document in rows.items():
            columns = {
                to_bytes("%s:%s" % (column_family, key)): to_bytes(value)
                for key, value in document.items()
                if is_not_blank(value)
            }
            if columns:
                batch.put(to_bytes(row_key), columns)


def load_es_password(
    username: str, password_arg: Optional[str], use_es_manager: bool
) -> str:
    """ES 密码加载链。"""
    password = password_arg
    if password is None and use_es_manager:
        password = load_password_from_es_manager(username)
    if password is None:
        password = os.environ.get("ES_PASSWORD")
    if password is None:
        password = getpass.getpass("ES password: ")
    return password


def write_csv_report(
    path: str, rows: Iterable[Mapping[str, Any]], merge_fields: List[str]
) -> None:
    """
    将 merge 过程每行结果写入 CSV。

    列：hbase_row_key, assetId, es_hit_count, write_status, 及各 merge 字段。
    """
    with open(path, "w", encoding="utf-8", newline="") as fp:
        writer = csv.writer(fp)
        writer.writerow(
            ["hbase_row_key", "assetId", "es_hit_count", "write_status"] + merge_fields
        )
        for row in rows:
            writer.writerow(
                [
                    row.get("hbase_row_key"),
                    row.get("assetId"),
                    row.get("es_hit_count"),
                    row.get("write_status"),
                ]
                + [row.get(field, "") for field in merge_fields]
            )
    print("Wrote CSV report to %s" % path)


def main(argv: Optional[Iterable[str]] = None) -> int:
    """
    主流程：HBase scan → 分批 ES 查询 → 合并字段 → batch 写 HBase。

    返回:
        0 成功
    """
    parser = argparse.ArgumentParser(
        description="Merge selected ES fields into HBase rows by HBase assetId -> ES assert_id."
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
    parser.add_argument("--row-key", help="Only merge one HBase row key / assetId.")
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Max HBase rows to scan. Default 0 means all rows.",
    )
    parser.add_argument(
        "--hbase-scan-batch-size", type=int, default=500, help="HBase scan batch size."
    )
    parser.add_argument(
        "--hbase-write-batch-size",
        type=int,
        default=100,
        help="HBase write batch size.",
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
    parser.add_argument(
        "--merge-fields",
        default="program_id",
        help="Comma-separated ES _source fields to write into HBase.",
    )
    parser.add_argument(
        "--es-batch-size",
        type=int,
        default=100,
        help="Number of assetIds per ES query.",
    )
    parser.add_argument(
        "--size-per-asset", type=int, default=5, help="ES result budget per assetId."
    )
    parser.add_argument(
        "--timeout", type=float, default=10.0, help="ES request timeout seconds."
    )
    parser.add_argument(
        "--verify-cert", action="store_true", help="Verify ES HTTPS certificate."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not write HBase; only print/report what would be written.",
    )
    parser.add_argument("--output-csv", help="Write merge report CSV.")
    args = parser.parse_args(argv)

    merge_fields = parse_csv(args.merge_fields)
    if not merge_fields:
        raise ValueError("--merge-fields cannot be empty")

    hbase_connection = connect_hbase(
        hosts=parse_csv(args.hbase_hosts),
        port=args.hbase_port,
        timeout_ms=args.hbase_timeout_ms,
    )
    try:
        # 收集待处理 HBase 行
        if args.row_key:
            hbase_rows = [
                get_one_hbase_row(
                    connection=hbase_connection,
                    table_name=args.hbase_table,
                    column_family=args.column_family,
                    row_key=args.row_key,
                )
            ]
        else:
            hbase_rows = list(
                iter_hbase_asset_ids(
                    connection=hbase_connection,
                    table_name=args.hbase_table,
                    column_family=args.column_family,
                    scan_batch_size=args.hbase_scan_batch_size,
                    limit=args.limit,
                )
            )

        password = load_es_password(
            args.username, args.password, not args.no_es_manager
        )
        es_client = EsClient(
            nodes=parse_csv(args.nodes),
            scheme=args.scheme,
            username=args.username,
            password=password,
            timeout=args.timeout,
            verify_cert=args.verify_cert,
        )

        report_rows: List[Dict[str, Any]] = []
        rows_to_write: Dict[str, Dict[str, Any]] = {}
        scanned = 0
        matched = 0
        written = 0
        missing = 0

        # 按 es_batch_size 分批：查 ES → 合并 → 写 HBase
        for batch_index, hbase_batch in enumerate(
            chunks(hbase_rows, args.es_batch_size), 1
        ):
            es_hits_by_asset_id = query_es_by_asset_ids(
                client=es_client,
                index=args.index,
                hbase_rows=hbase_batch,
                package_ids=parse_package_ids(args.package_ids),
                merge_fields=merge_fields,
                size_per_asset=args.size_per_asset,
            )

            for row_key, asset_id in hbase_batch:
                scanned += 1
                hits = es_hits_by_asset_id.get(asset_id, [])
                document = merged_values(hits, merge_fields)
                report_row: Dict[str, Any] = {
                    "hbase_row_key": row_key,
                    "assetId": asset_id,
                    "es_hit_count": len(hits),
                    "write_status": "dry_run"
                    if args.dry_run and document
                    else "not_found",
                }
                report_row.update(document)

                if hits:
                    matched += 1
                else:
                    missing += 1

                if document:
                    rows_to_write[row_key] = document
                    report_row["write_status"] = (
                        "dry_run" if args.dry_run else "written"
                    )
                report_rows.append(report_row)

            # 非 dry-run 时批量写 HBase，写完清空缓冲
            if rows_to_write and not args.dry_run:
                put_hbase_rows(
                    connection=hbase_connection,
                    table_name=args.hbase_table,
                    column_family=args.column_family,
                    rows=rows_to_write,
                    batch_size=args.hbase_write_batch_size,
                )
                written += len(rows_to_write)
                rows_to_write = {}

            print(
                "Processed batch=%s scanned=%s es_matched=%s missing_in_es=%s written=%s"
                % (batch_index, scanned, matched, missing, written),
                file=sys.stderr,
                flush=True,
            )
    finally:
        hbase_connection.close()

    print("=== ES -> HBase merge summary ===")
    print("hbase_rows=%s" % scanned)
    print("es_matched_rows=%s" % matched)
    print("missing_in_es=%s" % missing)
    print("hbase_rows_written=%s" % (0 if args.dry_run else written))
    print("merge_fields=%s" % ",".join(merge_fields))
    if args.output_csv:
        write_csv_report(args.output_csv, report_rows, merge_fields)
    return 0


if __name__ == "__main__":
    sys.exit(main())
