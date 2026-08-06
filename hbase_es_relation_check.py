import argparse
import csv
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


DEFAULT_HBASE_TABLE = "recommend:video_search_recomm_poms_embedding_test"
DEFAULT_HBASE_HOSTS = (
    "h1003.dm.migu.cn,h1004.dm.migu.cn,h1005.dm.migu.cn,h1006.dm.migu.cn,"
    "h1007.dm.migu.cn,h1008.dm.migu.cn,h1009.dm.migu.cn,h1010.dm.migu.cn,"
    "h1011.dm.migu.cn,h1012.dm.migu.cn"
)
ES_SOURCE_FIELDS = [
    "assert_id",
    "program_id",
    "product_info_package_id",
    "name",
]


def to_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def is_not_blank(value: Any) -> bool:
    return value is not None and str(value).strip() != ""


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


def iter_hbase_assert_ids(
    connection,
    table_name: str,
    column_family: str,
    scan_batch_size: int,
    limit: int,
) -> Iterable[Tuple[str, str]]:
    table = connection.table(table_name)
    asset_column = ("%s:assetId" % column_family).encode("utf-8")
    assert_column = ("%s:assert_id" % column_family).encode("utf-8")
    scanned = 0

    for row_key, row in table.scan(
        columns=[asset_column, assert_column], batch_size=scan_batch_size
    ):
        row_key_text = to_text(row_key)
        assert_id = (
            to_text(row.get(asset_column))
            or to_text(row.get(assert_column))
            or row_key_text
        )
        if not is_not_blank(assert_id):
            continue
        yield row_key_text or str(assert_id), str(assert_id)
        scanned += 1
        if limit > 0 and scanned >= limit:
            break


def build_es_batch_query(
    assert_ids: List[str], package_ids: List[Any], size: int
) -> Dict[str, Any]:
    package_id_texts = [str(item) for item in package_ids]
    query: Dict[str, Any] = {
        "size": size,
        "_source": ES_SOURCE_FIELDS,
        "query": {
            "bool": {
                "must": [
                    {
                        "bool": {
                            "should": [
                                {"terms": {"assert_id": assert_ids}},
                                {"terms": {"assert_id.keyword": assert_ids}},
                            ],
                            "minimum_should_match": 1,
                        }
                    }
                ],
            }
        },
    }
    if package_ids:
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


def chunks(values: List[str], batch_size: int) -> Iterable[List[str]]:
    for index in range(0, len(values), batch_size):
        yield values[index : index + batch_size]


def query_es_relationships(
    client: EsClient,
    index: str,
    assert_ids: List[str],
    package_ids: List[Any],
    es_batch_size: int,
    max_hits_per_assert: int,
    label: str = "filtered",
) -> Dict[str, List[Mapping[str, Any]]]:
    index_path = "/" + index
    result: Dict[str, List[Mapping[str, Any]]] = {}
    total_batches = 0

    for batch in chunks(assert_ids, es_batch_size):
        total_batches += 1
        size = max(1, min(10000, len(batch) * max_hits_per_assert))
        query = build_es_batch_query(batch, package_ids, size)
        response = client.request("POST", index_path + "/_search", query)
        hits = response.get("hits", {}).get("hits", [])
        for hit in hits:
            source = hit.get("_source", {})
            if not isinstance(source, Mapping):
                continue
            assert_id = source.get("assert_id")
            if not is_not_blank(assert_id):
                continue
            result.setdefault(str(assert_id), []).append(source)
        print(
            "Queried ES %s batch %s, assert_ids=%s, hits=%s"
            % (label, total_batches, len(batch), len(hits)),
            file=sys.stderr,
            flush=True,
        )

    return result


def summarize_relationship(
    hbase_rows: List[Tuple[str, str]],
    es_hits_by_assert_id: Mapping[str, List[Mapping[str, Any]]],
    missing_hits_without_package_filter: Mapping[str, List[Mapping[str, Any]]],
    example_limit: int,
) -> Dict[str, Any]:
    hbase_assert_ids = [assert_id for _, assert_id in hbase_rows]
    unique_hbase_assert_ids = sorted(set(hbase_assert_ids))
    assert_to_programs: Dict[str, set] = {}
    program_to_asserts: Dict[str, set] = {}

    for assert_id, hits in es_hits_by_assert_id.items():
        for source in hits:
            program_id = source.get("program_id")
            if not is_not_blank(program_id):
                continue
            program_id_text = str(program_id)
            assert_to_programs.setdefault(assert_id, set()).add(program_id_text)
            program_to_asserts.setdefault(program_id_text, set()).add(assert_id)

    missing_in_es = [
        assert_id
        for assert_id in unique_hbase_assert_ids
        if assert_id not in es_hits_by_assert_id
    ]
    missing_found_without_package_filter = [
        assert_id
        for assert_id in missing_in_es
        if missing_hits_without_package_filter.get(assert_id)
    ]
    missing_still_not_found = [
        assert_id
        for assert_id in missing_in_es
        if not missing_hits_without_package_filter.get(assert_id)
    ]
    multi_program_asserts = {
        assert_id: programs
        for assert_id, programs in assert_to_programs.items()
        if len(programs) > 1
    }
    multi_assert_programs = {
        program_id: assert_ids
        for program_id, assert_ids in program_to_asserts.items()
        if len(assert_ids) > 1
    }

    print("=== HBase -> ES relationship summary ===")
    print("hbase_rows=%s" % len(hbase_rows))
    print("hbase_unique_assert_id=%s" % len(unique_hbase_assert_ids))
    print("es_matched_assert_id=%s" % len(es_hits_by_assert_id))
    print("hbase_assert_id_missing_in_es=%s" % len(missing_in_es))
    print(
        "missing_found_without_package_filter=%s"
        % len(missing_found_without_package_filter)
    )
    print(
        "missing_still_not_found_without_package_filter=%s"
        % len(missing_still_not_found)
    )
    print("assert_id_with_multiple_program_id=%s" % len(multi_program_asserts))
    print("program_id_with_multiple_assert_id=%s" % len(multi_assert_programs))

    if missing_in_es:
        print("=== examples: HBase assert_id missing in ES ===")
        for assert_id in missing_in_es[:example_limit]:
            hits = missing_hits_without_package_filter.get(assert_id, [])
            package_ids = sorted(
                {
                    str(hit.get("product_info_package_id"))
                    for hit in hits
                    if is_not_blank(hit.get("product_info_package_id"))
                }
            )
            program_ids = sorted(
                {
                    str(hit.get("program_id"))
                    for hit in hits
                    if is_not_blank(hit.get("program_id"))
                }
            )
            if package_ids or program_ids:
                print(
                    "%s package_ids=%s program_ids=%s"
                    % (assert_id, "|".join(package_ids), "|".join(program_ids))
                )
            else:
                print("%s package_ids=<not_found>" % assert_id)

    if multi_program_asserts:
        print("=== examples: one HBase assert_id -> multiple ES program_id ===")
        for index, (assert_id, programs) in enumerate(
            sorted(multi_program_asserts.items()), 1
        ):
            if index > example_limit:
                break
            print("%s -> %s" % (assert_id, ",".join(sorted(programs))))

    if multi_assert_programs:
        print("=== examples: one ES program_id -> multiple HBase assert_id ===")
        for index, (program_id, assert_ids) in enumerate(
            sorted(multi_assert_programs.items()), 1
        ):
            if index > example_limit:
                break
            print("%s -> %s" % (program_id, ",".join(sorted(assert_ids))))

    return {
        "hbase_rows": hbase_rows,
        "missing_in_es": missing_in_es,
        "missing_found_without_package_filter": missing_found_without_package_filter,
        "missing_still_not_found": missing_still_not_found,
        "assert_to_programs": assert_to_programs,
        "program_to_asserts": program_to_asserts,
    }


def write_csv_report(
    path: str,
    hbase_rows: List[Tuple[str, str]],
    es_hits_by_assert_id: Mapping[str, List[Mapping[str, Any]]],
    missing_hits_without_package_filter: Mapping[str, List[Mapping[str, Any]]],
) -> None:
    with open(path, "w", encoding="utf-8", newline="") as fp:
        writer = csv.writer(fp)
        writer.writerow(
            [
                "hbase_row_key",
                "assert_id",
                "program_ids",
                "program_count",
                "es_hit_count",
                "product_info_package_ids",
                "names",
                "missing_without_package_filter_hit_count",
                "missing_without_package_filter_product_info_package_ids",
                "missing_without_package_filter_program_ids",
                "missing_without_package_filter_names",
            ]
        )
        for row_key, assert_id in hbase_rows:
            hits = es_hits_by_assert_id.get(assert_id, [])
            missing_hits = (
                [] if hits else missing_hits_without_package_filter.get(assert_id, [])
            )
            program_ids = sorted(
                {
                    str(hit.get("program_id"))
                    for hit in hits
                    if is_not_blank(hit.get("program_id"))
                }
            )
            package_ids = sorted(
                {
                    str(hit.get("product_info_package_id"))
                    for hit in hits
                    if is_not_blank(hit.get("product_info_package_id"))
                }
            )
            names = sorted(
                {str(hit.get("name")) for hit in hits if is_not_blank(hit.get("name"))}
            )
            missing_program_ids = sorted(
                {
                    str(hit.get("program_id"))
                    for hit in missing_hits
                    if is_not_blank(hit.get("program_id"))
                }
            )
            missing_package_ids = sorted(
                {
                    str(hit.get("product_info_package_id"))
                    for hit in missing_hits
                    if is_not_blank(hit.get("product_info_package_id"))
                }
            )
            missing_names = sorted(
                {
                    str(hit.get("name"))
                    for hit in missing_hits
                    if is_not_blank(hit.get("name"))
                }
            )
            writer.writerow(
                [
                    row_key,
                    assert_id,
                    "|".join(program_ids),
                    len(program_ids),
                    len(hits),
                    "|".join(package_ids),
                    "|".join(names),
                    len(missing_hits),
                    "|".join(missing_package_ids),
                    "|".join(missing_program_ids),
                    "|".join(missing_names),
                ]
            )
    print("Wrote CSV report to %s" % path)


def load_es_password(
    username: str, password_arg: Optional[str], use_es_manager: bool
) -> str:
    password = password_arg
    if password is None and use_es_manager:
        password = load_password_from_es_manager(username)
    if password is None:
        password = os.environ.get("ES_PASSWORD")
    if password is None:
        password = getpass.getpass("ES password: ")
    return password


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Check HBase assetId/assert_id relationships against ES program_id."
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
        "--hbase-scan-batch-size", type=int, default=500, help="HBase scan batch size."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20000,
        help="Max HBase rows to check. Use 0 for all rows.",
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
        "--package-ids", default="1,141", help="Allowed product_info_package_id values."
    )
    parser.add_argument(
        "--es-batch-size",
        type=int,
        default=100,
        help="Number of assert_ids per ES query.",
    )
    parser.add_argument(
        "--max-hits-per-assert",
        type=int,
        default=20,
        help="ES result size budget per assert_id.",
    )
    parser.add_argument(
        "--timeout", type=float, default=10.0, help="ES request timeout seconds."
    )
    parser.add_argument(
        "--verify-cert", action="store_true", help="Verify ES HTTPS certificate."
    )
    parser.add_argument(
        "--example-limit", type=int, default=20, help="Max examples to print."
    )
    parser.add_argument(
        "--no-diagnose-missing",
        action="store_true",
        help="Do not re-query missing assert_id values without product_info_package_id filter.",
    )
    parser.add_argument(
        "--output-csv", help="Write detailed relationship report to this CSV file."
    )
    args = parser.parse_args(argv)

    hbase_connection = connect_hbase(
        hosts=parse_csv(args.hbase_hosts),
        port=args.hbase_port,
        timeout_ms=args.hbase_timeout_ms,
    )
    try:
        hbase_rows = list(
            iter_hbase_assert_ids(
                connection=hbase_connection,
                table_name=args.hbase_table,
                column_family=args.column_family,
                scan_batch_size=args.hbase_scan_batch_size,
                limit=args.limit,
            )
        )
    finally:
        hbase_connection.close()

    print(
        "Loaded %s HBase rows from %s" % (len(hbase_rows), args.hbase_table),
        file=sys.stderr,
        flush=True,
    )
    assert_ids = sorted(
        {assert_id for _, assert_id in hbase_rows if is_not_blank(assert_id)}
    )
    if not assert_ids:
        print("No assert_id found in HBase table.")
        return 0

    password = load_es_password(args.username, args.password, not args.no_es_manager)
    es_client = EsClient(
        nodes=parse_csv(args.nodes),
        scheme=args.scheme,
        username=args.username,
        password=password,
        timeout=args.timeout,
        verify_cert=args.verify_cert,
    )
    es_hits_by_assert_id = query_es_relationships(
        client=es_client,
        index=args.index,
        assert_ids=assert_ids,
        package_ids=parse_package_ids(args.package_ids),
        es_batch_size=args.es_batch_size,
        max_hits_per_assert=args.max_hits_per_assert,
        label="package-filtered",
    )
    missing_assert_ids = sorted(
        {
            assert_id
            for _, assert_id in hbase_rows
            if assert_id not in es_hits_by_assert_id
        }
    )
    missing_hits_without_package_filter: Dict[str, List[Mapping[str, Any]]] = {}
    if missing_assert_ids and not args.no_diagnose_missing:
        print(
            "Diagnosing %s missing assert_ids without product_info_package_id filter"
            % len(missing_assert_ids),
            file=sys.stderr,
            flush=True,
        )
        missing_hits_without_package_filter = query_es_relationships(
            client=es_client,
            index=args.index,
            assert_ids=missing_assert_ids,
            package_ids=[],
            es_batch_size=args.es_batch_size,
            max_hits_per_assert=args.max_hits_per_assert,
            label="missing-no-package-filter",
        )

    summarize_relationship(
        hbase_rows,
        es_hits_by_assert_id,
        missing_hits_without_package_filter,
        args.example_limit,
    )

    if args.output_csv:
        write_csv_report(
            args.output_csv,
            hbase_rows,
            es_hits_by_assert_id,
            missing_hits_without_package_filter,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
