import argparse
import base64
import getpass
import importlib
import json
import os
import re
import ssl
import sys
from typing import Any, Dict, Iterable, List, Mapping, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen


DEFAULT_NODES = (
    "10.194.9.2:29201,10.194.9.3:29201,10.194.9.4:29201,"
    "10.194.9.5:29201,10.194.9.6:29201,10.194.9.2:29202,"
    "10.194.9.3:29202,10.194.9.4:29202,10.194.9.5:29202,10.194.9.6:29202"
)
DEFAULT_INDEX = "video_poms_ondemand_content_detail"
DEFAULT_SOURCE_FIELDS = [
    "assert_id",
    "program_id",
    "product_info_package_id",
    "name",
]
MAPPING_FIELDS_TO_PRINT = {
    "assert_id",
    "program_id",
    "product_info_package_id",
}


def parse_csv(value: str) -> List[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def parse_package_ids(value: str) -> List[Any]:
    values: List[Any] = []
    for item in parse_csv(value):
        try:
            values.append(int(item))
        except ValueError:
            values.append(item)
    return values


def basic_auth_header(username: str, password: str) -> str:
    token = base64.b64encode(("%s:%s" % (username, password)).encode("utf-8")).decode(
        "ascii"
    )
    return "Basic " + token


def is_not_blank(value: Any) -> bool:
    return value is not None and str(value).strip() != ""


def extract_password(value: Any) -> Optional[str]:
    if not is_not_blank(value):
        return None
    if isinstance(value, Mapping):
        for key in ("password", "es_password", "passwd", "pwd"):
            password = extract_password(value.get(key))
            if password:
                return password
        return None
    password = str(value).strip()
    if password.startswith("ENC("):
        return None
    return password


def try_call_password_func(func: Any, username: str) -> Optional[str]:
    for args in ((), (username,), ("elastic",)):
        try:
            password = extract_password(func(*args))
        except TypeError:
            continue
        except Exception as exc:
            print("EsManager password function failed: %s" % exc, file=sys.stderr)
            continue
        if password:
            return password
    return None


def try_get_password_from_object(obj: Any, username: str) -> Optional[str]:
    for attr in (
        "password",
        "passwd",
        "pwd",
        "es_password",
        "ES_PASSWORD",
        "es_pwd",
    ):
        password = extract_password(getattr(obj, attr, None))
        if password:
            return password

    for method_name in (
        "get_password",
        "getPassword",
        "get_es_password",
        "getEsPassword",
        "parse_password",
        "parsePassword",
        "decrypt_password",
        "decryptPassword",
    ):
        func = getattr(obj, method_name, None)
        if callable(func):
            password = try_call_password_func(func, username)
            if password:
                return password
    return None


def load_password_from_es_manager_source() -> Optional[str]:
    es_manager_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "EsManager.py"
    )
    if not os.path.exists(es_manager_path):
        return None

    with open(es_manager_path, "r", encoding="utf-8") as fp:
        source = fp.read()

    secret_match = re.search(r"SECRET\s*=\s*['\"]([^'\"]+)['\"]", source)
    password_match = re.search(r"PASSWORD\s*=\s*['\"]([^'\"]+)['\"]", source)
    if not secret_match or not password_match:
        return None

    try:
        crypt_common = importlib.import_module("CryptCommon")
    except Exception as exc:
        print(
            "Found EsManager.py password config, but could not import CryptCommon.py: %s. "
            "Please put CryptCommon.py in the same directory as es_lookup_program_id.py."
            % exc,
            file=sys.stderr,
        )
        return None

    try:
        secret_key = crypt_common.base64_decode(secret_match.group(1))
        password = crypt_common.decrypt(password_match.group(1), secret_key)
    except Exception as exc:
        print("Could not decrypt password from EsManager.py: %s" % exc, file=sys.stderr)
        return None
    return extract_password(password)


def load_password_from_es_manager(username: str) -> Optional[str]:
    password = load_password_from_es_manager_source()
    if password:
        return password

    try:
        es_manager_module = importlib.import_module("EsManager")
    except Exception as exc:
        print("Could not import EsManager.py: %s" % exc, file=sys.stderr)
        return None

    password = try_get_password_from_object(es_manager_module, username)
    if password:
        return password

    for factory_name in (
        "get_es_manager",
        "getEsManager",
        "create_es_manager",
        "createEsManager",
        "get_manager",
    ):
        factory = getattr(es_manager_module, factory_name, None)
        if callable(factory):
            manager = None
            for args in ((), (username,), ("elastic",)):
                try:
                    manager = factory(*args)
                    break
                except TypeError:
                    continue
                except Exception as exc:
                    print(
                        "EsManager factory %s failed: %s" % (factory_name, exc),
                        file=sys.stderr,
                    )
                    break
            password = try_get_password_from_object(manager, username)
            if password:
                return password

    for class_name in (
        "EsManager",
        "ESManager",
        "ElasticSearchManager",
        "ElasticsearchManager",
    ):
        cls = getattr(es_manager_module, class_name, None)
        if cls is None:
            continue
        manager = None
        for args in ((), (username,), ("elastic",)):
            try:
                manager = cls(*args)
                break
            except TypeError:
                continue
            except Exception as exc:
                print(
                    "EsManager class %s init failed: %s" % (class_name, exc),
                    file=sys.stderr,
                )
                break
        password = try_get_password_from_object(manager, username)
        if password:
            return password

    return None


class EsClient:
    def __init__(
        self,
        nodes: Iterable[str],
        scheme: str,
        username: str,
        password: str,
        timeout: float,
        verify_cert: bool,
    ):
        self.nodes = [node.strip() for node in nodes if node.strip()]
        self.scheme = scheme
        self.timeout = timeout
        self.auth_header = basic_auth_header(username, password)
        self.ssl_context = None if verify_cert else ssl._create_unverified_context()

    def request(
        self, method: str, path: str, body: Optional[Mapping[str, Any]] = None
    ) -> Mapping[str, Any]:
        if not self.nodes:
            raise RuntimeError("No ES nodes configured")

        data = None
        if body is not None:
            data = json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode(
                "utf-8"
            )

        last_error = None
        for node in self.nodes:
            url = "%s://%s%s" % (self.scheme, node, path)
            print(
                "Requesting ES %s %s via %s" % (method, path, node),
                file=sys.stderr,
                flush=True,
            )
            request = Request(
                url,
                data=data,
                method=method,
                headers={
                    "Authorization": self.auth_header,
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
            )
            try:
                with urlopen(
                    request, timeout=self.timeout, context=self.ssl_context
                ) as response:
                    raw = response.read().decode("utf-8")
                    print(
                        "ES request succeeded via %s" % node,
                        file=sys.stderr,
                        flush=True,
                    )
                    return json.loads(raw) if raw else {}
            except HTTPError as exc:
                detail = exc.read().decode("utf-8", errors="replace")
                raise RuntimeError(
                    "ES HTTP error %s from %s: %s" % (exc.code, url, detail)
                )
            except URLError as exc:
                last_error = exc
                print(
                    "Could not connect to ES node %s: %s" % (node, exc), file=sys.stderr
                )

        raise RuntimeError("Could not connect to any ES node") from last_error


def build_lookup_query(
    assert_id: str, package_ids: List[Any], source_fields: List[str], size: int
) -> Dict[str, Any]:
    package_id_texts = [str(item) for item in package_ids]
    return {
        "size": size,
        "_source": source_fields,
        "query": {
            "bool": {
                "must": [
                    {
                        "bool": {
                            "should": [
                                {"term": {"assert_id": assert_id}},
                                {"term": {"assert_id.keyword": assert_id}},
                            ],
                            "minimum_should_match": 1,
                        }
                    }
                ],
                "filter": [
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
                ],
            }
        },
    }


def walk_mapping_properties(
    properties: Mapping[str, Any], prefix: str = ""
) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    for name, info in properties.items():
        path = "%s.%s" % (prefix, name) if prefix else name
        if not isinstance(info, Mapping):
            continue
        result[path] = info
        nested = info.get("properties")
        if isinstance(nested, Mapping):
            result.update(walk_mapping_properties(nested, path))
    return result


def extract_mapping_properties(mappings: Mapping[str, Any]) -> Mapping[str, Any]:
    properties = mappings.get("properties", {})
    if isinstance(properties, Mapping):
        return properties

    for mapping_type in mappings.values():
        if not isinstance(mapping_type, Mapping):
            continue
        properties = mapping_type.get("properties", {})
        if isinstance(properties, Mapping):
            return properties
    return {}


def print_relevant_mapping(mapping_response: Mapping[str, Any]) -> None:
    print("=== relevant mapping fields ===")
    found = False
    for index_name, index_mapping in mapping_response.items():
        mappings = (
            index_mapping.get("mappings", {})
            if isinstance(index_mapping, Mapping)
            else {}
        )
        properties = (
            extract_mapping_properties(mappings)
            if isinstance(mappings, Mapping)
            else {}
        )
        flattened = walk_mapping_properties(properties)
        for path, info in sorted(flattened.items()):
            leaf_name = path.split(".")[-1]
            if leaf_name in MAPPING_FIELDS_TO_PRINT:
                found = True
                brief = {
                    key: info.get(key) for key in ("type", "fields") if key in info
                }
                print(
                    "%s.%s: %s"
                    % (index_name, path, json.dumps(brief, ensure_ascii=False))
                )
    if not found:
        print("No relevant mapping fields found in mapping response.")


def print_hits(response: Mapping[str, Any]) -> None:
    hits_block = response.get("hits", {})
    total = hits_block.get("total", 0)
    if isinstance(total, Mapping):
        total_value = total.get("value", 0)
    else:
        total_value = total
    hits = hits_block.get("hits", [])
    print("=== search result ===")
    print("total=%s returned=%s" % (total_value, len(hits)))
    if not hits:
        print("No hit. Check assert_id, product_info_package_id, or field mapping.")
        return

    program_ids = []
    for index, hit in enumerate(hits, 1):
        source = hit.get("_source", {})
        program_id = source.get("program_id")
        if program_id is not None:
            program_ids.append(str(program_id))
        print("--- hit %s ---" % index)
        print("_id=%s _score=%s" % (hit.get("_id"), hit.get("_score")))
        print(json.dumps(source, ensure_ascii=False, indent=2, sort_keys=True))

    if program_ids:
        print("=== program_id list ===")
        for program_id in program_ids:
            print(program_id)


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Lookup program_id from ES by assert_id."
    )
    parser.add_argument("--assert-id", required=True, help="assert_id value to query.")
    parser.add_argument("--index", default=DEFAULT_INDEX, help="ES index name.")
    parser.add_argument(
        "--nodes", default=DEFAULT_NODES, help="Comma-separated ES host:port nodes."
    )
    parser.add_argument("--scheme", default="https", help="ES scheme, usually https.")
    parser.add_argument("--username", default="elastic", help="ES username.")
    parser.add_argument(
        "--password",
        help="ES password. If omitted, EsManager.py, ES_PASSWORD, or prompt is used.",
    )
    parser.add_argument(
        "--no-es-manager",
        action="store_true",
        help="Do not try to load password from EsManager.py.",
    )
    parser.add_argument(
        "--package-ids", default="1,141", help="Allowed product_info_package_id values."
    )
    parser.add_argument(
        "--source-fields",
        default=",".join(DEFAULT_SOURCE_FIELDS),
        help="Comma-separated _source fields.",
    )
    parser.add_argument("--size", type=int, default=5, help="Max hits to return.")
    parser.add_argument(
        "--timeout", type=float, default=10.0, help="Request timeout seconds."
    )
    parser.add_argument(
        "--verify-cert", action="store_true", help="Verify HTTPS certificate."
    )
    parser.add_argument(
        "--show-mapping",
        action="store_true",
        help="Print mapping info for key fields before querying.",
    )
    parser.add_argument(
        "--print-query", action="store_true", help="Print ES query JSON."
    )
    args = parser.parse_args(argv)

    password = args.password
    if password is None and not args.no_es_manager:
        password = load_password_from_es_manager(args.username)
    if password is None:
        import os

        password = os.environ.get("ES_PASSWORD")
    if password is None:
        password = getpass.getpass("ES password: ")

    client = EsClient(
        nodes=parse_csv(args.nodes),
        scheme=args.scheme,
        username=args.username,
        password=password,
        timeout=args.timeout,
        verify_cert=args.verify_cert,
    )

    index_path = "/" + quote(args.index, safe="")
    if args.show_mapping:
        mapping = client.request("GET", index_path + "/_mapping")
        print_relevant_mapping(mapping)

    query = build_lookup_query(
        assert_id=str(args.assert_id),
        package_ids=parse_package_ids(args.package_ids),
        source_fields=parse_csv(args.source_fields),
        size=args.size,
    )
    if args.print_query:
        print("=== query ===")
        print(json.dumps(query, ensure_ascii=False, indent=2, sort_keys=True))

    response = client.request("POST", index_path + "/_search", query)
    print_hits(response)
    return 0


if __name__ == "__main__":
    sys.exit(main())
