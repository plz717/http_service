# -*- coding: utf-8 -*-
"""
es_lookup_program_id.py — 通过 assert_id 从 Elasticsearch 查询 program_id

【用途】
  根据 HBase/业务侧的 assert_id（资产 ID），在 ES 索引中检索匹配的文档，
  输出 program_id、product_info_package_id、name 等字段，用于排查 HBase 与 ES 的关联关系。

【依赖】
  - Python 标准库：argparse, base64, getpass, json, ssl, urllib 等
  - 可选：同目录下的 EsManager.py、CryptCommon.py（用于自动加载 ES 密码）
  - 环境变量 ES_PASSWORD（备用密码来源）

【典型命令行用法】
  # 按 assert_id 查询，密码从 EsManager.py 自动加载
  python es_lookup_program_id.py --assert-id 1234567890

  # 指定 package_id 过滤、打印 ES 查询体、查看字段 mapping
  python es_lookup_program_id.py --assert-id 1234567890 --package-ids 1,141 --print-query --show-mapping

  # 手动指定密码，跳过 EsManager
  python es_lookup_program_id.py --assert-id 1234567890 --password xxx --no-es-manager
"""

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


# 默认 ES 集群节点列表（host:port，逗号分隔）
DEFAULT_NODES = (
    "10.194.9.2:29201,10.194.9.3:29201,10.194.9.4:29201,"
    "10.194.9.5:29201,10.194.9.6:29201,10.194.9.2:29202,"
    "10.194.9.3:29202,10.194.9.4:29202,10.194.9.5:29202,10.194.9.6:29202"
)
# 默认 ES 索引名（点播内容详情）
DEFAULT_INDEX = "video_poms_ondemand_content_detail"
# 默认返回的 _source 字段列表
DEFAULT_SOURCE_FIELDS = [
    "assert_id",
    "program_id",
    "product_info_package_id",
    "name",
]
# 打印 mapping 时需要展示的关键字段名
MAPPING_FIELDS_TO_PRINT = {
    "assert_id",
    "program_id",
    "product_info_package_id",
}


def parse_csv(value: str) -> List[str]:
    """
    将逗号分隔字符串解析为去空白后的字符串列表。

    参数:
        value: 逗号分隔的原始字符串
    返回:
        非空元素组成的列表
    """
    return [item.strip() for item in value.split(",") if item.strip()]


def parse_package_ids(value: str) -> List[Any]:
    """
    解析 product_info_package_id 列表，优先尝试转为整数。

    参数:
        value: 逗号分隔的 package_id 字符串，如 "1,141"
    返回:
        整数或原字符串组成的列表（兼容 ES terms 查询的数值/文本两种类型）
    """
    values: List[Any] = []
    for item in parse_csv(value):
        try:
            values.append(int(item))
        except ValueError:
            values.append(item)
    return values


def basic_auth_header(username: str, password: str) -> str:
    """
    生成 HTTP Basic 认证头。

    参数:
        username: ES 用户名
        password: ES 密码
    返回:
        形如 "Basic xxx" 的 Authorization 头值
    """
    token = base64.b64encode(("%s:%s" % (username, password)).encode("utf-8")).decode(
        "ascii"
    )
    return "Basic " + token


def is_not_blank(value: Any) -> bool:
    """判断值是否非 None 且去除空白后非空字符串。"""
    return value is not None and str(value).strip() != ""


def extract_password(value: Any) -> Optional[str]:
    """
    从多种来源提取明文密码。

    支持：字符串、嵌套 dict（password/es_password 等键）、
    忽略 ENC(...) 加密占位符。

    参数:
        value: 密码或包含密码的结构
    返回:
        明文密码，无法提取时返回 None
    """
    if not is_not_blank(value):
        return None
    if isinstance(value, Mapping):
        # 递归尝试常见密码字段名
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
    """
    以多种参数签名尝试调用密码获取函数。

    参数:
        func: 可调用对象（如 get_password）
        username: ES 用户名
    返回:
        成功解出的密码，否则 None
    """
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
    """
    从对象属性或方法中提取 ES 密码。

    依次尝试：password 类属性 → get_password 类方法。

    参数:
        obj: EsManager 模块/实例等
        username: ES 用户名
    返回:
        明文密码或 None
    """
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
    """
    直接解析 EsManager.py 源码中的 SECRET/PASSWORD 常量并解密。

    依赖同目录 CryptCommon.py 的 base64_decode 与 decrypt。

    返回:
        解密后的 ES 密码，失败返回 None
    """
    es_manager_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "EsManager.py"
    )
    if not os.path.exists(es_manager_path):
        return None

    with open(es_manager_path, "r", encoding="utf-8") as fp:
        source = fp.read()

    # 正则提取 SECRET 与 PASSWORD 配置
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
        # 使用 SECRET 解密 PASSWORD 密文
        secret_key = crypt_common.base64_decode(secret_match.group(1))
        password = crypt_common.decrypt(password_match.group(1), secret_key)
    except Exception as exc:
        print("Could not decrypt password from EsManager.py: %s" % exc, file=sys.stderr)
        return None
    return extract_password(password)


def load_password_from_es_manager(username: str) -> Optional[str]:
    """
    综合多种方式从 EsManager 加载 ES 密码。

    顺序：源码解密 → import EsManager 模块 → 工厂函数 → 管理类实例化。

    参数:
        username: ES 用户名（用于部分工厂/构造方法）
    返回:
        明文密码或 None
    """
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

    # 尝试工厂函数创建 manager 实例
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

    # 尝试直接实例化管理类
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
    """
    轻量级 ES HTTP 客户端（无 elasticsearch-py 依赖）。

    支持多节点故障转移、Basic 认证、可选 SSL 证书校验。
    """

    def __init__(
        self,
        nodes: Iterable[str],
        scheme: str,
        username: str,
        password: str,
        timeout: float,
        verify_cert: bool,
    ):
        """
        初始化 ES 客户端。

        参数:
            nodes: ES 节点 host:port 列表
            scheme: http 或 https
            username/password: 认证凭据
            timeout: 单次请求超时（秒）
            verify_cert: 是否校验 HTTPS 证书
        """
        self.nodes = [node.strip() for node in nodes if node.strip()]
        self.scheme = scheme
        self.timeout = timeout
        self.auth_header = basic_auth_header(username, password)
        # 不校验证书时使用未验证 SSL 上下文
        self.ssl_context = None if verify_cert else ssl._create_unverified_context()

    def request(
        self, method: str, path: str, body: Optional[Mapping[str, Any]] = None
    ) -> Mapping[str, Any]:
        """
        向 ES 发起 HTTP 请求，失败时依次尝试下一个节点。

        参数:
            method: HTTP 方法（GET/POST 等）
            path: 请求路径（含索引名，如 /index/_search）
            body: 可选 JSON 请求体
        返回:
            解析后的 JSON 响应 dict
        异常:
            RuntimeError: 所有节点均不可达或 HTTP 错误
        """
        if not self.nodes:
            raise RuntimeError("No ES nodes configured")

        data = None
        if body is not None:
            data = json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode(
                "utf-8"
            )

        last_error = None
        # 多节点轮询：任一节点成功即返回
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
    """
    构建按 assert_id + product_info_package_id 过滤的 ES 查询 DSL。

    同时匹配 text 与 .keyword 子字段，兼容不同 mapping 类型。

    参数:
        assert_id: 资产 ID
        package_ids: 允许的 package_id 列表
        source_fields: 返回的 _source 字段
        size: 最大命中数
    返回:
        ES _search 请求体 dict
    """
    package_id_texts = [str(item) for item in package_ids]
    return {
        "size": size,
        "_source": source_fields,
        "query": {
            "bool": {
                "must": [
                    {
                        "bool": {
                            # assert_id 精确匹配（text 或 keyword）
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
                            # product_info_package_id 过滤（数值或 keyword）
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
    """
    递归展平 ES mapping 的 properties 为 dot 路径 -> 字段信息。

    参数:
        properties: mapping.properties 节点
        prefix: 当前路径前缀
    返回:
        路径到字段定义的 dict
    """
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
    """
    从索引 mapping 响应中提取顶层 properties。

    兼容 ES 6.x/7.x 带 type 与无 type 两种结构。

    参数:
        mappings: index._mapping.mappings 部分
    返回:
        properties dict，找不到时返回空 dict
    """
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
    """
    打印与 assert_id/program_id/package_id 相关的 mapping 摘要。

    参数:
        mapping_response: GET /index/_mapping 的完整响应
    """
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
    """
    格式化打印 ES 搜索结果及 program_id 列表。

    参数:
        response: ES _search 响应体
    """
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
    """
    命令行入口：解析参数、加载密码、执行 ES 查询并打印结果。

    密码加载优先级：--password > EsManager.py > ES_PASSWORD 环境变量 > 交互输入

    参数:
        argv: 可选命令行参数列表，默认 sys.argv
    返回:
        进程退出码，成功为 0
    """
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

    # 密码加载链：命令行 → EsManager → 环境变量 → 交互式输入
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

    # 执行 ES 搜索
    response = client.request("POST", index_path + "/_search", query)
    print_hits(response)
    return 0


if __name__ == "__main__":
    sys.exit(main())
