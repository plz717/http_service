# =============================================================================
# 【文件说明】EsManager.py（带中文注释的副本，原文件未改动）
# -----------------------------------------------------------------------------
# 用途：封装 Elasticsearch 客户端连接。
#   - 内置集群节点列表（HTTPS）
#   - 通过 CryptCommon 解密源码中的密文，得到 elastic 用户密码
#   - 构造 Elasticsearch 实例，供业务脚本直接使用 EsManager().es
#
# 依赖：
#   - elasticsearch / elasticsearch_dsl（本文件主体主要用 Elasticsearch）
#   - 同目录 CryptCommon.py
#
# 使用示例：
#   from EsManager import EsManager
#   es = EsManager().es
#   es.get(index="...", id="...")
#
# 注意：本文件无 CLI；密码以密文写在源码中，verify_certs=False。
# =============================================================================

import sys
import math, json
import time
import re
import CryptCommon as CryptCommon
from elasticsearch import Elasticsearch, helpers
from elasticsearch_dsl import Search, Q


class EsManager:
    """
    ES 连接管理器。
    初始化后通过属性 self.es 访问官方 Elasticsearch 客户端。
    """

    def __init__(
        self,
    ):
        # 搜推 ES 集群节点（29201 / 29202 两套端口）
        self.es_hosts = [
            "https://10.194.9.2:29201",
            "https://10.194.9.3:29201",
            "https://10.194.9.4:29201",
            "https://10.194.9.5:29201",
            "https://10.194.9.6:29201",
            "https://10.194.9.2:29202",
            "https://10.194.9.3:29202",
            "https://10.194.9.4:29202",
            "https://10.194.9.5:29202",
            "https://10.194.9.6:29202",
        ]
        # 解密得到明文密码
        passwd = self.password()
        #        self.es = Elasticsearch(self.es_hosts, http_auth=('elastic', passwd), use_ssl=False)
        # 生产连接：Basic Auth + 关闭证书校验（内网自签证书场景）
        self.es = Elasticsearch(
            self.es_hosts,
            http_auth=("elastic", passwd),
            verify_certs=False,
            ssl_show_warn=False,
        )

    def password(
        self,
    ):
        """
        从硬编码密文还原 ES 密码。
        步骤：
          1. SECRET 经 Base64 解码得到 AES 密钥
          2. PASSWORD（十六进制密文）用 AES-CBC 解密得到明文密码
        返回明文密码字符串。
        """
        SECRET = "WnRQS2xyZXcxZHlERnBWSA=="
        PASSWORD = "61e058ae3bc4d89e7c73763c875f57a9"
        secret_key = CryptCommon.base64_decode(SECRET)
        passwd = CryptCommon.decrypt(PASSWORD, secret_key)

        return passwd
