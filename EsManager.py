import sys
import math, json
import time
import re
import CryptCommon as CryptCommon
from elasticsearch import Elasticsearch, helpers
from elasticsearch_dsl import Search, Q


class EsManager:
    def __init__(
        self,
    ):
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
        passwd = self.password()
        #        self.es = Elasticsearch(self.es_hosts, http_auth=('elastic', passwd), use_ssl=False)
        self.es = Elasticsearch(
            self.es_hosts,
            http_auth=("elastic", passwd),
            verify_certs=False,
            ssl_show_warn=False,
        )

    def password(
        self,
    ):
        SECRET = "WnRQS2xyZXcxZHlERnBWSA=="
        PASSWORD = "61e058ae3bc4d89e7c73763c875f57a9"
        secret_key = CryptCommon.base64_decode(SECRET)
        passwd = CryptCommon.decrypt(PASSWORD, secret_key)

        return passwd
