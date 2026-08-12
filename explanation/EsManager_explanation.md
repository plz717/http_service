# EsManager.py 说明

## 用途

封装 Elasticsearch 客户端连接：内置集群节点列表，通过 `CryptCommon` 解密硬编码密文得到密码，构造 `elasticsearch.Elasticsearch` 实例。

被 `calculate_content_status.py` 等业务脚本用来访问搜推 ES；`es_lookup_program_id.py` 也会解析本文件中的 `SECRET`/`PASSWORD` 以获取密码。

## 类结构

```python
class EsManager:
    def __init__(self):
        # 连接一组 https://host:port
        # http_auth=("elastic", decrypted_password)
        # verify_certs=False
        self.es = Elasticsearch(...)

    def password(self):
        # base64_decode(SECRET) → key，再 decrypt(PASSWORD, key)
```

初始化后通过 `EsManager().es` 拿到可直接用的客户端。

## 使用方式

```python
from EsManager import EsManager

es = EsManager().es
# 例如查文档 / update 等
doc = es.get(index="some_index", id="doc_id")
```

本文件本身**没有 CLI 入口**，不能单独 `python EsManager.py` 做业务操作；需被其他脚本导入。

## 依赖

- `elasticsearch` / `elasticsearch_dsl`（文件中 import 了 dsl，本类主体只用到 `Elasticsearch`）
- 同目录 `CryptCommon.py`

## 注意

密码以密文形式写在源码中，解密依赖本地 `CryptCommon`。部署时需保证网络可达内网 ES 节点，并注意证书校验已关闭（`verify_certs=False`）。
