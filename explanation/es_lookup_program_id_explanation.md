# es_lookup_program_id.py 说明

## 用途

按 **`assert_id`** 在 Elasticsearch 点播内容索引中查询，返回 `program_id` 及相关字段。可顺带打印关键字段 mapping。

是 HBase↔ES 关联工具链的基础：自带轻量 `EsClient`，并能从同目录 `EsManager.py` + `CryptCommon.py` 自动解密 ES 密码。

## 默认参数

- 索引：`video_poms_ondemand_content_detail`
- 过滤：`product_info_package_id` 为 `1` 或 `141`
- 返回字段：`assert_id`、`program_id`、`product_info_package_id`、`name`

## 密码获取顺序

1. `--password`
2. 解析 `EsManager.py` 中加密配置（除非 `--no-es-manager`）
3. 环境变量 `ES_PASSWORD`
4. 交互式 `getpass` 提示

## 使用方式

```bash
python es_lookup_program_id.py --assert-id <ASSERT_ID> [选项]
```

### 主要选项

| 选项 | 说明 |
|------|------|
| `--assert-id` | 必填 |
| `--index` | ES 索引 |
| `--nodes` | 逗号分隔 `host:port` |
| `--scheme` | 默认 `https` |
| `--username` | 默认 `elastic` |
| `--password` | 明文密码 |
| `--package-ids` | 默认 `1,141` |
| `--source-fields` | `_source` 字段列表 |
| `--size` | 返回条数 |
| `--show-mapping` | 先打印相关 mapping |
| `--print-query` | 打印查询 DSL |
| `--verify-cert` | 校验 HTTPS 证书 |

### 示例

```bash
python es_lookup_program_id.py --assert-id ABC123 --show-mapping --print-query
```
