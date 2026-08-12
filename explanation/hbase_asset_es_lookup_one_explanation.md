# hbase_asset_es_lookup_one.py 说明

## 用途

取 **一条** HBase 向量行的 `assetId`，再到 ES 用 `assert_id` 查完整文档，打印字段路径与 JSON。用于确认「HBase 一行 ↔ ES 文档」字段结构，方便后续合并字段设计。

## 工作流程

1. 连接 HBase：指定 `--row-key` 则 get，否则 scan 取第一行。
2. 读取 `assetId`。
3. 用 `es_lookup_program_id.EsClient` 按 `assert_id` 搜索（可带 package_id 过滤）。
4. 打印/可选写入报告（含字段路径树）。

## 使用方式

```bash
python hbase_asset_es_lookup_one.py [选项]
```

### 主要选项

| 选项 | 说明 |
|------|------|
| `--hbase-table` | 默认多模态 embedding 表 |
| `--row-key` | 指定 row；省略则取表中第一行 |
| `--index` / `--nodes` / `--username` / `--password` | ES 连接 |
| `--package-ids` | 默认 `1,141`；空字符串表示不过滤 |
| `--size` | ES 返回条数，默认 1 |
| `--print-query` | 打印 DSL |
| `--output` | 报告写到文件 |

### 示例

```bash
# 指定 assetId
python hbase_asset_es_lookup_one.py --row-key 1234567890 --output log/hbase_es_lookup_one.txt

# 随便取一行摸底
python hbase_asset_es_lookup_one.py --output log/hbase_es_lookup_one_plz.txt
```
