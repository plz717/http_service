# merge_es_fields_to_hbase.py 说明

## 用途

按关联关系 **把 ES 字段合并写回 HBase**：用 HBase 行的 `assetId` 去 ES 查 `assert_id`，取出指定字段（默认 `program_id`），写回同一 HBase 行。

多 hit 时同一字段多个值会用 `|` 拼接。

支持 `--dry-run` 只出报告不写库。

## 工作流程

1. 取 HBase 行：`--row-key` 单行，或 scan（可 `--limit`）。
2. 按 `--es-batch-size` 分批查 ES。
3. 汇总 `--merge-fields`，batch put 到 HBase（非 dry-run）。
4. 打印汇总；可选 CSV。

## 使用方式

```bash
python merge_es_fields_to_hbase.py [选项]
```

### 主要选项

| 选项 | 说明 |
|------|------|
| `--hbase-table` | 目标表 |
| `--row-key` | 只处理一行 |
| `--limit` | scan 上限，0=全表 |
| `--merge-fields` | 逗号分隔，默认 `program_id` |
| `--package-ids` | 默认 `1,141`；空串不过滤 |
| `--dry-run` | 不写 HBase |
| `--output-csv` | 合并结果报告 |
| `--es-batch-size` / `--hbase-write-batch-size` | 批大小 |

### 示例

```bash
# 先 dry-run 看一行
python merge_es_fields_to_hbase.py --row-key 1234567890 --dry-run --output-csv log/merge_one.csv

# 合并 program_id 到表（真实写入）
python merge_es_fields_to_hbase.py --limit 1000 --merge-fields program_id

# 多字段
python merge_es_fields_to_hbase.py --merge-fields program_id,name --dry-run
```
