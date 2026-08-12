# verify_type140_hbase_row.py 说明

## 用途

端到端核对工具：在 Kafka 中按 `assetId` 找到一条 type=140 消息，按与写入脚本相同的规则映射成 HBase 文档，再读出 HBase 对应行，**逐字段对比**，输出报告文件。

用于验证灌库是否正确、某条数据为何不一致。

## 工作流程

1. 加载 `config.yml`。
2. 从 Kafka earliest 扫到启动时 end offset，查找目标 `assetId`。
3. 调用 `kafka_to_hbase.build_poms_type140_document` 构造期望文档。
4. 用 `HBaseWriter.get_columns` 读取现有行。
5. 对比字段，写 txt 报告（含 Kafka 原文、映射文档、HBase 行、对比结果）。

## 依赖

复用 `kafka_to_hbase.py` 中的配置加载、解析与 HBase 读写函数；需能连 Kafka 与 HBase Thrift。

## 使用方式

```bash
python verify_type140_hbase_row.py -c config.yml --asset-id <ASSET_ID> --output <报告路径> [选项]
```

### 主要选项

| 选项 | 说明 |
|------|------|
| `--asset-id` | 必填，目标 assetId / row key |
| `--output` | 必填，报告输出路径 |
| `--table` | HBase 表名，默认取 config |
| `--filter-type` | 外层消息 type，默认取 config |
| `--vector-list-type` | vectorList 项 type，默认 1 |
| `--max-scan` | 最多扫描消息数，0 表示扫完快照 |
| `--progress-records` | 进度日志间隔 |

### 示例

```bash
python verify_type140_hbase_row.py -c config.yml \
  --asset-id 1234567890 \
  --output log/verify.txt
```

stdout 会打印匹配/不匹配字段数；不匹配字段会打印 `DIFF field=...`。
