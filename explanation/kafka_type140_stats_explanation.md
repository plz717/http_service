# kafka_type140_stats.py 说明

## 用途

对 Kafka 中 **type=140**（可配置）消息做**只读统计**，不写 HBase、不提交 consumer group offset。

统计内容包括：

- 扫描条数、匹配条数、唯一 `assetId`、重复消息数
- 各 `type` 分布
- `vectorItem` / `vectorList` 是否存在、各 type 的维度与 base64 校验状态
- 重复 `assetId` TopN、缺失 `assetId` 样例
- 可选保存匹配消息样本

常用于灌库前摸底：消息量、向量质量、重复情况。

## 工作方式

1. 读取 `config.yml`（至少需要 `kafka.bootstrap_servers`、`kafka.topic`）。
2. 手动 assign 分区，从 beginning 扫到启动时的 end offset。
3. 解析 JSON，按 `filter-type`（默认 140）过滤并累计计数。
4. 结束时打印摘要。

## 使用方式

```bash
python kafka_type140_stats.py -c config.yml [选项]
```

### 主要选项

| 选项 | 说明 |
|------|------|
| `--filter-type` | 要统计的消息 type，默认读 config 或 140 |
| `--max-scan N` | 最多扫描 N 条 |
| `--max-matched N` | 最多匹配 N 条 |
| `--poll-records` | 每次 poll 条数 |
| `--progress-records` / `--progress-seconds` | 进度打印频率 |
| `--top-n` | 重复 assetId / 缺失样例打印数量 |
| `--expected-vector-dimension` | 期望 float32 维数，默认 1024（解码后 4096 字节） |
| `--save-samples PATH` | 保存样本路径 |
| `--sample-count` | 最多保存样本条数 |

### 示例

```bash
# 扫完整快照并出统计
python kafka_type140_stats.py -c config.yml

# 快速抽样：最多扫 5 万条，并存 5 条样本
python kafka_type140_stats.py -c config.yml --max-scan 50000 --save-samples log/kafka_140_samples.txt
```
