# kafka_to_hbase_v2.py 说明

## 用途

`kafka_to_hbase.py` 的加固版本。业务逻辑、配置项、CLI 参数与 v1 **基本一致**，主要差异是：**遇到非法/空 JSON Kafka 消息时只打 WARNING 并跳过，不会因单条坏消息整进程退出**。

适合全量灌库或线上常驻时，topic 里可能混有脏数据的场景。

## 与 v1 的主要差异

1. 新增 `preview_kafka_value()`：把坏消息内容截断打印，便于排查。
2. `decode_kafka_value()`：对 `None`、空 bytes、空白字符串给出明确错误。
3. 消费循环中对 `JSONDecodeError` / `UnicodeDecodeError` / `ValueError` / `TypeError` 捕获后跳过，并计入 `type_counts["<invalid_json>"]`。
4. argparse 描述中标明 v2 行为。

其余：POMS type=140 写入、版本防倒灌、榜单链路、`--full-load` / `--stop-at-start-end-offsets` 等与 v1 相同。

## 依赖

与 v1 相同：`kafka-python`、`happybase`、`PyYAML`。

## 使用方式

```bash
python kafka_to_hbase_v2.py -c config.yml [选项]
```

选项与 `kafka_to_hbase.py` 完全对齐，例如：

```bash
# dry-run 摸底
python kafka_to_hbase_v2.py -c config.yml --dry-run --count 5 --inspect-fields

# 全量快照灌库（推荐用 v2，脏消息不会打断）
python kafka_to_hbase_v2.py -c config.yml --full-load --stop-at-start-end-offsets

# 常驻
nohup python kafka_to_hbase_v2.py -c config.yml --full-load > kafka_to_hbase_v2.log 2>&1 &
```

## 选型建议

| 场景 | 建议 |
|------|------|
| 需要严格失败、尽早暴露脏数据 | 用 `kafka_to_hbase.py` |
| 全量/生产常驻，容忍脏消息 | 用 `kafka_to_hbase_v2.py` |
