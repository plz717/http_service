# kafka_to_hbase.py 说明

## 用途

从 Kafka 消费消息并写入 HBase 的主脚本。当前生产主链路是 **POMS `type=140` 多模态向量**：过滤消息 → 解析 JSON → 选取 `vectorList` 中指定类型（默认 `type=1`）→ base64 向量校验 → 版本防倒灌 → 写入 HBase。

同时保留遗留 **榜单（chart / chart_content）** 写入逻辑（当未配置 `write_table` 时走 `ChartMessageProcessor`）。

## 核心组件

| 组件 | 作用 |
|------|------|
| `load_config` / `DEFAULT_CONFIG` | 读取 YAML，与默认配置深合并并校验必填项 |
| `HBaseWriter` | 通过 Thrift（happybase）连接 HBase，支持 put / batch / get，多 host 轮询重试 |
| `ChartMessageProcessor` | 处理 `chart` / `chart_content` 上下线 |
| `build_poms_type140_document` | 从 type=140 消息构造待写文档 |
| `write_poms_type140_to_hbase` | 与现有行比较 version/time，防旧数据覆盖 |
| `run_consumer` | Kafka 消费主循环 |

## 依赖

- `kafka-python`
- `happybase`
- `PyYAML`

## 使用方式

必须指定配置文件：

```bash
python kafka_to_hbase.py -c config.yml [选项]
```

### 常用选项

| 选项 | 说明 |
|------|------|
| `--dry-run` | 只消费解析，不写 HBase，也不提交 offset |
| `--once` | 处理 1 条匹配消息后退出 |
| `--count N` | 匹配 N 条后退出 |
| `--write-count N` | 成功写入 N 条后退出 |
| `--max-scan N` | 扫描 N 条（含未匹配）后退出 |
| `--full-load` | 持续全量消费写入，忽略 count 限制 |
| `--stop-at-start-end-offsets` | 从 earliest 扫到启动时的 end offset 后退出（全量快照灌库） |
| `--stop-after-idle-seconds N` | 空闲 N 秒后退出 |
| `--filter-type` | 只处理指定 `type`（也可在 config 的 `message_filter.type` 配置） |
| `--inspect-fields` | 打印消息字段路径与类型 |
| `--save-samples PATH` | 把匹配消息追加到文件 |
| `--write-table` | one-shot 写入目标表 |
| `--write-fields` | 逗号分隔写入字段 |
| `--row-key-field` | row key 字段名 |
| `--group-id` | 覆盖消费组 |
| `--vector-list-type` | 选取 `vectorList` 中的 type（默认 1） |

### 典型命令

```bash
# 摸底：看字段，不写库
python kafka_to_hbase.py -c config.yml --dry-run --count 5 --inspect-fields

# 灰度试写：写成功 2 条就停
python kafka_to_hbase.py -c config.yml --write-count 2 --group-id test-group-001

# 全量初始化：扫完启动时快照后退出
python kafka_to_hbase.py -c config.yml --full-load --stop-at-start-end-offsets

# 常驻增量消费
nohup python kafka_to_hbase.py -c config.yml --full-load > kafka_to_hbase.log 2>&1 &
```

## 注意事项

1. 默认手动 commit offset（`enable_auto_commit=false`），每批处理完再提交。
2. `dry-run` 既不写 HBase 也不 commit，可反复看同一批消息。
3. 换 `group_id` 会按 `auto_offset_reset`（通常 earliest）重新消费。
4. 非法向量（非合法 base64 或字节长度与维数不符）会跳过，不入库。
