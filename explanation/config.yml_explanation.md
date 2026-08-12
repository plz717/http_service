# config.yml 说明

## 用途

`kafka_to_hbase.py` / `kafka_to_hbase_v2.py` / `kafka_type140_stats.py` / `verify_type140_hbase_row.py` / `hbase_table_stats.py`（可选）等脚本的**运行配置**。与脚本内 `DEFAULT_CONFIG` 深合并，用户只配差异项即可。

## 主要配置块

### kafka

| 字段 | 含义 |
|------|------|
| `bootstrap_servers` | Broker 列表 |
| `topic` | 如 `video-search-recomm-poms-content` |
| `group_id` | 消费组；换组会重新从头/按 reset 策略消费 |
| `api_version` | 如 `2.0.0` |
| `auto_offset_reset` | 生产常用 `earliest` |
| `enable_auto_commit` | 建议 `false`，脚本手动 commit |
| `max_poll_records` / `poll_timeout_seconds` | 拉取参数 |
| `log_each_message` / `log_skipped_messages` | 全量时常关，避免刷屏 |
| `progress_log_interval_*` | full-load 进度日志 |

### message_filter

- `type: "140"`：只处理多模态向量消息。

### hbase

- `thrift_hosts` / `thrift_port` / `timeout_ms` / `column_family` / `batch_size`

### tables

- `poms_type140_embedding`：type=140 向量表
- `description_*` / `content_*`：遗留榜单表

### one_shot

- `mode: poms_type140`
- `write_table`、`row_key_field: assetId`、`vector_list_type: "1"`
- `fields`：写入 HBase 的列清单

### content_index / operations / logging

榜单索引开关、上下线操作别名、日志级别。

## 使用方式

不是可执行程序，通过 `-c` 传给 Python 脚本，例如：

```bash
python kafka_to_hbase.py -c config.yml --full-load
python kafka_type140_stats.py -c config.yml
python verify_type140_hbase_row.py -c config.yml --asset-id XXX --output log/verify.txt
python hbase_table_stats.py -c config.yml --limit 1000
```

修改本文件后无需改代码即可切换 topic、表名、group、过滤 type 等。
