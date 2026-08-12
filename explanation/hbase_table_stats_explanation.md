# hbase_table_stats.py 说明

## 用途

通过 HBase Thrift 扫描表，输出**行数、列出现次数、样例行**等统计。也可只查单行。

用于灌库后核对表规模、列是否齐全。

## 工作方式

1. 连接 HBase（hosts 可来自命令行，或 `-c config.yml` 复用 kafka_to_hbase 配置）。
2. `--row-key` 模式：`get` 单行并打印。
3. 否则全表/`--limit` 扫描，统计 `row_count`、空行、各 qualifier 出现次数、采样若干行。
4. 输出 JSON 摘要（可 `--output` 写文件）。

## 使用方式

```bash
python hbase_table_stats.py [选项]
```

### 主要选项

| 选项 | 说明 |
|------|------|
| `-c/--config` | 可选，从 YAML 读 thrift_hosts、表名等 |
| `--hbase-hosts` / `--hbase-port` / `--hbase-timeout-ms` | Thrift 连接 |
| `--hbase-table` | 表名 |
| `--column-family` | 列族，默认 `cf` |
| `--row-key` | 只查这一行 |
| `--limit N` | 最多扫 N 行，0=全表 |
| `--columns` | 只扫指定 qualifier（逗号分隔） |
| `--sample-size` | 摘要中样例行数 |
| `--progress-every` | 进度打印间隔 |
| `--output` | JSON 输出文件 |

### 示例

```bash
# 复用 config，全表统计（大表较慢）
python hbase_table_stats.py -c config.yml --output log/whole_hbase_stats.txt

# 只看前 1000 行
python hbase_table_stats.py -c config.yml --limit 1000

# 查单行
python hbase_table_stats.py -c config.yml --row-key 1234567890
```
