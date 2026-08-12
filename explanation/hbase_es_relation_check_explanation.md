# hbase_es_relation_check.py 说明

## 用途

批量检查 HBase 中 `assetId`/`assert_id` 与 ES `program_id` 的对应关系，输出统计与样例，可选 CSV 明细。

关注点：

- HBase 有多少行、多少唯一 assert_id
- ES 匹配率；带 package 过滤后仍缺失的数量
- 去掉 package 过滤后「其实能查到」的缺失（诊断产品线过滤问题）
- 一对多：一个 assert_id 多个 program_id / 一个 program_id 多个 assert_id

## 工作流程

1. Scan HBase 收集 `(row_key, assert_id)`（`--limit`，0=全表）。
2. 分批 terms 查询 ES（带 `product_info_package_id` 过滤）。
3. 对仍缺失的 id，可再查一遍不加 package 过滤（`--no-diagnose-missing` 可关）。
4. 打印摘要；可选 `--output-csv`。

## 使用方式

```bash
python hbase_es_relation_check.py [选项]
```

### 主要选项

| 选项 | 说明 |
|------|------|
| `--hbase-table` | 默认测试 embedding 表名 |
| `--limit` | 默认 20000；0=全表 |
| `--package-ids` | 默认 `1,141` |
| `--es-batch-size` | 每批 assert_id 数量 |
| `--max-hits-per-assert` | 每个 assert 的结果预算 |
| `--example-limit` | 控制台样例条数 |
| `--output-csv` | 明细 CSV |
| `--no-diagnose-missing` | 不对缺失做无 package 二次查询 |

### 示例

```bash
python hbase_es_relation_check.py \
  --hbase-table recommend:video_search_recomm_poms_type1_embedding_multimode \
  --limit 5000 \
  --output-csv log/hbase_es_relation.csv
```
