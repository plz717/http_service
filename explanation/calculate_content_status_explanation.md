# calculate_content_status.py 说明

## 用途

内容上下线状态同步服务：消费 Kafka 中的**挂件 / 点播**消息，判断是否应下线，并联动更新 HBase、搜推 ES、Redis 等。

与本仓库的 POMS type=140 向量灌库链路不同，这是更早期的「内容状态」常驻任务，依赖外部 `common` 包（`TimeUtils`、`RedisManager`、`hbase_client`、`kafka_client`、`PersistentModule` 等），通常在服务器指定 `PYTHONPATH` 下运行。

## 类 `CalculateContentStatus` 主要方法

| 方法 | 作用 |
|------|------|
| `GetOnePendantStatus` | 根据赛事/赛季/测试标记/直播间状态等判断挂件是否在线 |
| `ConsumerPendantKafka` | 消费挂件 Kafka；下线时写 HBase/ES/Redis |
| `ConsumerOndemandKafka` | 消费点播 type=208/217；`operation==2` 视为下线 |
| `ConsumerDebugKafka` | 调试消费，打印少量消息 |
| `UpdateRemoteRedis` | 清理并刷新远程 Redis 下线 zset（`refresh`） |

下线时会设置 `status=0`、`additional_status=0`，并尽量删除 `video_cp:<id>` 等 Redis key。

## 使用方式

需要 JSON 参数文件（脚本里默认路径由 shell 传入，如 `conf/calculate_content_status.json`）：

```bash
python calculate_content_status.py \
  --func_type=pendant|ondemand|refresh \
  --target_dir=<项目根目录> \
  --param_spec=<参数json路径>
```

| `--func_type` | 行为 |
|---------------|------|
| `pendant` | 常驻消费挂件下线 |
| `ondemand` | 常驻消费点播下线 |
| `refresh` | 一次性刷新远程 Redis 下线集合 |

生产环境更常用配套 shell（见 `calculate_content_status.sh`）做 start/stop/status。

## 依赖提示

本目录单独 clone 时，若缺少 `common` 下的模块与 `conf/*.json`，脚本无法直接运行；需在完整推荐算法工程环境中使用。
