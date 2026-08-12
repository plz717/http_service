# calculate_content_status.sh 说明

## 用途

`calculate_content_status.py` 的运维封装：设置 `PYTHONPATH`、创建工作目录、以 `nohup` 启停进程，并支持 Redis 刷新。

## 参数

```bash
sh calculate_content_status.sh <start_type> <content_type>
```

| 位置参数 | 含义 |
|----------|------|
| `$1` `start_type` | `status` / `start` / `restart` / `stop` / `refresh` |
| `$2` `content_type` | 传给 Python 的 `--func_type`，如 `pendant`、`ondemand` |

`refresh` 时不依赖第二个参数的业务含义（内部固定 `--func_type=refresh`）。

## 子命令行为

| 命令 | 行为 |
|------|------|
| `start` | `nohup python3 calculate_content_status.py --func_type=$2 ... &`，再检查状态 |
| `stop` | 按命令行匹配 kill `-9` |
| `restart` | stop + start |
| `status` | 若未运行则自动 start |
| `refresh` | 前台跑一次 Redis 刷新 |

脚本会把 `base_dir` 当作项目根，参数文件固定为：

`${base_dir}/conf/calculate_content_status.json`

并 export：

`PYTHONPATH=/bigdata/zhangcong_yy/recommend_algorithm/video/common:...`

## 使用示例

```bash
# 启动挂件下线消费者
sh calculate_content_status.sh start pendant

# 启动点播下线消费者
sh calculate_content_status.sh start ondemand

# 查看/保活
sh calculate_content_status.sh status pendant

# 停止
sh calculate_content_status.sh stop pendant

# 刷新远程 Redis 下线 zset
sh calculate_content_status.sh refresh
```

## 注意

- 面向 Linux 部署路径编写；Windows 本地一般不能直接用。
- `kill -9` 强制结束，重启前确认不会丢关键内存状态（本服务状态主要在 Redis/HBase）。
- 需保证 `conf/calculate_content_status.json` 与 common 依赖存在。
