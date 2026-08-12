#!/usr/bin/bash
# =============================================================================
# 【脚本说明】calculate_content_status.sh（带中文注释的副本，原文件未改动）
# -----------------------------------------------------------------------------
# 用途：calculate_content_status.py 的运维封装（Linux）。
#   - 设置 PYTHONPATH 指向推荐算法 common 库
#   - 创建 data/log 等工作目录
#   - 以 nohup 启停挂件/点播消费者，或执行 Redis refresh
#
# 用法：
#   sh calculate_content_status.sh <start_type> <content_type>
#
# 参数：
#   $1 start_type   : status | start | restart | stop | refresh
#   $2 content_type : 传给 Python 的 --func_type，如 pendant / ondemand
#                     （refresh 时内部固定 func_type=refresh）
#
# 示例：
#   sh calculate_content_status.sh start pendant
#   sh calculate_content_status.sh stop ondemand
#   sh calculate_content_status.sh refresh
#
# 注意：依赖服务器路径与 conf/calculate_content_status.json；面向 Linux。
# =============================================================================

# 加载用户环境（可能含 conda/java 等）
. ~/.bashrc
# 把视频推荐 common 库加入 PYTHONPATH，供 calculate_content_status.py import
export PYTHONPATH=/bigdata/zhangcong_yy/recommend_algorithm/video/common:${PYTHONPATH}


# 脚本所在目录视为项目根；若路径含 /bin 则去掉（兼容 bin 子目录部署）
base_dir=$(cd $(dirname $0);pwd|sed "s/\/bin//g");
# 当天日期（当前脚本未强依赖 dayid，保留兼容）
dayid=`date -d"-0days" "+%Y%m%d"`
# 第一个参数：动作；第二个参数：业务类型（pendant/ondemand）
start_type=$1
content_type=$2


# 设置目标目录
target_dir=${base_dir}
echo $target_dir


# 首次运行时创建常用子目录
if [ ! -d "$target_dir/data" ]; then
    mkdir -p "$target_dir/lock"
    mkdir -p "$target_dir/data"
    mkdir -p "$target_dir/debug"
    mkdir -p "$target_dir/result"
    mkdir -p "$target_dir/model"
    mkdir -p "$target_dir/monitor"
    mkdir -p "$target_dir/log"
    echo "目录 $target_dir 已创建。"
else
    echo "目录 $target_dir 已存在。"
fi


# 打印带时间戳的调试分隔行
function debug(){
    msg=$1
    echo "[debug] `date +"%Y-%m-%d %H:%M:%S"` =================$msg=================="
}


# 把逗号分隔字符串拆成多行（当前主流程未调用，保留工具函数）
function string2arr(){
    mystr=$1
    array=(${mystr//,/ })  
    for var in ${array[@]}
    do
       echo $var
    done 
}


# 后台启动 Python 消费者：func_type 由 content_type 决定
function start(){
    echo "Start executor ..."
    nohup python3 ${base_dir}/calculate_content_status.py --func_type=${content_type} --target_dir=${base_dir} --param_spec=${base_dir}/conf/calculate_content_status.json &
}


# 按命令行特征查找进程并用 kill -9 强制结束
function stop(){
    echo "Stop executor ..."


    pid_num=$(ps -ef | grep 'calculate_content_status.py --func_type='${content_type} | grep -v "grep" | awk '{print $2}')
    if [ -n "$pid_num" ]; then
        echo "kill process ${pid_num}"
        kill -9 "${pid_num}"
    else
        echo 'No running server process found'
    fi  
}


# 若未运行则自动 start（保活）；已运行则直接退出 0
function status(){
    echo "Status executor ..."
    pid_count=$(ps -ef | grep 'calculate_content_status.py --func_type='${content_type} | grep -v "grep" | wc -l)
    if [[ $pid_count>0 ]]; 
    then
        echo "Process Aready Run!!!"
        exit 0
    else
        start
        echo 'No running server process found'
    fi
}


# 前台执行一次 Redis 下线 zset 刷新（func_type=refresh）
function refresh(){
    echo "Start refresh ..."
    python3 ${base_dir}/calculate_content_status.py --func_type=refresh --target_dir=${base_dir} --param_spec=${base_dir}/conf/calculate_content_status.json
}


debug 'start executing'


# 按第一个参数分发到对应函数
case ${start_type} in 
    "status")
        status
    ;;
    "start")
        start 
        sleep 2s
        status
    ;;
    "restart")
        stop
        start 
        sleep 2s
        status
    ;;
    "stop")
        stop
    ;;
    "refresh")
        refresh
    ;;
    *)
      # 帮助提示（历史文案仍写 run_service.sh）
      echo "sh run_service.sh [status|start|restart|stop]"
esac


debug 'end executing'
