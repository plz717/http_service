
#!/usr/bin/bash
. ~/.bashrc
export PYTHONPATH=/bigdata/zhangcong_yy/recommend_algorithm/video/common:${PYTHONPATH}


base_dir=$(cd $(dirname $0);pwd|sed "s/\/bin//g");
dayid=`date -d"-0days" "+%Y%m%d"`
start_type=$1
content_type=$2


# 设置目标目录
target_dir=${base_dir}
echo $target_dir


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


function debug(){
    msg=$1
    echo "[debug] `date +"%Y-%m-%d %H:%M:%S"` =================$msg=================="
}


function string2arr(){
    mystr=$1
    array=(${mystr//,/ })  
    for var in ${array[@]}
    do
       echo $var
    done 
}


function start(){
    echo "Start executor ..."
    nohup python3 ${base_dir}/calculate_content_status.py --func_type=${content_type} --target_dir=${base_dir} --param_spec=${base_dir}/conf/calculate_content_status.json &
}


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


function refresh(){
    echo "Start refresh ..."
    python3 ${base_dir}/calculate_content_status.py --func_type=refresh --target_dir=${base_dir} --param_spec=${base_dir}/conf/calculate_content_status.json
}


debug 'start executing'


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
      echo "sh run_service.sh [status|start|restart|stop]"
esac


debug 'end executing'

