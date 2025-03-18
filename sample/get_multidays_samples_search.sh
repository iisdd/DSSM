#!/usr/bin/env bash
# 定义开始和结束日期
# start_date="2024-08-01"
# end_date="2024-10-27"
start_date="2024-10-27"
end_date="2024-10-27"
# end_date="2024-10-17"
# 当前日期设置为开始日期
current_date=$start_date
# 循环直到当前日期超过结束日期
while [ "$(date -d "$current_date" +%s)" -le "$(date -d "$end_date" +%s)" ]
do
    # 调用 run_sample.sh 脚本
    ./run_sample.sh sample.py "$current_date" "samples" "SEARCH"
    # 从 HDFS 获取数据
    formatted_date=${current_date//-/}
    # 删除本地路径中的旧数据
    local_path="/home/jupyterhub/daiyuxuan/dssm_samples_search/data_new_fea/SEARCH_samples_$formatted_date"
    if [ -d "$local_path" ]; then
        rm -rf "$local_path"
    fi
    hadoop fs -get "/user/daiyuxuan/dssm_search/samples/SEARCH_samples_$formatted_date" "$local_path"
    # 使用 date 命令计算下一天的日期
    current_date=$(date -I -d "$current_date + 1 day")
done

# 运行示例  ./get_multidays_samples_search.sh