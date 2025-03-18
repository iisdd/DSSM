#!/usr/bin/env bash
source /home/hadoop/.bashrc

CAD=$(cd `dirname $0`;pwd)
cd $CAD

py_file=$1
date=$2
filename=$3
scene=$4

source /home/hadoop/.bashrc
export SPARK_HOME="/usr/local/spark3"
source activate /home/vip/anaconda2.7/
/usr/local/spark3/bin/spark-submit\
    --queue root.ai.exposure.online\
    --master yarn\
    --deploy-mode cluster\
    --driver-memory 8g\
    --executor-memory 14g\
    --executor-cores 3\
    --jars /data/exposure_common/spark-tensorflow-connector_2.12-1.11.0.jar\
    --conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict\
    --conf spark.hadoop.hive.exec.dynamic.partition=true\
    --conf spark.hadoop.hive.vectorized.execution.enabled=true\
    --conf spark.hadoop.hive.vectorized.execution.reduce.enabled=true\
    --conf spark.hadoop.hive.enforce.bucketing=true\
    --conf spark.hadoop.hive.auto.convert.join=true\
    --conf spark.dynamicAllocation.enabled=true\
    --conf spark.shuffle.service.enabled=true\
    --conf spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive=true\
    --conf spark.hive.mapred.supports.subdirectories=true\
    --conf spark.sql.hive.convertMetastoreOrc=false\
    --conf spark.port.maxRetries=100\
    --archives /home/hadoop/.conda/envs/spark_python3/python3.zip#python3 \
    --conf "spark.pyspark.python=python3/bin/python" \
    --conf spark.yarn.executor.memoryOverhead=25000\
    --conf spark.driver.maxResultSize=3g $py_file -d $date -f $filename

# 运行示例  ./run_sample.sh sample.py "$current_date" "samples" "SEARCH"