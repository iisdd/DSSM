#!/bin/bash 
date=$1
$SPARK_HOME/bin/spark-submit \
--master yarn \
--conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict \
--conf spark.hadoop.hive.exec.dynamic.partition=True \
--conf spark.hadoop.hive.vectorized.execution.enabled=True \
--conf spark.hadoop.hive.vectorized.execution.reduce.enabled=True \
--conf spark.hadoop.hive.enforce.bucketin=True \
--conf spark.hadoop.hive.auto.convert.join=True \
--conf spark.dynamicAllocation.enabled=True \
--conf spark.shuffle.service.enabled=True \
--conf spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive=True \
--conf spark.metrics.conf= \
--conf spark.hive.mapred.supports.subdirectories=True \
--conf spark.sql.hive.convertMetastoreOrc=false \
--conf spark.port.maxRetries=100 \
--archives hdfs:///user/daiyuxuan/dssm_env/py3.6_env.tar.gz#dssm_env \
--conf spark.executorEnv.PYSPARK_PYTHON=./dssm_env/bin/python \
--conf spark.executorEnv.PYSPARK_DRIVER=./dssm_env/bin/python \
--conf spark.executorEnv.LD_LIBRARY_PATH=./dssm_env/lib \
--conf spark.yarn.appMasterEnv.LD_LIBRARY_PATH=./dssm_env/lib \
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./dssm_env/bin/python \
--conf spark.yarn.appMasterEnv.PYSPARK_DRIVER=./dssm_env/bin/python \
--executor-cores 5 \
--num-executors 20 \
--executor-memory 8g \
--driver-memory 4g \
--name airflow-spark \
--queue root.ai.exposure.online \
--deploy-mode cluster \
--conf spark.network.timeout=10000000 \
--conf spark.yarn.executor.memoryOverhead=5000 \
--conf spark.sql.broadcastTimeout=1500 \
/home/jupyterhub/daiyuxuan/dssm_models_search/inference_spark_item.py -d ${date} 