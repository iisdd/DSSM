# -*- coding: UTF-8 -*-
import sh
import json
import os
import redis
import time
import random
import pymysql
import numpy as np
from pyspark.sql.types import *
import json
import sys
from pyspark import SparkContext
from pyspark.sql import HiveContext
import datetime as dt
from pyspark.sql import SparkSession
import argparse
from numpy.linalg import norm
from pyspark.sql.functions import lit
from pyspark.sql import Window
from pyspark.sql import functions as F
import datetime
import math
import time
import tensorflow as tf
from pyspark import SparkFiles
from pyspark.sql.functions import udf
from pyspark.sql.functions import monotonically_increasing_id, row_number
from pyspark.sql.types import StringType
import configparser
from io import StringIO
from rediscluster import RedisCluster
import pandas as pd
from tensorflow import keras
from tensorflow.keras.layers import Flatten
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import roc_auc_score
from collections import defaultdict
import joblib

sc = SparkContext(
    appName="c2b_dssm_user_search%s@dyx" % (dt.datetime.now().strftime("%Y%m%d"))
)
sqlContext = HiveContext(sc)
sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
sqlContext.sql("set hive.execution.engine=tez")
sqlContext.sql("set hive.vectorized.execution.enabled=true")
sqlContext.sql("set hive.vectorized.execution.reduce.enabled=true")
sqlContext.sql("set hive.exec.dynamic.partition=true")
sqlContext.sql("set hive.enforce.bucketing=true")
sqlContext.sql("set hive.auto.convert.join=true")
sqlContext.sql("set hive.merge.mapredfiles = true")
sqlContext.sql("set hive.merge.smallfiles.avgsize=16000000")
sqlContext.sql("set mapreduce.input.fileinputformat.input.dir.recursive=true")
spark = SparkSession.builder.getOrCreate()
sparse_user_feature = ["s_u_click_tag_id_0_7", "s_u_click_seats_0_7", "s_u_click_minor_category_id_0_7", "s_u_click_guobie_0_7", "s_u_click_gearbox_0_7", "s_u_click_fuel_type_0_7", "s_u_click_emission_standard_0_7", "s_u_click_city_id_0_7", "s_u_click_car_year_0_7", "s_u_click_auto_type_0_7", "s_u_click_car_color_0_7", "s_u_click_evaluate_level_segment_0_7", "s_u_click_evaluate_score_segment_0_7", "s_u_click_model_price_bin_0_7", "s_u_bid_tag_id_0_7", "s_u_bid_seats_0_7", "s_u_bid_minor_category_id_0_7", "s_u_bid_guobie_0_7", "s_u_bid_gearbox_0_7", "s_u_bid_fuel_type_0_7", "s_u_bid_emission_standard_0_7", "s_u_bid_city_id_0_7", "s_u_bid_car_year_0_7", "s_u_bid_auto_type_0_7", "s_u_bid_car_color_0_7", "s_u_bid_evaluate_level_segment_0_7", "s_u_bid_evaluate_score_segment_0_7", "s_u_bid_model_price_bin_0_7", "s_u_deal_tag_id_0_30", "s_u_deal_seats_0_30", "s_u_deal_minor_category_id_0_30", "s_u_deal_guobie_0_30", "s_u_deal_gearbox_0_30", "s_u_deal_fuel_type_0_30", "s_u_deal_emission_standard_0_30", "s_u_deal_city_id_0_30", "s_u_deal_car_year_0_30", "s_u_deal_auto_type_0_30", "s_u_deal_car_color_0_30", "s_u_deal_tag_id_0_365", "s_u_deal_seats_0_365", "s_u_deal_minor_category_id_0_365", "s_u_deal_guobie_0_365", "s_u_deal_gearbox_0_365", "s_u_deal_fuel_type_0_365", "s_u_deal_emission_standard_0_365", "s_u_deal_city_id_0_365", "s_u_deal_car_year_0_365", "s_u_deal_auto_type_0_365", "s_u_deal_car_color_0_365", "user_id"]
dense_user_feature = ["d_u_beseen_cnt_0_7", "d_u_beseen_cnt_7_14", "d_u_beseen_cnt_14_30", "d_u_click_cnt_0_7", "d_u_click_cnt_7_14", "d_u_click_cnt_14_30", "d_u_bid_cnt_0_7", "d_u_bid_cnt_7_14", "d_u_bid_cnt_14_30", "d_u_collect_cnt_0_7", "d_u_collect_cnt_7_14", "d_u_collect_cnt_14_30", "d_u_contract_cnt_30", "d_u_contract_cnt_365", "d_u_click_suggest_price_max_0_30", "d_u_click_suggest_price_min_0_30", "d_u_click_suggest_price_median_0_30", "d_u_click_road_haul_max_0_30", "d_u_click_road_haul_min_0_30", "d_u_click_road_haul_median_0_30", "d_u_click_evaluate_score_max_0_30", "d_u_click_evaluate_score_min_0_30", "d_u_click_evaluate_score_median_0_30", "d_u_bid_suggest_price_max_0_30", "d_u_bid_suggest_price_min_0_30", "d_u_bid_suggest_price_median_0_30", "d_u_bid_road_haul_max_0_30", "d_u_bid_road_haul_min_0_30", "d_u_bid_road_haul_median_0_30", "d_u_bid_evaluate_score_max_0_30", "d_u_bid_evaluate_score_min_0_30", "d_u_bid_evaluate_score_median_0_30"]
sparse_item_feature = ["s_i_c2b_evaluate_score_segment", "s_i_city_id", "s_i_emission_standard", "s_i_minor_category_id", "s_i_model_price_bin", "s_i_car_year", "s_i_gearbox", "s_i_air_displacement", "s_i_tag_id", "s_i_car_color", "s_i_guobie", "s_i_evaluate_level", "s_i_auto_type", "s_i_fuel_type", "s_i_seats", "s_i_c2b_ctob_car_level"]
dense_item_feature = ["d_i_c2b_offline_car_click_bid_rate_0_3_d", "d_i_c2b_offline_car_click_bid_rate_0_30_d", "d_i_c2b_offline_car_beseen_click_rate_0_3_d", "d_i_c2b_offline_car_beseen_click_rate_0_30_d", "d_i_c2b_offline_car_tag_id_bid_percent_ratio_0_7_d", "d_i_c2b_offline_car_tag_id_bid_percent_ratio_7_14_d", "d_i_c2b_offline_car_tag_id_bid_percent_ratio_14_30_d", "d_i_c2b_offline_car_tag_id_click_rate_0_7_d", "d_i_c2b_offline_car_tag_id_click_rate_7_14_d", "d_i_c2b_offline_car_tag_id_quick_collection_rate_0_7_d", "d_i_c2b_offline_car_tag_id_quick_collection_rate_7_14_d", "d_i_c2b_offline_car_tag_id_bid_rate_0_7_d", "d_i_c2b_offline_car_tag_id_bid_rate_7_14_d", "d_i_c2b_offline_car_minor_category_id_click_rate_0_7_d", "d_i_c2b_offline_car_minor_category_id_quick_collection_rate_0_7_d", "d_i_c2b_offline_car_minor_category_id_quick_collection_rate_7_14_d", "d_i_transfer_num", "d_i_c2b_evaluate_score", "d_i_c2b_ctob_model_price", "d_i_c2b_seller_price", "d_i_c2b_ctob_diff_price", "d_i_road_haul"]

sparse_fea = sparse_user_feature + sparse_item_feature
dense_fea = dense_user_feature + dense_item_feature
user_fea = sparse_user_feature + dense_user_feature
item_fea = sparse_item_feature + dense_item_feature
total_fea = sparse_user_feature + dense_user_feature + sparse_item_feature + dense_item_feature # 注意按顺序

model_epoch = 'epoch2'
model_date = '2024-09-19'
parser = argparse.ArgumentParser()
parser.add_argument('-d', '--prod_date', default=(dt.datetime.today() - dt.timedelta(days=1)).strftime('%Y-%m-%d'),
                    type=str, required=False, help="prod date")
arg = parser.parse_args()
prod_date = arg.prod_date

print("模型版本: {}_{}".format(model_date, model_epoch))  # 打印模型版本
print("生产日期: {}".format(prod_date))  # 打印生产日期
print("当前小时数: {}".format(datetime.datetime.now().hour))

def spark_load_model(model_name):
    prepath = "hdfs:///user/daiyuxuan/dssm_search/models/"
    spark.sparkContext.addFile(prepath+model_name, True)
    model_file_local = SparkFiles.get(model_name)
    model = tf.keras.models.load_model(model_file_local)
    return model
# 加载模型
print("="*10, "模型加载中", "="*10)
user_layer_model = spark_load_model('user_layer_model_{model_epoch}_{model_date}'.format(model_epoch = model_epoch, model_date = model_date))
print("="*10, "模型加载完毕", "="*10)

# 预处理，加载保存的中位数和scaler
def load_scaler_and_medians(scaler_path, medians_path, dense_user_feature, dense_fea, model_date):
    spark.sparkContext.addFile(scaler_path)
    spark.sparkContext.addFile(medians_path)
    local_scaler_path = SparkFiles.get("scaler_{model_date}.pkl".format(model_date = model_date))
    local_medians_path = SparkFiles.get("train_medians_{model_date}.pkl".format(model_date = model_date))
    scaler = joblib.load(local_scaler_path)
    train_medians = joblib.load(local_medians_path)

    # 只取dense_fea那部分
    mean_ = [scaler.mean_[i] for i in range(len(scaler.mean_)) if dense_fea[i] in dense_user_feature]
    scale_ = [scaler.scale_[i] for i in range(len(scaler.scale_)) if dense_fea[i] in dense_user_feature]
    train_medians = {key: train_medians[key] for key in dense_user_feature if key in train_medians}
  
    # 新建一个StandardScaler类并赋值
    new_scaler = StandardScaler()
    new_scaler.mean_ = np.array(mean_)
    new_scaler.scale_ = np.array(scale_)
    
    return train_medians, new_scaler

medians_path = "hdfs:///user/daiyuxuan/dssm_search/models/train_medians_{model_date}.pkl".format(model_date = model_date)
scaler_path = "hdfs:///user/daiyuxuan/dssm_search/models/scaler_{model_date}.pkl".format(model_date = model_date)
print("="*10, "median、scaler加载中", "="*10)
train_medians, scaler = load_scaler_and_medians(scaler_path, medians_path, dense_user_feature, dense_fea, model_date)
print("="*10, "median、scaler加载完毕", "="*10)

print("="*10, "样本生成中", "="*10)
def gen_data(prod_date):
    yesterday = (dt.datetime.strptime(prod_date, '%Y-%m-%d') - dt.timedelta(days=1)).strftime('%Y-%m-%d')
    start_date = (dt.datetime.strptime(prod_date, '%Y-%m-%d') - dt.timedelta(days=7)).strftime('%Y-%m-%d')
    sample_sql = """
        select user_id 
        from gzlc_real.fact_ctob_user_behavior  
        where dt between '{start_date}' and '{prod_date}' -- 7天内活跃用户
        and (page_type like '%%SEARCH%%' or page_type like '%%GRAB%%') -- 今日秒杀场景看最近两天新抢拍+今日秒杀的车，可以覆盖99%
        and user_id != '' and user_id is not null
        and action = 'beseen'
        group by user_id
        -- limit 200
    """.format(start_date = start_date, prod_date = prod_date)
    user_feat_sql = """
        with user_activity_dt as (
            select
                cast(user_id as string) as user_id,
                max(case when action = 'beseen' then cnt_0_7 else 0 end) beseen_cnt_0_7,
                max(case when action = 'beseen' then cnt_7_14 else 0 end) beseen_cnt_7_14,
                max(case when action = 'beseen' then cnt_14_30 else 0 end) beseen_cnt_14_30,

                max(case when action = 'click' then cnt_0_7 else 0 end) click_cnt_0_7,
                max(case when action = 'click' then cnt_7_14 else 0 end) click_cnt_7_14,
                max(case when action = 'click' then cnt_14_30 else 0 end) click_cnt_14_30,

                max(case when action = 'bid' then cnt_0_7 else 0 end) bid_cnt_0_7,
                max(case when action = 'bid' then cnt_7_14 else 0 end) bid_cnt_7_14,
                max(case when action = 'bid' then cnt_14_30 else 0 end) bid_cnt_14_30,

                max(case when action = 'quick_collection' then cnt_0_7 else 0 end) collect_cnt_0_7,
                max(case when action = 'quick_collection' then cnt_7_14 else 0 end) collect_cnt_7_14,
                max(case when action = 'quick_collection' then cnt_14_30 else 0 end) collect_cnt_14_30
            from
                g3_feature_dev.c2b_user_action_cnt_v2
            where dt = '{yesterday}'
            group by user_id
        ),

        user_convert_cn_dt as (
          select
                cast(user_id as string) as user_id,
                cast(if(cnt_0_30 is null, 0, cnt_0_30) as double) as contract_cnt_30,
                cast(if(cnt_0_365 is null, 0, cnt_0_365) as double) as contract_cnt_365
          from g3_feature_dev.c2b_user_convert_cnt_v2
            where dt = '{yesterday}'
        ),

        user_action_statis as (
            select
                cast(user_id as string) as user_id,
                max(case when action = 'click' then suggest_price_max else 0 end) as click_suggest_price_max_0_30,
                max(case when action = 'click' then suggest_price_min else 0 end) as click_suggest_price_min_0_30,
                max(case when action = 'click' then suggest_price_p50 else 0 end) as click_suggest_price_median_0_30,
                max(case when action = 'click' then road_haul_max else 0 end) as click_road_haul_max_0_30,
                max(case when action = 'click' then road_haul_min else 0 end) as click_road_haul_min_0_30,
                max(case when action = 'click' then road_haul_p50 else 0 end) as click_road_haul_median_0_30,
                max(case when action = 'click' then evaluate_score_max else 0 end) as click_evaluate_score_max_0_30,
                max(case when action = 'click' then evaluate_score_min else 0 end) as click_evaluate_score_min_0_30,
                max(case when action = 'click' then evaluate_score_p50 else 0 end) as click_evaluate_score_median_0_30,

                max(case when action = 'bid' then suggest_price_max else 0 end) as bid_suggest_price_max_0_30,
                max(case when action = 'bid' then suggest_price_min else 0 end) as bid_suggest_price_min_0_30,
                max(case when action = 'bid' then suggest_price_p50 else 0 end) as bid_suggest_price_median_0_30,
                max(case when action = 'bid' then road_haul_max else 0 end) as bid_road_haul_max_0_30,
                max(case when action = 'bid' then road_haul_min else 0 end) as bid_road_haul_min_0_30,
                max(case when action = 'bid' then road_haul_p50 else 0 end) as bid_road_haul_median_0_30,
                max(case when action = 'bid' then evaluate_score_max else 0 end) as bid_evaluate_score_max_0_30,
                max(case when action = 'bid' then evaluate_score_min else 0 end) as bid_evaluate_score_min_0_30,
                max(case when action = 'bid' then evaluate_score_p50 else 0 end) as bid_evaluate_score_median_0_30
            from g3_feature_dev.c2b_user_action_statis_v2
            where dt = '{yesterday}'
            group by user_id

        ),

        user_action_by_id_ratio as (
            select *
            from g3_feature_dev.c2b_user_action_by_id_ratio_v2
            where dt = '{yesterday}'
        ),

        user_action_max_ratio as (
            select
                cast(user_id as string) as user_id,
                cast(cluster as string) as cluster,
                cast(action as string) as action,
                max(ratio_0_7) as max_ratio_0_7,
                max(ratio_7_14) as max_ratio_7_14,
                max(ratio_14_30) as max_ratio_14_30
            from g3_feature_dev.c2b_user_action_by_id_ratio_v2
            where dt = '{yesterday}'
            group by user_id, cluster, action
        ),

        user_action_by_id_ratio_0_7 as (
            select A.user_id,
                max(case when (concat(A.action, '_', A.cluster) = 'click_tag_id' and max_ratio_0_7 > 0) then B.key else -1 end) as click_tag_id_0_7,
                max(case when (concat(A.action, '_', A.cluster) = 'click_seats' and max_ratio_0_7 > 0) then B.key else -1 end) as click_seats_0_7,
                max(case when (concat(A.action, '_', A.cluster) = 'click_minor_category_id' and max_ratio_0_7 > 0) then B.key else -1 end) as click_minor_category_id_0_7,
                max(case when (concat(A.action, '_', A.cluster) = 'click_guobie' and max_ratio_0_7 > 0) then B.key else -1 end) as click_guobie_0_7,
                max(case when (concat(A.action, '_', A.cluster) = 'click_gearbox' and max_ratio_0_7 > 0) then B.key else -1 end) as click_gearbox_0_7,
                max(case when (concat(A.action, '_', A.cluster) = 'click_fuel_type' and max_ratio_0_7 > 0) then B.key else -1 end) as click_fuel_type_0_7,
                max(case when (concat(A.action, '_', A.cluster) = 'click_emission_standard' and max_ratio_0_7 > 0) then B.key else -1 end) as click_emission_standard_0_7,
                max(case when (concat(A.action, '_', A.cluster) = 'click_city_id' and max_ratio_0_7 > 0) then B.key else -1 end) as click_city_id_0_7,
                max(case when (concat(A.action, '_', A.cluster) = 'click_car_year' and max_ratio_0_7 > 0) then B.key else -1 end) as click_car_year_0_7,
                max(case when (concat(A.action, '_', A.cluster) = 'click_auto_type' and max_ratio_0_7 > 0) then B.key else -1 end) as click_auto_type_0_7,
                max(case when (concat(A.action, '_', A.cluster) = 'click_car_color' and max_ratio_0_7 > 0) then B.key else -1 end) as click_car_color_0_7,
                -- evaluate_level_segment原来是1~4,改成2~5
                max(case when (concat(A.action, '_', A.cluster) = 'click_evaluate_level_segment' and max_ratio_0_7 > 0) then B.key+1 else 1 end) as click_evaluate_level_segment_0_7,
                max(case when (concat(A.action, '_', A.cluster) = 'click_evaluate_score_segment' and max_ratio_0_7 > 0) then B.key else -1 end) as click_evaluate_score_segment_0_7,
                max(case when (concat(A.action, '_', A.cluster) = 'click_model_price_bin' and max_ratio_0_7 > 0) then B.key else -1 end) as click_model_price_bin_0_7,

                max(case when (concat(A.action, '_', A.cluster) = 'bid_tag_id' and max_ratio_0_7 > 0) then B.key else -1 end) as bid_tag_id_0_7,
                max(case when (concat(A.action, '_', A.cluster) = 'bid_seats' and max_ratio_0_7 > 0) then B.key else -1 end) as bid_seats_0_7,
                max(case when (concat(A.action, '_', A.cluster) = 'bid_minor_category_id' and max_ratio_0_7 > 0) then B.key else -1 end) as bid_minor_category_id_0_7,
                max(case when (concat(A.action, '_', A.cluster) = 'bid_guobie' and max_ratio_0_7 > 0) then B.key else -1 end) as bid_guobie_0_7,
                max(case when (concat(A.action, '_', A.cluster) = 'bid_gearbox' and max_ratio_0_7 > 0) then B.key else -1 end) as bid_gearbox_0_7,
                max(case when (concat(A.action, '_', A.cluster) = 'bid_fuel_type' and max_ratio_0_7 > 0) then B.key else -1 end) as bid_fuel_type_0_7,
                max(case when (concat(A.action, '_', A.cluster) = 'bid_emission_standard' and max_ratio_0_7 > 0) then B.key else -1 end) as bid_emission_standard_0_7,
                max(case when (concat(A.action, '_', A.cluster) = 'bid_city_id' and max_ratio_0_7 > 0) then B.key else -1 end) as bid_city_id_0_7,
                max(case when (concat(A.action, '_', A.cluster) = 'bid_car_year' and max_ratio_0_7 > 0) then B.key else -1 end) as bid_car_year_0_7,
                max(case when (concat(A.action, '_', A.cluster) = 'bid_auto_type' and max_ratio_0_7 > 0) then B.key else -1 end) as bid_auto_type_0_7,
                max(case when (concat(A.action, '_', A.cluster) = 'bid_car_color' and max_ratio_0_7 > 0) then B.key else -1 end) as bid_car_color_0_7,
                max(case when (concat(A.action, '_', A.cluster) = 'bid_evaluate_level_segment' and max_ratio_0_7 > 0) then B.key+1 else 1 end) as bid_evaluate_level_segment_0_7,
                max(case when (concat(A.action, '_', A.cluster) = 'bid_evaluate_score_segment' and max_ratio_0_7 > 0) then B.key else -1 end) as bid_evaluate_score_segment_0_7,
                max(case when (concat(A.action, '_', A.cluster) = 'bid_model_price_bin' and max_ratio_0_7 > 0) then B.key else -1 end) as bid_model_price_bin_0_7
            from user_action_max_ratio A
            left join user_action_by_id_ratio B on A.user_id = B.user_id and A.cluster = B.cluster and A.action = B.action and A.max_ratio_0_7 = B.ratio_0_7
            group by A.user_id
        ),

        user_convert_by_id_ratio as (
            select *
            from g3_feature_dev.c2b_user_convert_by_id_ratio_v2
            where dt = '{yesterday}'
        ),

        user_convert_max_ratio as (
            select
                cast(user_id as string) as user_id,
                cast(cluster as string) as cluster,
                max(ratio_0_30) as max_ratio_0_30,
                max(ratio_0_365) as max_ratio_0_365
            from g3_feature_dev.c2b_user_convert_by_id_ratio_v2
            where dt = '{yesterday}'
            group by user_id, cluster
        ),

        user_convert_by_id_ratio_0_30 as (
            select A.user_id,
                max(case when A.cluster = 'tag_id' and max_ratio_0_30 > 0 then B.key else -1 end) as deal_tag_id_0_30,
                max(case when A.cluster = 'seats' and max_ratio_0_30 > 0 then B.key else -1 end) as deal_seats_0_30,
                max(case when A.cluster = 'minor_category_id' and max_ratio_0_30 > 0 then B.key else -1 end) as deal_minor_category_id_0_30,
                max(case when A.cluster = 'guobie' and max_ratio_0_30 > 0 then B.key else -1 end) as deal_guobie_0_30,
                max(case when A.cluster = 'gearbox' and max_ratio_0_30 > 0 then B.key else -1 end) as deal_gearbox_0_30,
                max(case when A.cluster = 'fuel_type' and max_ratio_0_30 > 0 then B.key else -1 end) as deal_fuel_type_0_30,
                max(case when A.cluster = 'emission_standard' and max_ratio_0_30 > 0 then B.key else -1 end) as deal_emission_standard_0_30,
                max(case when A.cluster = 'city_id' and max_ratio_0_30 > 0 then B.key else -1 end) as deal_city_id_0_30,
                max(case when A.cluster = 'car_year' and max_ratio_0_30 > 0 then B.key else -1 end) as deal_car_year_0_30,
                max(case when A.cluster = 'auto_type' and max_ratio_0_30 > 0 then B.key else -1 end) as deal_auto_type_0_30,
                max(case when A.cluster = 'car_color' and max_ratio_0_30 > 0 then B.key else -1 end) as deal_car_color_0_30
            from user_convert_max_ratio A
            left join user_convert_by_id_ratio B on A.user_id = B.user_id and A.cluster = B.cluster and A.max_ratio_0_30 = B.ratio_0_30
            group by A.user_id
        ),

        user_convert_by_id_ratio_0_365 as (
            select A.user_id,
                max(case when A.cluster = 'tag_id' and max_ratio_0_365 > 0 then B.key else -1 end) as deal_tag_id_0_365,
                max(case when A.cluster = 'seats' and max_ratio_0_365 > 0 then B.key else -1 end) as deal_seats_0_365,
                max(case when A.cluster = 'minor_category_id' and max_ratio_0_365 > 0 then B.key else -1 end) as deal_minor_category_id_0_365,
                max(case when A.cluster = 'guobie' and max_ratio_0_365 > 0 then B.key else -1 end) as deal_guobie_0_365,
                max(case when A.cluster = 'gearbox' and max_ratio_0_365 > 0 then B.key else -1 end) as deal_gearbox_0_365,
                max(case when A.cluster = 'fuel_type' and max_ratio_0_365 > 0 then B.key else -1 end) as deal_fuel_type_0_365,
                max(case when A.cluster = 'emission_standard' and max_ratio_0_365 > 0 then B.key else -1 end) as deal_emission_standard_0_365,
                max(case when A.cluster = 'city_id' and max_ratio_0_365 > 0 then B.key else -1 end) as deal_city_id_0_365,
                max(case when A.cluster = 'car_year' and max_ratio_0_365 > 0 then B.key else -1 end) as deal_car_year_0_365,
                max(case when A.cluster = 'auto_type' and max_ratio_0_365 > 0 then B.key else -1 end) as deal_auto_type_0_365,
                max(case when A.cluster = 'car_color' and max_ratio_0_365 > 0 then B.key else -1 end) as deal_car_color_0_365
            from user_convert_max_ratio A
            left join user_convert_by_id_ratio B on A.user_id = B.user_id and A.cluster = B.cluster and A.max_ratio_0_365 = B.ratio_0_365
            group by A.user_id
        ),

        lookup_table as (
            select *
            from g3_feature_dev.c2b_encoding_lookup_table
            where dt = '2024-06-04'
        )

        select cast(A.user_id as int) user_id
            -- dense类
            ,beseen_cnt_0_7 as d_u_beseen_cnt_0_7
            ,beseen_cnt_7_14 as d_u_beseen_cnt_7_14
            ,beseen_cnt_14_30 as d_u_beseen_cnt_14_30
            ,click_cnt_0_7 as d_u_click_cnt_0_7
            ,click_cnt_7_14 as d_u_click_cnt_7_14
            ,click_cnt_14_30 as d_u_click_cnt_14_30
            ,bid_cnt_0_7 as d_u_bid_cnt_0_7
            ,bid_cnt_7_14 as d_u_bid_cnt_7_14
            ,bid_cnt_14_30 as d_u_bid_cnt_14_30
            ,collect_cnt_0_7 as d_u_collect_cnt_0_7
            ,collect_cnt_7_14 as d_u_collect_cnt_7_14
            ,collect_cnt_14_30 as d_u_collect_cnt_14_30
            ,contract_cnt_30 as d_u_contract_cnt_30
            ,contract_cnt_365 as d_u_contract_cnt_365
            ,click_suggest_price_max_0_30 as d_u_click_suggest_price_max_0_30
            ,click_suggest_price_min_0_30 as d_u_click_suggest_price_min_0_30
            ,click_suggest_price_median_0_30 as d_u_click_suggest_price_median_0_30
            ,click_road_haul_max_0_30 as d_u_click_road_haul_max_0_30
            ,click_road_haul_min_0_30 as d_u_click_road_haul_min_0_30
            ,click_road_haul_median_0_30 as d_u_click_road_haul_median_0_30
            ,click_evaluate_score_max_0_30 as d_u_click_evaluate_score_max_0_30
            ,click_evaluate_score_min_0_30 as d_u_click_evaluate_score_min_0_30
            ,click_evaluate_score_median_0_30 as d_u_click_evaluate_score_median_0_30
            ,bid_suggest_price_max_0_30 as d_u_bid_suggest_price_max_0_30
            ,bid_suggest_price_min_0_30 as d_u_bid_suggest_price_min_0_30
            ,bid_suggest_price_median_0_30 as d_u_bid_suggest_price_median_0_30
            ,bid_road_haul_max_0_30 as d_u_bid_road_haul_max_0_30
            ,bid_road_haul_min_0_30 as d_u_bid_road_haul_min_0_30
            ,bid_road_haul_median_0_30 as d_u_bid_road_haul_median_0_30
            ,bid_evaluate_score_max_0_30 as d_u_bid_evaluate_score_max_0_30
            ,bid_evaluate_score_min_0_30 as d_u_bid_evaluate_score_min_0_30
            ,bid_evaluate_score_median_0_30 as d_u_bid_evaluate_score_median_0_30
            -- sparse类特征
            ,if(L1.encode_id is null, 1, L1.encode_id) as s_u_click_tag_id_0_7
            ,if(L2.encode_id is null, 1, L2.encode_id) as s_u_click_seats_0_7
            ,if(L3.encode_id is null, 1, L3.encode_id) as s_u_click_minor_category_id_0_7
            ,if(L4.encode_id is null, 1, L4.encode_id) as s_u_click_guobie_0_7
            ,if(L5.encode_id is null, 1, L5.encode_id) as s_u_click_gearbox_0_7
            ,if(L6.encode_id is null, 1, L6.encode_id) as s_u_click_fuel_type_0_7
            ,if(L7.encode_id is null, 1, L7.encode_id) as s_u_click_emission_standard_0_7
            ,if(L8.encode_id is null, 1, L8.encode_id) as s_u_click_city_id_0_7
            ,if(L9.encode_id is null, 1, L9.encode_id) as s_u_click_car_year_0_7
            ,if(L10.encode_id is null, 1, L10.encode_id) as s_u_click_auto_type_0_7
            ,if(L11.encode_id is null, 1, L11.encode_id) as s_u_click_car_color_0_7
            ,if(add1.encode_id is null, 1, add1.encode_id) as s_u_click_evaluate_level_segment_0_7
            ,if(add2.encode_id is null, 1, add2.encode_id) as s_u_click_evaluate_score_segment_0_7
            ,if(add3.encode_id is null, 1, add3.encode_id) as s_u_click_model_price_bin_0_7

            ,if(L12.encode_id is null, 1, L12.encode_id) as s_u_bid_tag_id_0_7
            ,if(L13.encode_id is null, 1, L13.encode_id) as s_u_bid_seats_0_7
            ,if(L14.encode_id is null, 1, L14.encode_id) as s_u_bid_minor_category_id_0_7
            ,if(L15.encode_id is null, 1, L15.encode_id) as s_u_bid_guobie_0_7
            ,if(L16.encode_id is null, 1, L16.encode_id) as s_u_bid_gearbox_0_7
            ,if(L17.encode_id is null, 1, L17.encode_id) as s_u_bid_fuel_type_0_7
            ,if(L18.encode_id is null, 1, L18.encode_id) as s_u_bid_emission_standard_0_7
            ,if(L19.encode_id is null, 1, L19.encode_id) as s_u_bid_city_id_0_7
            ,if(L20.encode_id is null, 1, L20.encode_id) as s_u_bid_car_year_0_7
            ,if(L21.encode_id is null, 1, L21.encode_id) as s_u_bid_auto_type_0_7
            ,if(L22.encode_id is null, 1, L22.encode_id) as s_u_bid_car_color_0_7
            ,if(add4.encode_id is null, 1, add4.encode_id) as s_u_bid_evaluate_level_segment_0_7
            ,if(add5.encode_id is null, 1, add5.encode_id) as s_u_bid_evaluate_score_segment_0_7
            ,if(add6.encode_id is null, 1, add6.encode_id) as s_u_bid_model_price_bin_0_7

            ,if(L23.encode_id is null, 1, L23.encode_id) as s_u_deal_tag_id_0_30
            ,if(L24.encode_id is null, 1, L24.encode_id) as s_u_deal_seats_0_30
            ,if(L25.encode_id is null, 1, L25.encode_id) as s_u_deal_minor_category_id_0_30
            ,if(L26.encode_id is null, 1, L26.encode_id) as s_u_deal_guobie_0_30
            ,if(L27.encode_id is null, 1, L27.encode_id) as s_u_deal_gearbox_0_30
            ,if(L28.encode_id is null, 1, L28.encode_id) as s_u_deal_fuel_type_0_30
            ,if(L29.encode_id is null, 1, L29.encode_id) as s_u_deal_emission_standard_0_30
            ,if(L30.encode_id is null, 1, L30.encode_id) as s_u_deal_city_id_0_30
            ,if(L31.encode_id is null, 1, L31.encode_id) as s_u_deal_car_year_0_30
            ,if(L32.encode_id is null, 1, L32.encode_id) as s_u_deal_auto_type_0_30
            ,if(L33.encode_id is null, 1, L33.encode_id) as s_u_deal_car_color_0_30

            ,if(L34.encode_id is null, 1, L34.encode_id) as s_u_deal_tag_id_0_365
            ,if(L35.encode_id is null, 1, L35.encode_id) as s_u_deal_seats_0_365
            ,if(L36.encode_id is null, 1, L36.encode_id) as s_u_deal_minor_category_id_0_365
            ,if(L37.encode_id is null, 1, L37.encode_id) as s_u_deal_guobie_0_365
            ,if(L38.encode_id is null, 1, L38.encode_id) as s_u_deal_gearbox_0_365
            ,if(L39.encode_id is null, 1, L39.encode_id) as s_u_deal_fuel_type_0_365
            ,if(L40.encode_id is null, 1, L40.encode_id) as s_u_deal_emission_standard_0_365
            ,if(L41.encode_id is null, 1, L41.encode_id) as s_u_deal_city_id_0_365
            ,if(L42.encode_id is null, 1, L42.encode_id) as s_u_deal_car_year_0_365
            ,if(L43.encode_id is null, 1, L43.encode_id) as s_u_deal_auto_type_0_365
            ,if(L44.encode_id is null, 1, L44.encode_id) as s_u_deal_car_color_0_365

        from user_activity_dt A
        left join user_convert_cn_dt B on A.user_id = B.user_id
        left join user_action_statis C on A.user_id = C.user_id
        left join user_action_by_id_ratio_0_7 D on A.user_id = D.user_id
        left join user_convert_by_id_ratio_0_30 E on A.user_id = E.user_id
        left join user_convert_by_id_ratio_0_365 F on A.user_id = F.user_id
        left join (select fea_val, encode_id from lookup_table where fea_name = 'tag_id' ) L1 on L1.fea_val = cast(D.click_tag_id_0_7 as string)
        left join (select fea_val, encode_id from lookup_table where fea_name = 'seats' ) L2 on L2.fea_val = cast(D.click_seats_0_7 as string)
        left join (select fea_val, encode_id from lookup_table where fea_name = 'minor_category_id' ) L3 on L3.fea_val = cast(D.click_minor_category_id_0_7 as string)
        left join (select fea_val, encode_id from lookup_table where fea_name = 'guobie' ) L4 on L4.fea_val = cast(D.click_guobie_0_7 as string)
        left join (select fea_val, encode_id from lookup_table where fea_name = 'gearbox' ) L5 on L5.fea_val = cast(D.click_gearbox_0_7 as string)
        left join (select fea_val, encode_id from lookup_table where fea_name = 'fuel_type' ) L6 on L6.fea_val = cast(D.click_fuel_type_0_7 as string)
        left join (select fea_val, encode_id from lookup_table where fea_name = 'emission_standard' ) L7 on L7.fea_val = cast(D.click_emission_standard_0_7 as string)
        left join (select fea_val, encode_id from lookup_table where fea_name = 'city_id' ) L8 on L8.fea_val = cast(D.click_city_id_0_7 as string)
        left join (select fea_val, encode_id from lookup_table where fea_name = 'car_year' ) L9 on L9.fea_val = cast(D.click_car_year_0_7 as string)
        left join (select fea_val, encode_id from lookup_table where fea_name = 'auto_type' ) L10 on L10.fea_val = cast(D.click_auto_type_0_7 as string)
        left join (select fea_val, encode_id from lookup_table where fea_name = 'car_color' ) L11 on L11.fea_val = cast(D.click_car_color_0_7 as string)
        left join (select fea_val, encode_id from lookup_table where fea_name = 'evaluate_level_segment' ) add1 on add1.fea_val = cast(D.click_evaluate_level_segment_0_7 as string)
        left join (select fea_val, encode_id from lookup_table where fea_name = 'evaluate_score_segment' ) add2 on add2.fea_val = cast(D.click_evaluate_score_segment_0_7 as string)
        left join (select fea_val, encode_id from lookup_table where fea_name = 'model_price_bin' ) add3 on add3.fea_val = cast(D.click_model_price_bin_0_7 as string)

        left join (select fea_val, encode_id from lookup_table where fea_name = 'tag_id' ) L12 on L12.fea_val = cast(D.bid_tag_id_0_7 as string)
        left join (select fea_val, encode_id from lookup_table where fea_name = 'seats' ) L13 on L13.fea_val = cast(D.bid_seats_0_7 as string)
        left join (select fea_val, encode_id from lookup_table where fea_name = 'minor_category_id' ) L14 on L14.fea_val = cast(D.bid_minor_category_id_0_7 as string)
        left join (select fea_val, encode_id from lookup_table where fea_name = 'guobie' ) L15 on L15.fea_val = cast(D.bid_guobie_0_7 as string)
        left join (select fea_val, encode_id from lookup_table where fea_name = 'gearbox' ) L16 on L16.fea_val = cast(D.bid_gearbox_0_7 as string)
        left join (select fea_val, encode_id from lookup_table where fea_name = 'fuel_type' ) L17 on L17.fea_val = cast(D.bid_fuel_type_0_7 as string)
        left join (select fea_val, encode_id from lookup_table where fea_name = 'emission_standard' ) L18 on L18.fea_val = cast(D.bid_emission_standard_0_7 as string)
        left join (select fea_val, encode_id from lookup_table where fea_name = 'city_id' ) L19 on L19.fea_val = cast(D.bid_city_id_0_7 as string)
        left join (select fea_val, encode_id from lookup_table where fea_name = 'car_year' ) L20 on L20.fea_val = cast(D.bid_car_year_0_7 as string)
        left join (select fea_val, encode_id from lookup_table where fea_name = 'auto_type' ) L21 on L21.fea_val = cast(D.bid_auto_type_0_7 as string)
        left join (select fea_val, encode_id from lookup_table where fea_name = 'car_color' ) L22 on L22.fea_val = cast(D.bid_car_color_0_7 as string)
        left join (select fea_val, encode_id from lookup_table where fea_name = 'evaluate_level_segment' ) add4 on add4.fea_val = cast(D.bid_evaluate_level_segment_0_7 as string)
        left join (select fea_val, encode_id from lookup_table where fea_name = 'evaluate_score_segment' ) add5 on add5.fea_val = cast(D.bid_evaluate_score_segment_0_7 as string)
        left join (select fea_val, encode_id from lookup_table where fea_name = 'model_price_bin' ) add6 on add6.fea_val = cast(D.bid_model_price_bin_0_7 as string)

        left join (select fea_val, encode_id from lookup_table where fea_name = 'tag_id' ) L23 on L23.fea_val = cast(E.deal_tag_id_0_30 as string)
        left join (select fea_val, encode_id from lookup_table where fea_name = 'seats' ) L24 on L24.fea_val = cast(E.deal_seats_0_30 as string)
        left join (select fea_val, encode_id from lookup_table where fea_name = 'minor_category_id' ) L25 on L25.fea_val = cast(E.deal_minor_category_id_0_30 as string)
        left join (select fea_val, encode_id from lookup_table where fea_name = 'guobie' ) L26 on L26.fea_val = cast(E.deal_guobie_0_30 as string)
        left join (select fea_val, encode_id from lookup_table where fea_name = 'gearbox' ) L27 on L27.fea_val = cast(E.deal_gearbox_0_30 as string)
        left join (select fea_val, encode_id from lookup_table where fea_name = 'fuel_type' ) L28 on L28.fea_val = cast(E.deal_fuel_type_0_30 as string)
        left join (select fea_val, encode_id from lookup_table where fea_name = 'emission_standard' ) L29 on L29.fea_val = cast(E.deal_emission_standard_0_30 as string)
        left join (select fea_val, encode_id from lookup_table where fea_name = 'city_id' ) L30 on L30.fea_val = cast(E.deal_city_id_0_30 as string)
        left join (select fea_val, encode_id from lookup_table where fea_name = 'car_year' ) L31 on L31.fea_val = cast(E.deal_car_year_0_30 as string)
        left join (select fea_val, encode_id from lookup_table where fea_name = 'auto_type' ) L32 on L32.fea_val = cast(E.deal_auto_type_0_30 as string)
        left join (select fea_val, encode_id from lookup_table where fea_name = 'car_color' ) L33 on L33.fea_val = cast(E.deal_car_color_0_30 as string)
        left join (select fea_val, encode_id from lookup_table where fea_name = 'tag_id' ) L34 on L34.fea_val = cast(F.deal_tag_id_0_365 as string)
        left join (select fea_val, encode_id from lookup_table where fea_name = 'seats' ) L35 on L35.fea_val = cast(F.deal_seats_0_365 as string)
        left join (select fea_val, encode_id from lookup_table where fea_name = 'minor_category_id' ) L36 on L36.fea_val = cast(F.deal_minor_category_id_0_365 as string)
        left join (select fea_val, encode_id from lookup_table where fea_name = 'guobie' ) L37 on L37.fea_val = cast(F.deal_guobie_0_365 as string)
        left join (select fea_val, encode_id from lookup_table where fea_name = 'gearbox' ) L38 on L38.fea_val = cast(F.deal_gearbox_0_365 as string)
        left join (select fea_val, encode_id from lookup_table where fea_name = 'fuel_type' ) L39 on L39.fea_val = cast(F.deal_fuel_type_0_365 as string)
        left join (select fea_val, encode_id from lookup_table where fea_name = 'emission_standard' ) L40 on L40.fea_val = cast(F.deal_emission_standard_0_365 as string)
        left join (select fea_val, encode_id from lookup_table where fea_name = 'city_id' ) L41 on L41.fea_val = cast(F.deal_city_id_0_365 as string)
        left join (select fea_val, encode_id from lookup_table where fea_name = 'car_year' ) L42 on L42.fea_val = cast(F.deal_car_year_0_365 as string)
        left join (select fea_val, encode_id from lookup_table where fea_name = 'auto_type' ) L43 on L43.fea_val = cast(F.deal_auto_type_0_365 as string)
        left join (select fea_val, encode_id from lookup_table where fea_name = 'car_color' ) L44 on L44.fea_val = cast(F.deal_car_color_0_365 as string)
    """.format(yesterday = yesterday)     

    sample_df = sqlContext.sql(sample_sql)
    user_feat_df = sqlContext.sql(user_feat_sql)
    return sample_df, user_feat_df

parser = argparse.ArgumentParser()
parser.add_argument('-d', '--prod_date', default=(dt.datetime.today() - dt.timedelta(days=1)).strftime('%Y-%m-%d'),
                    type=str, required=False, help="prod date")
arg = parser.parse_args()
prod_date = arg.prod_date
raw_sample_df, user_feat_df = gen_data(prod_date)
sample_df = raw_sample_df.join(user_feat_df, 'user_id', 'left')
print("="*10, "样本生成完毕", "="*10)

print("="*10, "样本预处理中", "="*10)
sample_df = sample_df.toPandas()
for col in dense_user_feature: # 填补dense特征
    sample_df[col].fillna(train_medians[col], inplace=True)
sample_df[dense_user_feature] = sample_df[dense_user_feature].astype('float32', copy=False)
sample_df[sparse_user_feature] = sample_df[sparse_user_feature].fillna(1) # 填补sparse特征
sample_df[sparse_user_feature] = sample_df[sparse_user_feature].astype('int32', copy=False)

# 归一化
dense_data = sample_df[dense_user_feature].values
dense_data_normalized = scaler.transform(dense_data)
sample_df[dense_user_feature] = dense_data_normalized
print("="*10, "样本预处理完毕", "="*10)

print("="*10, "输出embedding中", "="*10)

sample_df["original_user_id"] = sample_df["user_id"]
user_id = sample_df["original_user_id"]
sample_df["user_id"] = sample_df["user_id"].apply(lambda x: hash(x) % 8001)  # 对user_id进行哈希处理
user_input = [sample_df[fea].values.reshape(-1, 1).astype(np.float32) for fea in user_fea]
user_embedding = user_layer_model.predict(user_input)
user_embedding_list = [embedding.tolist() for embedding in user_embedding]
new_embedding_df = pd.DataFrame({"user_id": user_id.values.astype(str), "user_embedding": user_embedding_list})

print(new_embedding_df.head(3))

def redis_list(redis_str):
    """
    集群处理
    :return:
    """
    REDIS_NODES = []
    if "," in redis_str:
        lines = redis_str.split(",")
        for i in lines:
            line = i.split(":")
            REDIS_NODES.append({"host": line[0], "port": line[1]})
    else:
        line = redis_str.split(":")
        REDIS_NODES.append({"host": line[0], "port": line[1]})
    return REDIS_NODES

def redis_conn(redis_str):
    """
    连接redis集群
    :return:a
    """
    REDIS_NODES = redis_list(redis_str)
    req = None
    try:
        req = RedisCluster(
            startup_nodes=REDIS_NODES, max_connections=100, decode_responses=True
        )
    except Exception as e:
        print("conn error:{}".format(e))
    return req

def insert_user(dct):
    redis_str_1 = "10.16.18.63:13000,10.16.18.64:13000,10.16.18.65:13000,10.16.18.63:13001,10.16.18.64:13001,10.16.18.65:13001"
    redis_str_2 = "10.16.9.80:4581,10.16.9.81:4581,10.16.9.82:4581,10.16.9.83:4581,10.16.9.84:4581,10.16.9.85:4581,10.16.9.87:4581,10.16.9.88:4581"
    
    conn_1 = redis_conn(redis_str_1)
    conn_2 = redis_conn(redis_str_2)
    
    cnt = 0
    for user_id, embedding in dct.items():
        key = "g3:c2b_user_vector:c2b_dssm_user_model_dyx:{user_id}".format(user_id=user_id)
        embedding_str = json.dumps(embedding)  # 将列表转换为JSON字符串
        
        # 插入到第一个 Redis 集群
        conn_1.set(key, embedding_str)
        conn_1.expire(key, 604800)
        
        # 插入到第二个 Redis 集群
        conn_2.set(key, embedding_str)
        conn_2.expire(key, 604800)
        
        if cnt % 500 == 0 and cnt != 0:
            time.sleep(1)
            print("sleep 1 s")
        cnt += 1
    print("all insert user_id: ", cnt)

try:  # 读取已有的embedding
    path = "hdfs:///user/daiyuxuan/dssm_search/embedding"
    full_path = "{path}/user_{prod_date}/".format(path=path, prod_date=prod_date)
    old_embedding_df = spark.read.csv(full_path, sep='\t', header=True) 
    old_embedding_pd_df = old_embedding_df.toPandas()
    old_embedding_dct = old_embedding_pd_df.set_index('user_id')['user_embedding'].to_dict()
    old_embedding_dct = {k: json.loads(v) if isinstance(v, str) else v for k, v in old_embedding_dct.items()}
    print("Old Embedding Dictionary Size:", len(old_embedding_dct))
    print("打印5条old_embedding_dct的key-value对")
    for i, (key, value) in enumerate(old_embedding_dct.items()):
        if i >= 5:
            break
        print(f"Key: {key}, Value: {value}")
except Exception as e:
    old_embedding_dct = {}
    print(f"Error reading old embeddings: {e}")
    pass

new_embedding_dct = new_embedding_df.set_index('user_id')['user_embedding'].to_dict()
# 检查 old_embedding_dct 是否为空
if old_embedding_dct:
    print("Old Embedding Dictionary Key Type:", type(next(iter(old_embedding_dct.keys()))))
    print("Old Embedding Dictionary Value Type:", type(next(iter(old_embedding_dct.values()))))
else:
    print("Old Embedding Dictionary is empty")

print("New Embedding Dictionary Key Type:", type(next(iter(new_embedding_dct.keys()))))
print("New Embedding Dictionary Value Type:", type(next(iter(new_embedding_dct.values()))))

update_embedding_dct = {}
print_cnt, new_user_cnt, diff_user_cnt = 0, 0, 0
def cosine_similarity(a, b):
    return np.dot(a, b) / (norm(a) * norm(b))
for key, new_value in new_embedding_dct.items():
    if key not in old_embedding_dct:
        update_embedding_dct[key] = new_value
        new_user_cnt += 1
        if print_cnt < 5:
            print("new_user")
            print("user_id: {}".format(key))
            print("embedding: {}".format(new_value))
            print_cnt += 1
    elif cosine_similarity(old_embedding_dct[key], new_value) < 1 - 1e-6:
        update_embedding_dct[key] = new_value
        diff_user_cnt += 1
        if print_cnt < 10:
            print("diff_embbeding")
            print("user_id: {}".format(key))
            print("old_embbeding: {}".format(old_embedding_dct[key]))
            print("new_embbeding: {}".format(new_value))
            print_cnt += 1
print("update_embedding length", len(update_embedding_dct.keys()))
print("new_user_cnt", new_user_cnt)
print("diff_user_cnt", diff_user_cnt)
insert_user(update_embedding_dct)

spark_embedding_df = spark.createDataFrame(new_embedding_df)

print("="*10, "DataFrame Schema", "="*10)
def array_to_string(my_list):
    return '[' + ','.join([str(elem) for elem in my_list]) + ']'
array_to_string_udf = udf(array_to_string, StringType())
spark_embedding_df = spark_embedding_df.withColumn("user_embedding_str", array_to_string_udf(spark_embedding_df["user_embedding"]))
spark_embedding_df.printSchema()

spark_embedding_df.drop("user_embedding").withColumnRenamed("user_embedding_str", "user_embedding").repartition(1).write.csv(
    path="{path}/user_{prod_date}/".format(path=path, prod_date=prod_date),
    header=True,
    sep="\t",
    mode="overwrite",
)

print("="*10, "输出embedding完毕", "="*10)
sc.stop()