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
from numpy.linalg import norm
import json
import sys
from pyspark import SparkContext
from pyspark.sql import HiveContext
import datetime as dt
from pyspark.sql import SparkSession
import argparse
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
from tensorflow.keras.models import load_model
from tensorflow.keras.layers import Flatten
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import roc_auc_score
from collections import defaultdict
import joblib

sc = SparkContext(
    appName="c2b_dssm_item_search%s@dyx" % (dt.datetime.now().strftime("%Y%m%d"))
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

sparse_user_feature = ['s_u_click_tag_id_0_7', 's_u_click_seats_0_7', 's_u_click_minor_category_id_0_7', 's_u_click_guobie_0_7', 's_u_click_gearbox_0_7', 's_u_click_fuel_type_0_7', 's_u_click_emission_standard_0_7', 's_u_click_city_id_0_7', 's_u_click_car_year_0_7', 's_u_click_auto_type_0_7', 's_u_click_car_color_0_7', 's_u_click_c2b_evaluate_level_segment_0_7', 's_u_click_c2b_evaluate_score_segment_0_7', 's_u_click_model_price_bin_0_7', 's_u_bid_tag_id_0_7', 's_u_bid_seats_0_7', 's_u_bid_minor_category_id_0_7', 's_u_bid_guobie_0_7', 's_u_bid_gearbox_0_7', 's_u_bid_fuel_type_0_7', 's_u_bid_emission_standard_0_7', 's_u_bid_city_id_0_7', 's_u_bid_car_year_0_7', 's_u_bid_auto_type_0_7', 's_u_bid_car_color_0_7', 's_u_bid_c2b_evaluate_level_segment_0_7', 's_u_bid_c2b_evaluate_score_segment_0_7', 's_u_bid_model_price_bin_0_7', 's_u_deal_tag_id_0_30', 's_u_deal_seats_0_30', 's_u_deal_minor_category_id_0_30', 's_u_deal_guobie_0_30', 's_u_deal_gearbox_0_30', 's_u_deal_fuel_type_0_30', 's_u_deal_emission_standard_0_30', 's_u_deal_city_id_0_30', 's_u_deal_car_year_0_30', 's_u_deal_auto_type_0_30', 's_u_deal_car_color_0_30', 's_u_deal_tag_id_0_365', 's_u_deal_seats_0_365', 's_u_deal_minor_category_id_0_365', 's_u_deal_guobie_0_365', 's_u_deal_gearbox_0_365', 's_u_deal_fuel_type_0_365', 's_u_deal_emission_standard_0_365', 's_u_deal_city_id_0_365', 's_u_deal_car_year_0_365', 's_u_deal_auto_type_0_365', 's_u_deal_car_color_0_365', 'user_id']
dense_user_feature = ['d_u_beseen_cnt_0_7', 'd_u_beseen_cnt_7_14', 'd_u_beseen_cnt_14_30', 'd_u_click_cnt_0_7', 'd_u_click_cnt_7_14', 'd_u_click_cnt_14_30', 'd_u_bid_cnt_0_7', 'd_u_bid_cnt_7_14', 'd_u_bid_cnt_14_30', 'd_u_contract_cnt_30', 'd_u_contract_cnt_365', 'd_u_click_road_haul_max_0_7', 'd_u_click_road_haul_min_0_7', 'd_u_click_road_haul_median_0_7', 'd_u_click_evaluate_score_max_0_7', 'd_u_click_evaluate_score_min_0_7', 'd_u_click_evaluate_score_median_0_7', 'd_u_bid_road_haul_max_0_7', 'd_u_bid_road_haul_min_0_7', 'd_u_bid_road_haul_median_0_7', 'd_u_bid_evaluate_score_max_0_7', 'd_u_bid_evaluate_score_min_0_7', 'd_u_bid_evaluate_score_median_0_7', 'd_u_click_road_haul_max_0_30', 'd_u_click_road_haul_min_0_30', 'd_u_click_road_haul_median_0_30', 'd_u_click_evaluate_score_max_0_30', 'd_u_click_evaluate_score_min_0_30', 'd_u_click_evaluate_score_median_0_30', 'd_u_bid_road_haul_max_0_30', 'd_u_bid_road_haul_min_0_30', 'd_u_bid_road_haul_median_0_30', 'd_u_bid_evaluate_score_max_0_30', 'd_u_bid_evaluate_score_min_0_30', 'd_u_bid_evaluate_score_median_0_30', 'd_u_deal_road_haul_max_0_30', 'd_u_deal_road_haul_min_0_30', 'd_u_deal_road_haul_median_0_30', 'd_u_deal_evaluate_score_max_0_30', 'd_u_deal_evaluate_score_min_0_30', 'd_u_deal_evaluate_score_median_0_30', 'd_u_deal_road_haul_max_0_365', 'd_u_deal_road_haul_min_0_365', 'd_u_deal_road_haul_median_0_365', 'd_u_deal_evaluate_score_max_0_365', 'd_u_deal_evaluate_score_min_0_365', 'd_u_deal_evaluate_score_median_0_365', 'd_u_c2b_realtime_user_road_haul_avg_fix', 'd_u_c2b_realtime_user_evaluate_score_avg', 'd_u_c2b_realtime_user_seller_price_avg', 'd_u_c2b_realtime_user_model_price_avg', 'd_u_c2b_realtime_user_diff_price_avg', 'd_u_c2b_realtime_user_road_haul_max_fix', 'd_u_c2b_realtime_user_evaluate_score_max', 'd_u_c2b_realtime_user_seller_price_max', 'd_u_c2b_realtime_user_model_price_max', 'd_u_c2b_realtime_user_diff_price_max', 'd_u_c2b_realtime_user_road_haul_min_fix', 'd_u_c2b_realtime_user_evaluate_score_min', 'd_u_c2b_realtime_user_seller_price_min', 'd_u_c2b_realtime_user_model_price_min', 'd_u_c2b_realtime_user_diff_price_min', 'd_u_c2b_realtime_user_road_haul_p50_fix', 'd_u_c2b_realtime_user_evaluate_score_p50', 'd_u_c2b_realtime_user_seller_price_p50', 'd_u_c2b_realtime_user_model_price_p50', 'd_u_c2b_realtime_user_diff_price_p50']
sparse_item_feature = ['s_i_c2b_evaluate_score_segment', 's_i_city_id', 's_i_emission_standard', 's_i_minor_category_id', 's_i_model_price_bin', 's_i_car_year', 's_i_gearbox', 's_i_air_displacement', 's_i_tag_id', 's_i_car_color', 's_i_guobie', 's_i_c2b_evaluate_level', 's_i_auto_type', 's_i_fuel_type', 's_i_seats', 's_i_c2b_ctob_car_level']
dense_item_feature = ['d_i_c2b_offline_car_click_bid_rate_0_3_d', 'd_i_c2b_offline_car_click_bid_rate_0_30_d', 'd_i_c2b_offline_car_beseen_click_rate_0_3_d', 'd_i_c2b_offline_car_beseen_click_rate_0_30_d', 'd_i_c2b_offline_car_tag_id_bid_percent_ratio_0_7_d', 'd_i_c2b_offline_car_tag_id_bid_percent_ratio_7_14_d', 'd_i_c2b_offline_car_tag_id_bid_percent_ratio_14_30_d', 'd_i_c2b_offline_car_tag_id_click_rate_0_7_d', 'd_i_c2b_offline_car_tag_id_click_rate_7_14_d', 'd_i_c2b_offline_car_tag_id_bid_rate_0_7_d', 'd_i_c2b_offline_car_tag_id_bid_rate_7_14_d', 'd_i_c2b_offline_car_minor_category_id_click_rate_0_7_d', 'd_i_transfer_num', 'd_i_c2b_evaluate_score', 'd_i_c2b_ctob_model_price', 'd_i_c2b_seller_price', 'd_i_c2b_ctob_diff_price', 'd_i_road_haul']
sparse_sequence_features = ['s_seq_c2b_click_fuel_type_seq', 's_seq_c2b_click_car_year_seq', 's_seq_c2b_click_gearbox_seq', 's_seq_c2b_click_emission_standard_seq', 's_seq_c2b_click_tag_id_seq', 's_seq_c2b_click_city_id_seq', 's_seq_c2b_click_car_color_seq', 's_seq_c2b_click_guobie_seq', 's_seq_c2b_click_minor_category_id_seq', 's_seq_c2b_click_auto_type_seq', 's_seq_c2b_click_seats_seq', 's_seq_c2b_click_evaluate_level_seq', 's_seq_c2b_click_c2b_evaluate_score_segment_seq', 's_seq_c2b_click_c2b_ctob_car_level_seq', 's_seq_c2b_click_model_price_bin_seq', 's_seq_c2b_bid_fuel_type_seq', 's_seq_c2b_bid_car_year_seq', 's_seq_c2b_bid_gearbox_seq', 's_seq_c2b_bid_emission_standard_seq', 's_seq_c2b_bid_tag_id_seq', 's_seq_c2b_bid_city_id_seq', 's_seq_c2b_bid_car_color_seq', 's_seq_c2b_bid_guobie_seq', 's_seq_c2b_bid_minor_category_id_seq', 's_seq_c2b_bid_auto_type_seq', 's_seq_c2b_bid_seats_seq', 's_seq_c2b_bid_evaluate_level_seq', 's_seq_c2b_bid_c2b_evaluate_score_segment_seq', 's_seq_c2b_bid_c2b_ctob_car_level_seq', 's_seq_c2b_bid_model_price_bin_seq', 's_seq_c2b_contract_fuel_type_seq', 's_seq_c2b_contract_car_year_seq', 's_seq_c2b_contract_gearbox_seq', 's_seq_c2b_contract_emission_standard_seq', 's_seq_c2b_contract_tag_id_seq', 's_seq_c2b_contract_city_id_seq', 's_seq_c2b_contract_car_color_seq', 's_seq_c2b_contract_guobie_seq', 's_seq_c2b_contract_minor_category_id_seq', 's_seq_c2b_contract_auto_type_seq', 's_seq_c2b_contract_seats_seq', 's_seq_c2b_contract_evaluate_level_seq', 's_seq_c2b_contract_c2b_evaluate_score_segment_seq', 's_seq_c2b_contract_c2b_ctob_car_level_seq', 's_seq_c2b_contract_model_price_bin_seq']
dense_sequence_features = ['d_seq_c2b_click_c2b_seller_price_seq', 'd_seq_c2b_click_road_haul_seq', 'd_seq_c2b_click_c2b_evaluate_score_seq', 'd_seq_c2b_click_transfer_num_seq', 'd_seq_c2b_click_c2b_ctob_model_price_seq', 'd_seq_c2b_click_c2b_ctob_diff_price_seq', 'd_seq_c2b_bid_c2b_seller_price_seq', 'd_seq_c2b_bid_road_haul_seq', 'd_seq_c2b_bid_c2b_evaluate_score_seq', 'd_seq_c2b_bid_transfer_num_seq', 'd_seq_c2b_bid_c2b_ctob_model_price_seq', 'd_seq_c2b_bid_c2b_ctob_diff_price_seq', 'd_seq_c2b_contract_c2b_seller_price_seq', 'd_seq_c2b_contract_road_haul_seq', 'd_seq_c2b_contract_c2b_evaluate_score_seq', 'd_seq_c2b_contract_transfer_num_seq', 'd_seq_c2b_contract_c2b_ctob_model_price_seq', 'd_seq_c2b_contract_c2b_ctob_diff_price_seq']

sparse_fea = sparse_user_feature + sparse_item_feature
dense_fea = dense_user_feature + dense_item_feature
seq_fea = sparse_sequence_features + dense_sequence_features
user_fea = sparse_user_feature + dense_user_feature + sparse_sequence_features + dense_sequence_features
item_fea = sparse_item_feature + dense_item_feature
total_fea = sparse_user_feature + dense_user_feature + sparse_sequence_features + dense_sequence_features + sparse_item_feature + dense_item_feature  # 注意按顺序


model_epoch = 'epoch2'
model_date = '2024-12-06'
parser = argparse.ArgumentParser()
parser.add_argument('-d', '--prod_date', default=(dt.datetime.today()).strftime('%Y-%m-%d'),
                    type=str, required=False, help="prod date")
arg = parser.parse_args()
prod_date = arg.prod_date

print("模型版本: {}_{}".format(model_date, model_epoch))  # 打印模型版本
print("生产日期: {}".format(prod_date))  # 打印生产日期
print("当前小时数: {}".format(dt.datetime.now().hour))

def spark_load_model(model_name):
    prepath = "hdfs:///user/daiyuxuan/dssm_search/models/"
    spark.sparkContext.addFile(prepath+model_name, True)
    model_file_local = SparkFiles.get(model_name)
    model = load_model(model_file_local)
    return model
# 加载模型
print("="*10, "模型加载中", "="*10)
item_layer_model = spark_load_model('item_layer_model_{model_epoch}_{model_date}'.format(model_epoch = model_epoch, model_date = model_date))
print("="*10, "模型加载完毕", "="*10)

# 预处理，加载保存的中位数和scaler
def load_scaler_and_medians(scaler_path, medians_path, dense_item_feature, dense_fea, model_date):
    spark.sparkContext.addFile(scaler_path)
    spark.sparkContext.addFile(medians_path)
    local_scaler_path = SparkFiles.get("dense_scaler_{model_date}.pkl".format(model_date = model_date))
    local_medians_path = SparkFiles.get("dense_medians_{model_date}.pkl".format(model_date = model_date))
    scaler = joblib.load(local_scaler_path)
    train_medians = joblib.load(local_medians_path)

    # 只取dense_fea那部分
    mean_ = [scaler.mean_[i] for i in range(len(scaler.mean_)) if dense_fea[i] in dense_item_feature]
    scale_ = [scaler.scale_[i] for i in range(len(scaler.scale_)) if dense_fea[i] in dense_item_feature]
    train_medians = {key: train_medians[key] for key in dense_item_feature if key in train_medians}
  
    # 新建一个StandardScaler类并赋值
    new_scaler = StandardScaler()
    new_scaler.mean_ = np.array(mean_)
    new_scaler.scale_ = np.array(scale_)
    
    return train_medians, new_scaler

medians_path = "hdfs:///user/daiyuxuan/dssm_search/models/dense_medians_{model_date}.pkl".format(model_date = model_date)
scaler_path = "hdfs:///user/daiyuxuan/dssm_search/models/dense_scaler_{model_date}.pkl".format(model_date = model_date)
print("="*10, "median、scaler加载中", "="*10)
train_medians, scaler = load_scaler_and_medians(scaler_path, medians_path, dense_item_feature, dense_fea, model_date)
print("="*10, "median、scaler加载完毕", "="*10)

print("="*10, "样本生成中", "="*10)
def gen_data(prod_date):
    yesterday = (dt.datetime.strptime(prod_date, '%Y-%m-%d') - dt.timedelta(days=1)).strftime('%Y-%m-%d')
    sample_sql = """
        with car_info_today as (
            select cast(auction_car_sale_info.clue_id as int) clue_id
                  ,fuel_type
                  ,car_year
                  ,gearbox
                  ,emission_standard
                  ,tag_id
                  ,city_id
                  ,car_color
                  ,guobie
                  ,minor_category_id
                  ,auto_type
                  ,seats
                  ,on_shelf_days
                  ,seller_price
                  ,road_haul
                  ,evaluate_score
                  ,transfer_num
                  ,license_year
                  ,air_displacement
                  ,evaluate_level
                  ,model_price
                  ,car_level
                  ,seller_price - model_price as diff_price
                  ,case when evaluate_score >= 0 and evaluate_score <= 55 then 35
                        when evaluate_score > 55 and evaluate_score <= 69 then 65
                        when evaluate_score > 69 and evaluate_score <= 89 then 80
                        when evaluate_score > 89 and evaluate_score <= 100 then 95
                        else -1 end as evaluate_score_segment
                  ,case when model_price > 0 and model_price <= 20000 then 2
                        when model_price > 20000 and model_price <= 40000 then 4
                        when model_price > 40000 and model_price <= 60000 then 6
                        when model_price > 60000 and model_price <= 90000 then 9
                        when model_price > 90000 and model_price <= 150000 then 15
                        when model_price > 150000 and model_price <= 10000000 then 1000
                        else -1 end as model_price_bin -- 包括0和null
            from (
                select clue_id
                    ,fuel_type
                    ,regexp_extract(title, '(\\d+)款', 1) as car_year
                    ,gearbox
                    ,emission_standard
                    ,tag_id
                    ,city_id
                    ,car_color
                    ,guobie
                    ,minor_category_id
                    ,auto_type
                    ,seats
                    ,cast(datediff('{prod_date}', created_at) as int) as on_shelf_days
                    ,road_haul
                    ,transfer_num
                    ,license_date as license_year
                    ,air_displacement
                    ,evaluate_score
                    ,evaluate_level
                from ods.ods_ctob_vehicle_auction_car_sale_info
                where sale_status = 0 
                and (auction_type = 2 or auction_type = 1) -- 新抢拍和今日秒杀
                and clue_id in ('142522400')
                -- limit 5
            ) auction_car_sale_info
            left join (
                select clue_id
                      ,model_price
                      ,seller_price
                      ,car_level
                from (
                    select clue_id
                          ,ts
                          ,cast(model_price as int) as model_price
                          ,cast(seller_price as int) as seller_price
                          ,cast(car_level as int) as car_level
                          ,row_number() OVER (PARTITION BY clue_id ORDER BY ts desc) as rn
                    from gzlc_real.fact_ctob_car_level
                    where dt >= date_add('{prod_date}', -1) and dt <= '{prod_date}'
                ) a
                where rn = 1
            ) car_level
            on auction_car_sale_info.clue_id = cast(car_level.clue_id as int)
        )

        ,car_rate_yesterday as (
            select cast(clue_id as int) as clue_id
                ,cast(click_bid_rate_0_3 as double) as click_bid_rate_0_3
                ,cast(click_bid_rate_0_30 as double) as click_bid_rate_0_30
                ,cast(beseen_click_rate_0_3 as double) as beseen_click_rate_0_3
                ,cast(beseen_click_rate_0_30 as double) as beseen_click_rate_0_30
            from g3_feature_dev.c2b_car_rate_v2
            where dt = '{yesterday}'
        )
        
        ,bid_percent_ratio_yesterday as (
            select cast(clue_id as int) as clue_id
                ,cast(c2b_offline_car_tag_id_bid_percent_ratio_0_7_d as double) as c2b_offline_car_tag_id_bid_percent_ratio_0_7_d
                ,cast(c2b_offline_car_tag_id_bid_percent_ratio_7_14_d as double) as c2b_offline_car_tag_id_bid_percent_ratio_7_14_d
                ,cast(c2b_offline_car_tag_id_bid_percent_ratio_14_30_d as double) as c2b_offline_car_tag_id_bid_percent_ratio_14_30_d
            from g3_feature_dev.c2b_offline_car_attr_bid_percent
            where dt = '{yesterday}'
        )

        ,attr_rate_yesterday as (
            select cast(clue_id as int) as clue_id
                ,cast(c2b_offline_car_tag_id_click_rate_0_7_d as double) as c2b_offline_car_tag_id_click_rate_0_7_d
                ,cast(c2b_offline_car_tag_id_click_rate_7_14_d as double) as c2b_offline_car_tag_id_click_rate_7_14_d
                ,cast(c2b_offline_car_tag_id_quick_collection_rate_0_7_d as double) as c2b_offline_car_tag_id_quick_collection_rate_0_7_d
                ,cast(c2b_offline_car_tag_id_quick_collection_rate_7_14_d as double) as c2b_offline_car_tag_id_quick_collection_rate_7_14_d
                ,cast(c2b_offline_car_tag_id_bid_rate_0_7_d as double) as c2b_offline_car_tag_id_bid_rate_0_7_d
                ,cast(c2b_offline_car_tag_id_bid_rate_7_14_d as double) as c2b_offline_car_tag_id_bid_rate_7_14_d
                ,cast(c2b_offline_car_minor_category_id_click_rate_0_7_d as double) as c2b_offline_car_minor_category_id_click_rate_0_7_d
                ,cast(c2b_offline_car_minor_category_id_quick_collection_rate_0_7_d as double) as c2b_offline_car_minor_category_id_quick_collection_rate_0_7_d
                ,cast(c2b_offline_car_minor_category_id_quick_collection_rate_7_14_d as double) as c2b_offline_car_minor_category_id_quick_collection_rate_7_14_d
            from g3_feature_dev.c2b_offline_car_attr_rate
            where dt = '{yesterday}'
        )

        ,lookup_table as (
            select *
            from g3_feature_dev.c2b_encoding_lookup_table
            where dt = '2024-11-01'
        )

        select A.clue_id as clue_id
            -- sparse特征
            ,if(L1.encode_id is null, 1, L1.encode_id) as s_i_c2b_evaluate_score_segment
            ,if(L2.encode_id is null, 1, L2.encode_id) as s_i_city_id
            ,if(L3.encode_id is null, 1, L3.encode_id) as s_i_emission_standard
            ,if(L4.encode_id is null, 1, L4.encode_id) as s_i_minor_category_id
            ,if(L5.encode_id is null, 1, L5.encode_id) as s_i_model_price_bin
            ,if(L6.encode_id is null, 1, L6.encode_id) as s_i_car_year
            ,if(L7.encode_id is null, 1, L7.encode_id) as s_i_gearbox
            ,if(L8.encode_id is null, 1, L8.encode_id) as s_i_air_displacement
            ,if(L9.encode_id is null, 1, L9.encode_id) as s_i_tag_id
            ,if(L10.encode_id is null, 1, L10.encode_id) as s_i_car_color
            ,if(L11.encode_id is null, 1, L11.encode_id) as s_i_guobie
            ,if(L12.encode_id is null, 1, L12.encode_id) as s_i_c2b_evaluate_level
            ,if(L13.encode_id is null, 1, L13.encode_id) as s_i_auto_type
            ,if(L14.encode_id is null, 1, L14.encode_id) as s_i_fuel_type
            ,if(L15.encode_id is null, 1, L15.encode_id) as s_i_seats
            ,if(L16.encode_id is null, 1, L16.encode_id) as s_i_c2b_ctob_car_level
            -- dense特征
            ,if(click_bid_rate_0_3 is null, 0, click_bid_rate_0_3) as d_i_c2b_offline_car_click_bid_rate_0_3_d
            ,if(click_bid_rate_0_30 is null, 0, click_bid_rate_0_30) as d_i_c2b_offline_car_click_bid_rate_0_30_d
            ,if(beseen_click_rate_0_3 is null, 0, beseen_click_rate_0_3) as d_i_c2b_offline_car_beseen_click_rate_0_3_d
            ,if(beseen_click_rate_0_30 is null, 0, beseen_click_rate_0_30) as d_i_c2b_offline_car_beseen_click_rate_0_30_d
            ,if(c2b_offline_car_tag_id_bid_percent_ratio_0_7_d is null, 0, c2b_offline_car_tag_id_bid_percent_ratio_0_7_d) as d_i_c2b_offline_car_tag_id_bid_percent_ratio_0_7_d
            ,if(c2b_offline_car_tag_id_bid_percent_ratio_7_14_d is null, 0, c2b_offline_car_tag_id_bid_percent_ratio_7_14_d)  as d_i_c2b_offline_car_tag_id_bid_percent_ratio_7_14_d
            ,if(c2b_offline_car_tag_id_bid_percent_ratio_14_30_d is null, 0, c2b_offline_car_tag_id_bid_percent_ratio_14_30_d) as d_i_c2b_offline_car_tag_id_bid_percent_ratio_14_30_d
            ,if(c2b_offline_car_tag_id_click_rate_0_7_d is null, 0, c2b_offline_car_tag_id_click_rate_0_7_d) as d_i_c2b_offline_car_tag_id_click_rate_0_7_d
            ,if(c2b_offline_car_tag_id_click_rate_7_14_d is null, 0, c2b_offline_car_tag_id_click_rate_7_14_d) as d_i_c2b_offline_car_tag_id_click_rate_7_14_d
            ,if(c2b_offline_car_tag_id_bid_rate_0_7_d is null, 0, c2b_offline_car_tag_id_bid_rate_0_7_d) as d_i_c2b_offline_car_tag_id_bid_rate_0_7_d
            ,if(c2b_offline_car_tag_id_bid_rate_7_14_d is null, 0, c2b_offline_car_tag_id_bid_rate_7_14_d) as d_i_c2b_offline_car_tag_id_bid_rate_7_14_d
            ,if(c2b_offline_car_minor_category_id_click_rate_0_7_d is null, 0, c2b_offline_car_minor_category_id_click_rate_0_7_d) as d_i_c2b_offline_car_minor_category_id_click_rate_0_7_d
            ,transfer_num as d_i_transfer_num
            ,evaluate_score as d_i_c2b_evaluate_score
            ,model_price as d_i_c2b_ctob_model_price
            ,seller_price as d_i_c2b_seller_price
            ,diff_price as d_i_c2b_ctob_diff_price
            ,road_haul as d_i_road_haul
            
        from car_info_today A
        left join car_rate_yesterday B on cast(A.clue_id as string) = cast(B.clue_id as string)
        left join bid_percent_ratio_yesterday C on cast(A.clue_id as string) = cast(C.clue_id as string)
        left join attr_rate_yesterday D on cast(A.clue_id as string) = cast(D.clue_id as string)
        left join (select fea_val, encode_id from lookup_table where fea_name = 'c2b_evaluate_score_segment') L1 on L1.fea_val = A.evaluate_score_segment
        left join (select fea_val, encode_id from lookup_table where fea_name = 'city_id') L2 on L2.fea_val = A.city_id
        left join (select fea_val, encode_id from lookup_table where fea_name = 'emission_standard') L3 on L3.fea_val = A.emission_standard
        left join (select fea_val, encode_id from lookup_table where fea_name = 'minor_category_id') L4 on L4.fea_val = A.minor_category_id
        left join (select fea_val, encode_id from lookup_table where fea_name = 'model_price_bin') L5 on L5.fea_val = A.model_price_bin
        left join (select fea_val, encode_id from lookup_table where fea_name = 'car_year') L6 on cast(L6.fea_val as string) = A.car_year
        left join (select fea_val, encode_id from lookup_table where fea_name = 'gearbox') L7 on L7.fea_val = A.gearbox
        left join (select fea_val, encode_id from lookup_table where fea_name = 'air_displacement') L8 on L8.fea_val = A.air_displacement
        left join (select fea_val, encode_id from lookup_table where fea_name = 'tag_id') L9 on L9.fea_val = A.tag_id
        left join (select fea_val, encode_id from lookup_table where fea_name = 'car_color') L10 on L10.fea_val = A.car_color
        left join (select fea_val, encode_id from lookup_table where fea_name = 'guobie') L11 on L11.fea_val = A.guobie
        left join (select fea_val, encode_id from lookup_table where fea_name = 'evaluate_level') L12 on L12.fea_val = A.evaluate_level
        left join (select fea_val, encode_id from lookup_table where fea_name = 'auto_type') L13 on L13.fea_val = A.auto_type
        left join (select fea_val, encode_id from lookup_table where fea_name = 'fuel_type') L14 on L14.fea_val = A.fuel_type
        left join (select fea_val, encode_id from lookup_table where fea_name = 'seats') L15 on L15.fea_val = A.seats
        left join (select fea_val, encode_id from lookup_table where fea_name = 'car_level') L16 on L16.fea_val = A.car_level
    """.format(prod_date = prod_date, yesterday = yesterday)
    
    sample_df = sqlContext.sql(sample_sql)
    return sample_df

sample_df = gen_data(prod_date)
print("="*10, "样本生成完毕", "="*10)

print("="*10, "样本预处理中", "="*10)
sample_df = sample_df.toPandas()
for col in dense_item_feature: # 填补dense特征
    sample_df[col].fillna(train_medians[col], inplace=True)
sample_df[dense_item_feature] = sample_df[dense_item_feature].astype('float32', copy=False)
sample_df[sparse_item_feature] = sample_df[sparse_item_feature].fillna(1) # 填补sparse特征
sample_df[sparse_item_feature] = sample_df[sparse_item_feature].astype('int32', copy=False)

# 归一化
dense_data = sample_df[dense_item_feature].values
dense_data_normalized = scaler.transform(dense_data)
sample_df[dense_item_feature] = dense_data_normalized
print("="*10, "样本预处理完毕", "="*10)

print("="*10, "输出embedding中", "="*10)
clue_id = sample_df["clue_id"]
item_input = []
# 打印每个特征的值
for fea in item_fea:
    feature_values = sample_df[fea].values.reshape(-1, 1).astype(np.float32)
    print(f"Feature: {fea}, Values: {feature_values[:5]}")  # 打印前5个值进行检查
    item_input.append(feature_values)
item_embedding = item_layer_model.predict(item_input)
item_embedding_list = [embedding.tolist() for embedding in item_embedding]
new_embedding_df = pd.DataFrame({"clue_id": clue_id.values, "item_embedding": item_embedding_list})
print(new_embedding_df.head(3))

def insert_db(dct):
    connection = pymysql.connect(
        host="g1-ai-ku-m.dns.guazi.com",
        port=3323,
        user="profile_w",
        password="rf064NAl%ffPQYkL7g",
        db="ai_profile",
        charset="utf8",
    )
    cursor = connection.cursor()
    sql = "INSERT ignore dssm_c2b_car_embedding_search (clue_id, embedding, model_version) VALUES(%s, %s, %s) ON DUPLICATE KEY UPDATE clue_id=%s,embedding=%s,model_version=%s"
    cnt = 0
    model_version = "c2b_dssm_item_model_20241206"
    for clue_id, embedding in dct.items():
        embedding_str = json.dumps(embedding)  # 将embedding从list转换为str
        insert_data = (clue_id, embedding_str, model_version, clue_id, embedding_str, model_version)
        cur_sql = sql % insert_data
        if cnt < 5:
            print(cur_sql)
        # 相比cursor.execute(cur_sql)，使用 cursor.execute(sql, insert_data) 更安全，因为它使用参数化查询，防止 SQL 注入。
        cursor.execute(sql, insert_data)
        connection.commit()
        if cnt % 100 == 0 and cnt != 0:
            time.sleep(1)
            print("sleep 1 s")
        cnt += 1

try:  # 读取已有的embedding
    path = "hdfs:///user/daiyuxuan/dssm_search/embedding"
    full_path = "{path}/item_{prod_date}/".format(path=path, prod_date=prod_date)
    # 读取CSV文件
    old_embedding_df = spark.read.csv(full_path, sep='\t', header=True) 
    # 将Spark DataFrame转换为Pandas DataFrame
    old_embedding_pd_df = old_embedding_df.toPandas()
    # 将DataFrame转换为字典
    old_embedding_dct = old_embedding_pd_df.set_index('clue_id')['item_embedding'].to_dict()
    # 将old_embedding_dct的item_embedding转换为list
    old_embedding_dct = {k: json.loads(v) if isinstance(v, str) else v for k, v in old_embedding_dct.items()}
    print("Old Embedding Dictionary Size:", len(old_embedding_dct))
except Exception as e:
    old_embedding_dct = {}
    print(f"Error reading old embeddings: {e}")
    pass

print("Old Embedding Dictionary Sample:")
for i, (key, value) in enumerate(old_embedding_dct.items()):
    if i < 5:
        print(f"clue_id: {key}, embedding: {value}")
    else:
        break

new_embedding_dct = new_embedding_df.set_index('clue_id')['item_embedding'].to_dict()
update_embedding_dct = {}
print_cnt, new_item_cnt, diff_item_cnt = 0, 0, 0
def cosine_similarity(a, b):
    return np.dot(a, b) / (norm(a) * norm(b))
for key, new_value in new_embedding_dct.items():
    new_value_list = new_value if isinstance(new_value, list) else new_value.tolist()
    old_value_list = old_embedding_dct.get(key, None)
    if old_value_list is not None and not isinstance(old_value_list, list):
        old_value_list = old_value_list.tolist()
    
    if key not in old_embedding_dct:
        update_embedding_dct[key] = new_value_list
        new_item_cnt += 1
        if print_cnt < 5:
            print("new_item")
            print("clue_id: {}".format(key))
            print("embedding: {}".format(new_value_list))
            print_cnt += 1
    # elif cosine_similarity(old_value_list, new_value_list) < 1 - 1e-6:
    else:
        update_embedding_dct[key] = new_value_list
        diff_item_cnt += 1
        if print_cnt < 5:
            print("diff_embbeding")
            print("clue_id: {}".format(key))
            print("old_embbeding: {}".format(old_value_list))
            print("new_embbeding: {}".format(new_value_list))
            print_cnt += 1

print("update_embedding length", len(update_embedding_dct.keys()))
print("new_item_cnt", new_item_cnt)
print("diff_item_cnt", diff_item_cnt)
insert_db(update_embedding_dct)

spark_embedding_df = spark.createDataFrame(new_embedding_df)

print("="*10, "DataFrame Schema", "="*10)
def array_to_string(my_list):
    return '[' + ','.join([str(elem) for elem in my_list]) + ']'
array_to_string_udf = udf(array_to_string, StringType())
spark_embedding_df = spark_embedding_df.withColumn("item_embedding_str", array_to_string_udf(spark_embedding_df["item_embedding"]))
spark_embedding_df.printSchema()

spark_embedding_df.drop("item_embedding").withColumnRenamed("item_embedding_str", "item_embedding").repartition(1).write.csv(
    path="{path}/item_{prod_date}/".format(path=path, prod_date=prod_date),
    header=True,
    sep="\t",
    mode="overwrite",
)

print("="*10, "输出embedding完毕", "="*10)
sc.stop()
