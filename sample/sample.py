import sys
from collections import defaultdict
import numpy as np
from pyspark.sql.types import *
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
import datetime as dt
from pyspark.sql import SparkSession
from pyspark import StorageLevel
import argparse
import random
from pyspark.sql.functions import lit, row_number, desc, translate, split, size, concat_ws
from pyspark.sql.window import Window

sc = SparkContext(appName="c2b_dssm_samples_search%s@daiyuxuan@20" % (dt.datetime.now().strftime('%Y%m%d')))
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


def gen_data(prod_date, scene):
    yesterday = (dt.datetime.strptime(prod_date, '%Y-%m-%d') - dt.timedelta(days=1)).strftime('%Y-%m-%d')
    sample_sql = """
        select user_id
            ,clue_id
            ,recommend_id
            ,label
            ,ts
        from g3_feature_dev.c2b_dssm_train_samples
        where dt = '{prod_date}'
        and scene = '{scene}'
        and action = 'bid'
        and clue_id is not null
    """.format(prod_date = prod_date, scene = scene)

    car_feat_sql = """
        with car_info_today as (
            select cast(car_source_ymd.clue_id as int) clue_id
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
                      ,cast(datediff('{prod_date}', create_time) as int) as on_shelf_days
                      ,road_haul
                      ,transfer_num
                      ,license_year
                      ,air_displacement
                      ,evaluate_score
                      ,evaluate_level
                from guazi_dw_dwd.dim_com_car_source_ymd
                where dt = '{prod_date}'
                and create_time > '2024'
            ) car_source_ymd
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
                    where dt >= date_add('{prod_date}', -90) and dt <= '{prod_date}'
                ) a
                where rn = 1
            ) car_level
            on car_source_ymd.clue_id = cast(car_level.clue_id as int)
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
            ,if(c2b_offline_car_tag_id_quick_collection_rate_0_7_d is null, 0, c2b_offline_car_tag_id_quick_collection_rate_0_7_d) as d_i_c2b_offline_car_tag_id_quick_collection_rate_0_7_d
            ,if(c2b_offline_car_tag_id_quick_collection_rate_7_14_d is null, 0, c2b_offline_car_tag_id_quick_collection_rate_7_14_d) as d_i_c2b_offline_car_tag_id_quick_collection_rate_7_14_d
            ,if(c2b_offline_car_tag_id_bid_rate_0_7_d is null, 0, c2b_offline_car_tag_id_bid_rate_0_7_d) as d_i_c2b_offline_car_tag_id_bid_rate_0_7_d
            ,if(c2b_offline_car_tag_id_bid_rate_7_14_d is null, 0, c2b_offline_car_tag_id_bid_rate_7_14_d) as d_i_c2b_offline_car_tag_id_bid_rate_7_14_d
            ,if(c2b_offline_car_minor_category_id_click_rate_0_7_d is null, 0, c2b_offline_car_minor_category_id_click_rate_0_7_d) as d_i_c2b_offline_car_minor_category_id_click_rate_0_7_d
            ,if(c2b_offline_car_minor_category_id_quick_collection_rate_0_7_d is null, 0, c2b_offline_car_minor_category_id_quick_collection_rate_0_7_d) as d_i_c2b_offline_car_minor_category_id_quick_collection_rate_0_7_d
            ,if(c2b_offline_car_minor_category_id_quick_collection_rate_7_14_d is null, 0, c2b_offline_car_minor_category_id_quick_collection_rate_7_14_d) as d_i_c2b_offline_car_minor_category_id_quick_collection_rate_7_14_d
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
        left join (select fea_val, encode_id from lookup_table where fea_name = 'car_year') L6 on L6.fea_val = A.car_year
        left join (select fea_val, encode_id from lookup_table where fea_name = 'gearbox') L7 on L7.fea_val = A.gearbox
        left join (select fea_val, encode_id from lookup_table where fea_name = 'air_displacement') L8 on L8.fea_val = A.air_displacement
        left join (select fea_val, encode_id from lookup_table where fea_name = 'tag_id') L9 on L9.fea_val = A.tag_id
        left join (select fea_val, encode_id from lookup_table where fea_name = 'car_color') L10 on L10.fea_val = A.car_color
        left join (select fea_val, encode_id from lookup_table where fea_name = 'guobie') L11 on L11.fea_val = A.guobie
        left join (select fea_val, encode_id from lookup_table where fea_name = 'c2b_evaluate_level') L12 on L12.fea_val = A.evaluate_level
        left join (select fea_val, encode_id from lookup_table where fea_name = 'auto_type') L13 on L13.fea_val = A.auto_type
        left join (select fea_val, encode_id from lookup_table where fea_name = 'fuel_type') L14 on L14.fea_val = A.fuel_type
        left join (select fea_val, encode_id from lookup_table where fea_name = 'seats') L15 on L15.fea_val = A.seats
        left join (select fea_val, encode_id from lookup_table where fea_name = 'car_level') L16 on L16.fea_val = A.car_level
    """.format(prod_date = prod_date, yesterday = yesterday)

    user_offline_feat_sql = """
        with user_activity_dt as (
            select cast(user_id as string) as user_id,
                max(case when action = 'beseen' then cnt_0_7 else 0 end) beseen_cnt_0_7,
                max(case when action = 'beseen' then cnt_7_14 else 0 end) beseen_cnt_7_14,
                max(case when action = 'beseen' then cnt_14_30 else 0 end) beseen_cnt_14_30,

                max(case when action = 'click' then cnt_0_7 else 0 end) click_cnt_0_7,
                max(case when action = 'click' then cnt_7_14 else 0 end) click_cnt_7_14,
                max(case when action = 'click' then cnt_14_30 else 0 end) click_cnt_14_30,

                max(case when action = 'bid' then cnt_0_7 else 0 end) bid_cnt_0_7,
                max(case when action = 'bid' then cnt_7_14 else 0 end) bid_cnt_7_14,
                max(case when action = 'bid' then cnt_14_30 else 0 end) bid_cnt_14_30

            from
                g3_feature_dev.c2b_user_action_cnt_v2
            where dt = '{yesterday}'
            group by user_id
        ),

        user_convert_cnt_dt as (
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
                max(case when action = 'click' then road_haul_max else 0 end) as click_road_haul_max_0_30,
                max(case when action = 'click' then road_haul_min else 0 end) as click_road_haul_min_0_30,
                max(case when action = 'click' then road_haul_p50 else 0 end) as click_road_haul_median_0_30,
                max(case when action = 'click' then evaluate_score_max else 0 end) as click_evaluate_score_max_0_30,
                max(case when action = 'click' then evaluate_score_min else 0 end) as click_evaluate_score_min_0_30,
                max(case when action = 'click' then evaluate_score_p50 else 0 end) as click_evaluate_score_median_0_30,

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
            where dt = '2024-11-01'
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
            ,click_road_haul_max_0_30 as d_u_click_road_haul_max_0_30
            ,click_road_haul_min_0_30 as d_u_click_road_haul_min_0_30
            ,click_road_haul_median_0_30 as d_u_click_road_haul_median_0_30
            ,click_evaluate_score_max_0_30 as d_u_click_evaluate_score_max_0_30
            ,click_evaluate_score_min_0_30 as d_u_click_evaluate_score_min_0_30
            ,click_evaluate_score_median_0_30 as d_u_click_evaluate_score_median_0_30
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
            ,if(add1.encode_id is null, 1, add1.encode_id) as s_u_click_c2b_evaluate_level_segment_0_7
            ,if(add2.encode_id is null, 1, add2.encode_id) as s_u_click_c2b_evaluate_score_segment_0_7
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
            ,if(add4.encode_id is null, 1, add4.encode_id) as s_u_bid_c2b_evaluate_level_segment_0_7
            ,if(add5.encode_id is null, 1, add5.encode_id) as s_u_bid_c2b_evaluate_score_segment_0_7
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
        left join user_convert_cnt_dt B on A.user_id = B.user_id
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
        left join (select fea_val, encode_id from lookup_table where fea_name = 'c2b_evaluate_level_segment' ) add1 on add1.fea_val = cast(D.click_evaluate_level_segment_0_7 as string)
        left join (select fea_val, encode_id from lookup_table where fea_name = 'c2b_evaluate_score_segment' ) add2 on add2.fea_val = cast(D.click_evaluate_score_segment_0_7 as string)
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
        left join (select fea_val, encode_id from lookup_table where fea_name = 'c2b_evaluate_level_segment' ) add4 on add4.fea_val = cast(D.bid_evaluate_level_segment_0_7 as string)
        left join (select fea_val, encode_id from lookup_table where fea_name = 'c2b_evaluate_score_segment' ) add5 on add5.fea_val = cast(D.bid_evaluate_score_segment_0_7 as string)
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

    user_realtime_feat_sql = """
        with last_click_cars_history as ( -- 过去29天历史点击
            select user_id
                ,clue_id
                ,cast(ts as int) as ts
                ,row_number() over (partition by user_id order by cast(ts as int) desc) as rn
            from gzlc_real.fact_ctob_user_behavior
            where dt between date_add('{prod_date}', -29)
            and date_add('{prod_date}', -1)
            and action = 'click'
            and user_id != ''
            and clue_id != ''
        )
        ,last_click_cars_30d as ( -- 过去29天最近的10次点击+今天点击
            select user_id
                ,clue_id
                ,ts
            from last_click_cars_history
            where rn <= 10
            union all
            select user_id
                ,clue_id
                ,cast(ts as int) as ts
            from gzlc_real.fact_ctob_user_behavior
            where dt = '{prod_date}'
            and action = 'click'
            and user_id != ''
            and clue_id != ''
        )
        ,last_bid_cars_history as ( -- 过去29天历史点击
            select user_id
                ,clue_id
                ,cast(ts as int) as ts
                ,row_number() over (partition by user_id order by cast(ts as int) desc) as rn
            from gzlc_real.fact_ctob_user_behavior
            where dt between date_add('{prod_date}', -29)
            and date_add('{prod_date}', -1)
            and action = 'bid'
            and user_id != ''
            and clue_id != ''
        )
        ,last_bid_cars_30d as ( -- 过去29天最近的10次出价+今天出价
            select user_id
                ,clue_id
                ,ts
            from last_bid_cars_history
            where rn <= 10
            union all
            select user_id
                ,clue_id
                ,cast(ts as int) as ts
            from gzlc_real.fact_ctob_user_behavior
            where dt = '{prod_date}'
            and action = 'bid'
            and user_id != ''
            and clue_id != ''
        )
        ,last_contract_cars_90d as ( -- 过去90天最近的10次下单
            select user_id
                ,clue_id
                ,create_time
            from guazi_dw_dw.dw_ctob_appoint_prepay_detail_ymd
            where dt = date_add('{prod_date}', -1)
            and substr(create_time, 1, 10) between date_add('{prod_date}', -90) and date_add('{prod_date}', -1)
            and user_id is not null
            and clue_id is not null
        )
        ,samples as (
            select user_id
                ,clue_id
                ,recommend_id
                ,label
                ,ts
            from g3_feature_dev.c2b_dssm_train_samples
            where dt = '{prod_date}'
            and scene = '{scene}'
            and action = 'bid'
            and clue_id is not null
        )
        ,cur_time_user as (
            select user_id
                ,ts
            from samples
            group by user_id, ts
        )
        ,last_click_cars_real as (
            select a.user_id
                ,a.ts
                ,clue_id
                ,row_number() over (partition by a.user_id, a.ts order by b.ts desc) as rn
            from cur_time_user a
            join last_click_cars_30d b
            on a.user_id = b.user_id
            where a.ts > b.ts
        )
        ,last_click_top10 as (
            select user_id
                ,clue_id
                ,ts
                ,rn
            from last_click_cars_real
            where rn <= 10
        )
        ,last_bid_cars_real as (
            select a.user_id
                ,a.ts
                ,clue_id
                ,row_number() over (partition by a.user_id, a.ts order by b.ts desc) as rn
            from cur_time_user a
            join last_bid_cars_30d b
            on a.user_id = b.user_id
            where a.ts > b.ts
        )
        ,last_bid_top10 as (
            select user_id
                ,clue_id
                ,ts
                ,rn
            from last_bid_cars_real
            where rn <= 10
        )
        ,last_contract_cars_real as (
            select a.user_id
                ,a.ts
                ,clue_id
                ,row_number() over (partition by a.user_id, a.ts order by b.create_time desc) as rn
            from cur_time_user a
            join last_contract_cars_90d b
            on a.user_id = b.user_id
        )
        ,last_contract_top10 as ( -- 过去89天最近的10次下单
            select user_id
                ,clue_id
                ,ts
                ,rn
            from last_contract_cars_real
            where rn <= 10
        )

        ,cars as (
            select cast(car_source_ymd.clue_id as int) clue_id
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
                ,seller_price
                ,road_haul
                ,evaluate_score
                ,transfer_num
                ,license_year
                ,license_month
                ,evaluate_level
                ,model_price
                ,car_level
                ,seller_price - model_price as diff_price
                ,case when evaluate_level = 'A' then 1
                        when evaluate_level = 'B' then 2
                        when evaluate_level = 'C' then 3
                        when evaluate_level = 'D' then 4
                        else 0 end as evaluate_level_segment
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
                    ,road_haul
                    ,transfer_num
                    ,license_year
                    ,license_month
                    ,evaluate_score
                    ,evaluate_level
                from guazi_dw_dwd.dim_com_car_source_ymd
                where dt = '{prod_date}'
            ) car_source_ymd
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
                    where dt >= date_add('{prod_date}', -90) and dt <= '{prod_date}'
                ) a
                where rn = 1
            ) car_level
            on car_source_ymd.clue_id = cast(car_level.clue_id as int)
        )
        ,user_click_feature as (
            select cast(user_id as int) user_id
                ,ts
                -- sparse序列类
                ,array_sort(collect_list(struct(a.rn, a.clue_id))).clue_id as s_seq_c2b_click_clue_id_seq
                ,array_sort(collect_list(struct(a.rn, b.fuel_type))).fuel_type as s_seq_c2b_click_fuel_type_seq
                ,array_sort(collect_list(struct(a.rn, b.car_year))).car_year as s_seq_c2b_click_car_year_seq
                ,array_sort(collect_list(struct(a.rn, b.gearbox))).gearbox as s_seq_c2b_click_gearbox_seq
                ,array_sort(collect_list(struct(a.rn, b.emission_standard))).emission_standard as s_seq_c2b_click_emission_standard_seq
                ,array_sort(collect_list(struct(a.rn, b.tag_id))).tag_id as s_seq_c2b_click_tag_id_seq
                ,array_sort(collect_list(struct(a.rn, b.city_id))).city_id as s_seq_c2b_click_city_id_seq
                ,array_sort(collect_list(struct(a.rn, b.car_color))).car_color as s_seq_c2b_click_car_color_seq
                ,array_sort(collect_list(struct(a.rn, b.guobie))).guobie as s_seq_c2b_click_guobie_seq
                ,array_sort(collect_list(struct(a.rn, b.minor_category_id))).minor_category_id as s_seq_c2b_click_minor_category_id_seq
                ,array_sort(collect_list(struct(a.rn, b.auto_type))).auto_type as s_seq_c2b_click_auto_type_seq
                ,array_sort(collect_list(struct(a.rn, b.seats))).seats as s_seq_c2b_click_seats_seq
                ,array_sort(collect_list(struct(a.rn, b.evaluate_level))).evaluate_level as s_seq_c2b_click_evaluate_level_seq
                ,array_sort(collect_list(struct(a.rn, b.evaluate_score_segment))).evaluate_score_segment as s_seq_c2b_click_c2b_evaluate_score_segment_seq
                ,array_sort(collect_list(struct(a.rn, b.car_level))).car_level as s_seq_c2b_click_c2b_ctob_car_level_seq
                ,array_sort(collect_list(struct(a.rn, b.model_price_bin))).model_price_bin as s_seq_c2b_click_model_price_bin_seq
                -- dense序列类
                ,array_sort(collect_list(struct(a.rn, b.seller_price))).seller_price as d_seq_c2b_click_c2b_seller_price_seq
                ,array_sort(collect_list(struct(a.rn, b.road_haul))).road_haul as d_seq_c2b_click_road_haul_seq
                ,array_sort(collect_list(struct(a.rn, b.evaluate_score))).evaluate_score as d_seq_c2b_click_c2b_evaluate_score_seq
                ,array_sort(collect_list(struct(a.rn, b.transfer_num))).transfer_num as d_seq_c2b_click_transfer_num_seq
                ,array_sort(collect_list(struct(a.rn, b.model_price))).model_price as d_seq_c2b_click_c2b_ctob_model_price_seq
                ,array_sort(collect_list(struct(a.rn, b.diff_price))).diff_price as d_seq_c2b_click_c2b_ctob_diff_price_seq

                -- 数值类
                ,avg(road_haul) as d_u_c2b_realtime_user_road_haul_avg_fix
                ,avg(evaluate_score) as d_u_c2b_realtime_user_evaluate_score_avg
                ,avg(seller_price) as d_u_c2b_realtime_user_seller_price_avg
                ,avg(model_price) as d_u_c2b_realtime_user_model_price_avg
                ,avg(diff_price) as d_u_c2b_realtime_user_diff_price_avg
                ,max(road_haul) as d_u_c2b_realtime_user_road_haul_max_fix
                ,max(evaluate_score) as d_u_c2b_realtime_user_evaluate_score_max
                ,max(seller_price) as d_u_c2b_realtime_user_seller_price_max
                ,max(model_price) as d_u_c2b_realtime_user_model_price_max
                ,max(diff_price) as d_u_c2b_realtime_user_diff_price_max
                ,min(road_haul) as d_u_c2b_realtime_user_road_haul_min_fix
                ,min(evaluate_score) as d_u_c2b_realtime_user_evaluate_score_min
                ,min(seller_price) as d_u_c2b_realtime_user_seller_price_min
                ,min(model_price) as d_u_c2b_realtime_user_model_price_min
                ,min(diff_price) as d_u_c2b_realtime_user_diff_price_min
                ,percentile_approx(road_haul, 0.5) as d_u_c2b_realtime_user_road_haul_p50_fix
                ,percentile_approx(evaluate_score, 0.5)  as d_u_c2b_realtime_user_evaluate_score_p50
                ,percentile_approx(seller_price, 0.5) as d_u_c2b_realtime_user_seller_price_p50
                ,percentile_approx(model_price, 0.5) as d_u_c2b_realtime_user_model_price_p50
                ,percentile_approx(diff_price, 0.5) as d_u_c2b_realtime_user_diff_price_p50
            from last_click_top10 a
            join cars b
            on cast(a.clue_id as int) = b.clue_id
            group by user_id, ts
        )
        ,user_bid_feature as (
            select cast(user_id as int) user_id
                ,ts
                -- sparse序列类
                ,array_sort(collect_list(struct(a.rn, a.clue_id))).clue_id as s_seq_c2b_bid_clue_id_seq
                ,array_sort(collect_list(struct(a.rn, b.fuel_type))).fuel_type as s_seq_c2b_bid_fuel_type_seq
                ,array_sort(collect_list(struct(a.rn, b.car_year))).car_year as s_seq_c2b_bid_car_year_seq
                ,array_sort(collect_list(struct(a.rn, b.gearbox))).gearbox as s_seq_c2b_bid_gearbox_seq
                ,array_sort(collect_list(struct(a.rn, b.emission_standard))).emission_standard as s_seq_c2b_bid_emission_standard_seq
                ,array_sort(collect_list(struct(a.rn, b.tag_id))).tag_id as s_seq_c2b_bid_tag_id_seq
                ,array_sort(collect_list(struct(a.rn, b.city_id))).city_id as s_seq_c2b_bid_city_id_seq
                ,array_sort(collect_list(struct(a.rn, b.car_color))).car_color as s_seq_c2b_bid_car_color_seq
                ,array_sort(collect_list(struct(a.rn, b.guobie))).guobie as s_seq_c2b_bid_guobie_seq
                ,array_sort(collect_list(struct(a.rn, b.minor_category_id))).minor_category_id as s_seq_c2b_bid_minor_category_id_seq
                ,array_sort(collect_list(struct(a.rn, b.auto_type))).auto_type as s_seq_c2b_bid_auto_type_seq
                ,array_sort(collect_list(struct(a.rn, b.seats))).seats as s_seq_c2b_bid_seats_seq
                ,array_sort(collect_list(struct(a.rn, b.evaluate_level))).evaluate_level as s_seq_c2b_bid_evaluate_level_seq
                ,array_sort(collect_list(struct(a.rn, b.evaluate_score_segment))).evaluate_score_segment as s_seq_c2b_bid_c2b_evaluate_score_segment_seq
                ,array_sort(collect_list(struct(a.rn, b.car_level))).car_level as s_seq_c2b_bid_c2b_ctob_car_level_seq
                ,array_sort(collect_list(struct(a.rn, b.model_price_bin))).model_price_bin as s_seq_c2b_bid_model_price_bin_seq
                -- dense序列类
                ,array_sort(collect_list(struct(a.rn, b.seller_price))).seller_price as d_seq_c2b_bid_c2b_seller_price_seq
                ,array_sort(collect_list(struct(a.rn, b.road_haul))).road_haul as d_seq_c2b_bid_road_haul_seq
                ,array_sort(collect_list(struct(a.rn, b.evaluate_score))).evaluate_score as d_seq_c2b_bid_c2b_evaluate_score_seq
                ,array_sort(collect_list(struct(a.rn, b.transfer_num))).transfer_num as d_seq_c2b_bid_transfer_num_seq
                ,array_sort(collect_list(struct(a.rn, b.model_price))).model_price as d_seq_c2b_bid_c2b_ctob_model_price_seq
                ,array_sort(collect_list(struct(a.rn, b.diff_price))).diff_price as d_seq_c2b_bid_c2b_ctob_diff_price_seq
            from last_bid_top10 a
            join cars b
            on cast(a.clue_id as int) = b.clue_id
            group by user_id, ts
        )
        ,user_contract_feature as (
            select cast(user_id as int) user_id
                ,ts
                -- sparse序列类
                ,array_sort(collect_list(struct(a.rn, a.clue_id))).clue_id as s_seq_c2b_contract_clue_id_seq
                ,array_sort(collect_list(struct(a.rn, b.fuel_type))).fuel_type as s_seq_c2b_contract_fuel_type_seq
                ,array_sort(collect_list(struct(a.rn, b.car_year))).car_year as s_seq_c2b_contract_car_year_seq
                ,array_sort(collect_list(struct(a.rn, b.gearbox))).gearbox as s_seq_c2b_contract_gearbox_seq
                ,array_sort(collect_list(struct(a.rn, b.emission_standard))).emission_standard as s_seq_c2b_contract_emission_standard_seq
                ,array_sort(collect_list(struct(a.rn, b.tag_id))).tag_id as s_seq_c2b_contract_tag_id_seq
                ,array_sort(collect_list(struct(a.rn, b.city_id))).city_id as s_seq_c2b_contract_city_id_seq
                ,array_sort(collect_list(struct(a.rn, b.car_color))).car_color as s_seq_c2b_contract_car_color_seq
                ,array_sort(collect_list(struct(a.rn, b.guobie))).guobie as s_seq_c2b_contract_guobie_seq
                ,array_sort(collect_list(struct(a.rn, b.minor_category_id))).minor_category_id as s_seq_c2b_contract_minor_category_id_seq
                ,array_sort(collect_list(struct(a.rn, b.auto_type))).auto_type as s_seq_c2b_contract_auto_type_seq
                ,array_sort(collect_list(struct(a.rn, b.seats))).seats as s_seq_c2b_contract_seats_seq
                ,array_sort(collect_list(struct(a.rn, b.evaluate_level))).evaluate_level as s_seq_c2b_contract_evaluate_level_seq
                ,array_sort(collect_list(struct(a.rn, b.evaluate_score_segment))).evaluate_score_segment as s_seq_c2b_contract_c2b_evaluate_score_segment_seq
                ,array_sort(collect_list(struct(a.rn, b.car_level))).car_level as s_seq_c2b_contract_c2b_ctob_car_level_seq
                ,array_sort(collect_list(struct(a.rn, b.model_price_bin))).model_price_bin as s_seq_c2b_contract_model_price_bin_seq
                -- dense序列类
                ,array_sort(collect_list(struct(a.rn, b.seller_price))).seller_price as d_seq_c2b_contract_c2b_seller_price_seq
                ,array_sort(collect_list(struct(a.rn, b.road_haul))).road_haul as d_seq_c2b_contract_road_haul_seq
                ,array_sort(collect_list(struct(a.rn, b.evaluate_score))).evaluate_score as d_seq_c2b_contract_c2b_evaluate_score_seq
                ,array_sort(collect_list(struct(a.rn, b.transfer_num))).transfer_num as d_seq_c2b_contract_transfer_num_seq
                ,array_sort(collect_list(struct(a.rn, b.model_price))).model_price as d_seq_c2b_contract_c2b_ctob_model_price_seq
                ,array_sort(collect_list(struct(a.rn, b.diff_price))).diff_price as d_seq_c2b_contract_c2b_ctob_diff_price_seq
            from last_contract_top10 a
            join cars b
            on cast(a.clue_id as int) = b.clue_id
            group by user_id, ts
        )
        select a.*
            ,b.s_seq_c2b_bid_clue_id_seq
            ,b.s_seq_c2b_bid_fuel_type_seq
            ,b.s_seq_c2b_bid_car_year_seq
            ,b.s_seq_c2b_bid_gearbox_seq
            ,b.s_seq_c2b_bid_emission_standard_seq
            ,b.s_seq_c2b_bid_tag_id_seq
            ,b.s_seq_c2b_bid_city_id_seq
            ,b.s_seq_c2b_bid_car_color_seq
            ,b.s_seq_c2b_bid_guobie_seq
            ,b.s_seq_c2b_bid_minor_category_id_seq
            ,b.s_seq_c2b_bid_auto_type_seq
            ,b.s_seq_c2b_bid_seats_seq
            ,b.s_seq_c2b_bid_evaluate_level_seq
            ,b.s_seq_c2b_bid_c2b_evaluate_score_segment_seq
            ,b.s_seq_c2b_bid_c2b_ctob_car_level_seq
            ,b.s_seq_c2b_bid_model_price_bin_seq
            ,b.d_seq_c2b_bid_c2b_seller_price_seq
            ,b.d_seq_c2b_bid_road_haul_seq
            ,b.d_seq_c2b_bid_c2b_evaluate_score_seq
            ,b.d_seq_c2b_bid_transfer_num_seq
            ,b.d_seq_c2b_bid_c2b_ctob_model_price_seq
            ,b.d_seq_c2b_bid_c2b_ctob_diff_price_seq
            ,c.s_seq_c2b_contract_clue_id_seq
            ,c.s_seq_c2b_contract_fuel_type_seq
            ,c.s_seq_c2b_contract_car_year_seq
            ,c.s_seq_c2b_contract_gearbox_seq
            ,c.s_seq_c2b_contract_emission_standard_seq
            ,c.s_seq_c2b_contract_tag_id_seq
            ,c.s_seq_c2b_contract_city_id_seq
            ,c.s_seq_c2b_contract_car_color_seq
            ,c.s_seq_c2b_contract_guobie_seq
            ,c.s_seq_c2b_contract_minor_category_id_seq
            ,c.s_seq_c2b_contract_auto_type_seq
            ,c.s_seq_c2b_contract_seats_seq
            ,c.s_seq_c2b_contract_evaluate_level_seq
            ,c.s_seq_c2b_contract_c2b_evaluate_score_segment_seq
            ,c.s_seq_c2b_contract_c2b_ctob_car_level_seq
            ,c.s_seq_c2b_contract_model_price_bin_seq
            ,c.d_seq_c2b_contract_c2b_seller_price_seq
            ,c.d_seq_c2b_contract_road_haul_seq
            ,c.d_seq_c2b_contract_c2b_evaluate_score_seq
            ,c.d_seq_c2b_contract_transfer_num_seq
            ,c.d_seq_c2b_contract_c2b_ctob_model_price_seq
            ,c.d_seq_c2b_contract_c2b_ctob_diff_price_seq
        from user_click_feature a
        left join user_bid_feature b
        on a.user_id = b.user_id
        and a.ts = b.ts
        left join user_contract_feature c
        on a.user_id = c.user_id
        and a.ts = c.ts
    """.format(prod_date = prod_date, scene = scene)    


    sample_df = sqlContext.sql(sample_sql)
    car_feat_df = sqlContext.sql(car_feat_sql)
    user_offline_feat_df = sqlContext.sql(user_offline_feat_sql)
    user_realtime_feat_df = sqlContext.sql(user_realtime_feat_sql)
    return sample_df, car_feat_df, user_offline_feat_df, user_realtime_feat_df

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--prod_date', default=(dt.datetime.today() - dt.timedelta(days=1)).strftime('%Y-%m-%d'),
                        type=str, required=False, help="prod date")
    parser.add_argument('-f', '--filename', default='', type=str, required=False, help="filename")
    parser.add_argument('-s', '--scene', default='SEARCH', type=str, required=False, help="scene")
    arg = parser.parse_args()
    prod_date = arg.prod_date
    scene = arg.scene
    filename = arg.filename
    dir_path = "/user/daiyuxuan/dssm_search/samples/"
    dir_path = dir_path + "{0}_{1}_{2}".format(scene, filename, prod_date.replace('-', ''))

    sample_df, car_feat_df, user_offline_feat_df, user_realtime_feat_df = gen_data(prod_date, scene)
    user_realtime_feat_df.show(3)
    # 合并样本和离线用户特征
    sample_df = sample_df.join(user_offline_feat_df, 'user_id', 'left')
    # 合并样本和实时用户特征
    sample_df = sample_df.join(user_realtime_feat_df, ['user_id', 'ts'], 'left')
    sample_df.show(3)
    # 处理车源特征
    sample_df = sample_df.join(car_feat_df, 'clue_id', 'left')
    # 在写入CSV之前，将array<string>列转换为字符串
    array_columns = [col_name for col_name, dtype in sample_df.dtypes if dtype.startswith('array')]
    for col_name in array_columns:
        sample_df = sample_df.withColumn(col_name, concat_ws(',', col_name))
    # shuffle
    sample_df = sample_df.sample(fraction=1.0)
    print(sample_df.head(3))
    sample_df.coalesce(1).write.format('csv').option('header', True).option("quote", " ").mode(
        'overwrite').option('sep', '\t').csv(dir_path)
    # sc.stop()