-- 双塔用户离线特征，包括dense和sparse，生成到一个新分组里
with user_action_cnt as ( -- 1.车商曝光、点击、出价cnt
    select cast(user_id as int) as user_id
        ,max(case when action = 'beseen' then cnt_0_7 else 0 end) d_u_beseen_cnt_0_7
        ,max(case when action = 'beseen' then cnt_7_14 else 0 end) d_u_beseen_cnt_7_14
        ,max(case when action = 'beseen' then cnt_14_30 else 0 end) d_u_beseen_cnt_14_30
        ,max(case when action = 'click' then cnt_0_7 else 0 end) d_u_click_cnt_0_7
        ,max(case when action = 'click' then cnt_7_14 else 0 end) d_u_click_cnt_7_14
        ,max(case when action = 'click' then cnt_14_30 else 0 end) d_u_click_cnt_14_30
        ,max(case when action = 'bid' then cnt_0_7 else 0 end) d_u_bid_cnt_0_7
        ,max(case when action = 'bid' then cnt_7_14 else 0 end) d_u_bid_cnt_7_14
        ,max(case when action = 'bid' then cnt_14_30 else 0 end) d_u_bid_cnt_14_30
    from g3_feature_dev.c2b_user_action_cnt_v2
    where dt = '${date_y_m_d}'
    and user_id is not null
    group by user_id
)
,user_convert_cnt as ( -- 2.车商下单cnt
    select cast(user_id as int) as user_id
        ,cast(if(cnt_0_30 is null, 0, cnt_0_30) as int) as d_u_contract_cnt_30
        ,cast(if(cnt_0_365 is null, 0, cnt_0_365) as int) as d_u_contract_cnt_365
    from g3_feature_dev.c2b_user_convert_cnt_v2
    where dt = '${date_y_m_d}'
    and user_id is not null
)
,user_click_statis_7d as ( -- 3.车商7天点击dense特征
    select cast(user_id as int) as user_id
        ,road_haul_max as d_u_click_road_haul_max_0_7
        ,road_haul_min as d_u_click_road_haul_min_0_7
        ,road_haul_p50 as d_u_click_road_haul_median_0_7
        ,evaluate_score_max as d_u_click_evaluate_score_max_0_7
        ,evaluate_score_min as d_u_click_evaluate_score_min_0_7
        ,evaluate_score_p50 as d_u_click_evaluate_score_median_0_7
    from g3_feature_dev.c2b_user_action_statis_v2
    where dt = '${date_y_m_d}'
    and user_id is not null
    and action = 'click'
    and cnt_days = 7 
)
,user_bid_statis_7d as ( -- 4.车商7天出价dense特征
    select cast(user_id as int) as user_id
        ,road_haul_max as d_u_bid_road_haul_max_0_7
        ,road_haul_min as d_u_bid_road_haul_min_0_7
        ,road_haul_p50 as d_u_bid_road_haul_median_0_7
        ,evaluate_score_max as d_u_bid_evaluate_score_max_0_7
        ,evaluate_score_min as d_u_bid_evaluate_score_min_0_7
        ,evaluate_score_p50 as d_u_bid_evaluate_score_median_0_7
    from g3_feature_dev.c2b_user_action_statis_v2
    where dt = '${date_y_m_d}'
    and user_id is not null
    and action = 'bid'
    and cnt_days = 7 
)
,user_click_statis_30d as ( -- 5.车商30天点击dense特征
    select cast(user_id as int) as user_id
        ,road_haul_max as d_u_click_road_haul_max_0_30
        ,road_haul_min as d_u_click_road_haul_min_0_30
        ,road_haul_p50 as d_u_click_road_haul_median_0_30
        ,evaluate_score_max as d_u_click_evaluate_score_max_0_30
        ,evaluate_score_min as d_u_click_evaluate_score_min_0_30
        ,evaluate_score_p50 as d_u_click_evaluate_score_median_0_30
    from g3_feature_dev.c2b_user_action_statis_v2
    where dt = '${date_y_m_d}'
    and user_id is not null
    and action = 'click'
    and cnt_days = 30
)
,user_bid_statis_30d as ( -- 6.车商30天出价dense特征
    select cast(user_id as int) as user_id
        ,road_haul_max as d_u_bid_road_haul_max_0_30
        ,road_haul_min as d_u_bid_road_haul_min_0_30
        ,road_haul_p50 as d_u_bid_road_haul_median_0_30
        ,evaluate_score_max as d_u_bid_evaluate_score_max_0_30
        ,evaluate_score_min as d_u_bid_evaluate_score_min_0_30
        ,evaluate_score_p50 as d_u_bid_evaluate_score_median_0_30
    from g3_feature_dev.c2b_user_action_statis_v2
    where dt = '${date_y_m_d}'
    and user_id is not null
    and action = 'bid'
    and cnt_days = 30
)

,user_deal_statis_30d as ( -- 7.车商30天下单dense特征
    select cast(user_id as int) as user_id
        ,road_haul_max as d_u_deal_road_haul_max_0_30
        ,road_haul_min as d_u_deal_road_haul_min_0_30
        ,road_haul_p50 as d_u_deal_road_haul_median_0_30
        ,evaluate_score_max as d_u_deal_evaluate_score_max_0_30
        ,evaluate_score_min as d_u_deal_evaluate_score_min_0_30
        ,evaluate_score_p50 as d_u_deal_evaluate_score_median_0_30
    from g3_feature_dev.c2b_user_convert_statis_v2_fix
    where dt = '${date_y_m_d}'
    and user_id is not null
    and convert = 'beseen'
    and cnt_days = 30
)
,user_deal_statis_365d as ( -- 8.车商365天下单dense特征
    select cast(user_id as int) as user_id
        ,road_haul_max as d_u_deal_road_haul_max_0_365
        ,road_haul_min as d_u_deal_road_haul_min_0_365
        ,road_haul_p50 as d_u_deal_road_haul_median_0_365
        ,evaluate_score_max as d_u_deal_evaluate_score_max_0_365
        ,evaluate_score_min as d_u_deal_evaluate_score_min_0_365
        ,evaluate_score_p50 as d_u_deal_evaluate_score_median_0_365
    from g3_feature_dev.c2b_user_convert_statis_v2_fix
    where dt = '${date_y_m_d}'
    and user_id is not null
    and convert = 'beseen'
    and cnt_days = 365
)
,user_action_by_id_ratio as (
    select *
    from g3_feature_dev.c2b_user_action_by_id_ratio_v2
    where dt = '${date_y_m_d}'
)
,user_action_max_ratio as (
    select cast(user_id as int) as user_id
        ,cast(cluster as string) as cluster
        ,cast(action as string) as action
        ,max(ratio_0_7) as max_ratio_0_7
    from g3_feature_dev.c2b_user_action_by_id_ratio_v2
    where dt = '${date_y_m_d}'
    and user_id is not null
    group by user_id, cluster, action
)
,lookup_table as (
    select *
    from g3_feature_dev.c2b_encoding_lookup_table
    where dt = '2024-11-01'
)
,user_action_by_id_ratio_0_7 as ( -- 9.车商7天点击、出价sparse特征
    select A.user_id
        ,max(case when (concat(A.action, '_', A.cluster) = 'click_tag_id' and max_ratio_0_7 > 0) then B.key else -1 end) as s_u_click_tag_id_0_7
        ,max(case when (concat(A.action, '_', A.cluster) = 'click_seats' and max_ratio_0_7 > 0) then B.key else -1 end) as s_u_click_seats_0_7
        ,max(case when (concat(A.action, '_', A.cluster) = 'click_minor_category_id' and max_ratio_0_7 > 0) then B.key else -1 end) as s_u_click_minor_category_id_0_7
        ,max(case when (concat(A.action, '_', A.cluster) = 'click_guobie' and max_ratio_0_7 > 0) then B.key else -1 end) as s_u_click_guobie_0_7
        ,max(case when (concat(A.action, '_', A.cluster) = 'click_gearbox' and max_ratio_0_7 > 0) then B.key else -1 end) as s_u_click_gearbox_0_7
        ,max(case when (concat(A.action, '_', A.cluster) = 'click_fuel_type' and max_ratio_0_7 > 0) then B.key else -1 end) as s_u_click_fuel_type_0_7
        ,max(case when (concat(A.action, '_', A.cluster) = 'click_emission_standard' and max_ratio_0_7 > 0) then B.key else -1 end) as s_u_click_emission_standard_0_7
        ,max(case when (concat(A.action, '_', A.cluster) = 'click_city_id' and max_ratio_0_7 > 0) then B.key else -1 end) as s_u_click_city_id_0_7
        ,max(case when (concat(A.action, '_', A.cluster) = 'click_car_year' and max_ratio_0_7 > 0) then B.key else -1 end) as s_u_click_car_year_0_7
        ,max(case when (concat(A.action, '_', A.cluster) = 'click_auto_type' and max_ratio_0_7 > 0) then B.key else -1 end) as s_u_click_auto_type_0_7
        ,max(case when (concat(A.action, '_', A.cluster) = 'click_car_color' and max_ratio_0_7 > 0) then B.key else -1 end) as s_u_click_car_color_0_7
        ,max(case when (concat(A.action, '_', A.cluster) = 'click_evaluate_level_segment' and max_ratio_0_7 > 0) then B.key else -1 end) as s_u_click_c2b_evaluate_level_segment_0_7
        ,max(case when (concat(A.action, '_', A.cluster) = 'click_evaluate_score_segment' and max_ratio_0_7 > 0) then B.key else -1 end) as s_u_click_c2b_evaluate_score_segment_0_7
        ,max(case when (concat(A.action, '_', A.cluster) = 'click_model_price_bin' and max_ratio_0_7 > 0) then B.key else -1 end) as s_u_click_model_price_bin_0_7
        ,max(case when (concat(A.action, '_', A.cluster) = 'bid_tag_id' and max_ratio_0_7 > 0) then B.key else -1 end) as s_u_bid_tag_id_0_7
        ,max(case when (concat(A.action, '_', A.cluster) = 'bid_seats' and max_ratio_0_7 > 0) then B.key else -1 end) as s_u_bid_seats_0_7
        ,max(case when (concat(A.action, '_', A.cluster) = 'bid_minor_category_id' and max_ratio_0_7 > 0) then B.key else -1 end) as s_u_bid_minor_category_id_0_7
        ,max(case when (concat(A.action, '_', A.cluster) = 'bid_guobie' and max_ratio_0_7 > 0) then B.key else -1 end) as s_u_bid_guobie_0_7
        ,max(case when (concat(A.action, '_', A.cluster) = 'bid_gearbox' and max_ratio_0_7 > 0) then B.key else -1 end) as s_u_bid_gearbox_0_7
        ,max(case when (concat(A.action, '_', A.cluster) = 'bid_fuel_type' and max_ratio_0_7 > 0) then B.key else -1 end) as s_u_bid_fuel_type_0_7
        ,max(case when (concat(A.action, '_', A.cluster) = 'bid_emission_standard' and max_ratio_0_7 > 0) then B.key else -1 end) as s_u_bid_emission_standard_0_7
        ,max(case when (concat(A.action, '_', A.cluster) = 'bid_city_id' and max_ratio_0_7 > 0) then B.key else -1 end) as s_u_bid_city_id_0_7
        ,max(case when (concat(A.action, '_', A.cluster) = 'bid_car_year' and max_ratio_0_7 > 0) then B.key else -1 end) as s_u_bid_car_year_0_7
        ,max(case when (concat(A.action, '_', A.cluster) = 'bid_auto_type' and max_ratio_0_7 > 0) then B.key else -1 end) as s_u_bid_auto_type_0_7
        ,max(case when (concat(A.action, '_', A.cluster) = 'bid_car_color' and max_ratio_0_7 > 0) then B.key else -1 end) as s_u_bid_car_color_0_7
        ,max(case when (concat(A.action, '_', A.cluster) = 'bid_evaluate_level_segment' and max_ratio_0_7 > 0) then B.key else -1 end) as s_u_bid_c2b_evaluate_level_segment_0_7
        ,max(case when (concat(A.action, '_', A.cluster) = 'bid_evaluate_score_segment' and max_ratio_0_7 > 0) then B.key else -1 end) as s_u_bid_c2b_evaluate_score_segment_0_7
        ,max(case when (concat(A.action, '_', A.cluster) = 'bid_model_price_bin' and max_ratio_0_7 > 0) then B.key else -1 end) as s_u_bid_model_price_bin_0_7
    from user_action_max_ratio A 
    left join user_action_by_id_ratio B 
    on A.user_id = B.user_id 
    and A.cluster = B.cluster 
    and A.action = B.action 
    and A.max_ratio_0_7 = B.ratio_0_7
    group by A.user_id
)
,user_convert_by_id_ratio as (
    select *
    from g3_feature_dev.c2b_user_convert_by_id_ratio_v2
    where dt = '${date_y_m_d}'
)
,user_convert_max_ratio as (
    select cast(user_id as string) as user_id
        ,cast(cluster as string) as cluster
        ,max(ratio_0_30) as max_ratio_0_30
        ,max(ratio_0_365) as max_ratio_0_365
    from g3_feature_dev.c2b_user_convert_by_id_ratio_v2
    where dt = '${date_y_m_d}'
    and user_id is not null
    group by user_id, cluster
)
,user_convert_by_id_ratio_0_30 as ( -- 10.车商30天下单sparse特征
    select A.user_id
        ,max(case when A.cluster = 'tag_id' and max_ratio_0_30 > 0 then B.key else -1 end) as s_u_deal_tag_id_0_30
        ,max(case when A.cluster = 'seats' and max_ratio_0_30 > 0 then B.key else -1 end) as s_u_deal_seats_0_30
        ,max(case when A.cluster = 'minor_category_id' and max_ratio_0_30 > 0 then B.key else -1 end) as s_u_deal_minor_category_id_0_30
        ,max(case when A.cluster = 'guobie' and max_ratio_0_30 > 0 then B.key else -1 end) as s_u_deal_guobie_0_30
        ,max(case when A.cluster = 'gearbox' and max_ratio_0_30 > 0 then B.key else -1 end) as s_u_deal_gearbox_0_30
        ,max(case when A.cluster = 'fuel_type' and max_ratio_0_30 > 0 then B.key else -1 end) as s_u_deal_fuel_type_0_30
        ,max(case when A.cluster = 'emission_standard' and max_ratio_0_30 > 0 then B.key else -1 end) as s_u_deal_emission_standard_0_30
        ,max(case when A.cluster = 'city_id' and max_ratio_0_30 > 0 then B.key else -1 end) as s_u_deal_city_id_0_30
        ,max(case when A.cluster = 'car_year' and max_ratio_0_30 > 0 then B.key else -1 end) as s_u_deal_car_year_0_30
        ,max(case when A.cluster = 'auto_type' and max_ratio_0_30 > 0 then B.key else -1 end) as s_u_deal_auto_type_0_30
        ,max(case when A.cluster = 'car_color' and max_ratio_0_30 > 0 then B.key else -1 end) as s_u_deal_car_color_0_30
    from user_convert_max_ratio A
    left join user_convert_by_id_ratio B on A.user_id = B.user_id and A.cluster = B.cluster and A.max_ratio_0_30 = B.ratio_0_30
    group by A.user_id
),

user_convert_by_id_ratio_0_365 as ( -- 11.车商365天下单sparse特征
    select A.user_id
        ,max(case when A.cluster = 'tag_id' and max_ratio_0_365 > 0 then B.key else -1 end) as s_u_deal_tag_id_0_365
        ,max(case when A.cluster = 'seats' and max_ratio_0_365 > 0 then B.key else -1 end) as s_u_deal_seats_0_365
        ,max(case when A.cluster = 'minor_category_id' and max_ratio_0_365 > 0 then B.key else -1 end) as s_u_deal_minor_category_id_0_365
        ,max(case when A.cluster = 'guobie' and max_ratio_0_365 > 0 then B.key else -1 end) as s_u_deal_guobie_0_365
        ,max(case when A.cluster = 'gearbox' and max_ratio_0_365 > 0 then B.key else -1 end) as s_u_deal_gearbox_0_365
        ,max(case when A.cluster = 'fuel_type' and max_ratio_0_365 > 0 then B.key else -1 end) as s_u_deal_fuel_type_0_365
        ,max(case when A.cluster = 'emission_standard' and max_ratio_0_365 > 0 then B.key else -1 end) as s_u_deal_emission_standard_0_365
        ,max(case when A.cluster = 'city_id' and max_ratio_0_365 > 0 then B.key else -1 end) as s_u_deal_city_id_0_365
        ,max(case when A.cluster = 'car_year' and max_ratio_0_365 > 0 then B.key else -1 end) as s_u_deal_car_year_0_365
        ,max(case when A.cluster = 'auto_type' and max_ratio_0_365 > 0 then B.key else -1 end) as s_u_deal_auto_type_0_365
        ,max(case when A.cluster = 'car_color' and max_ratio_0_365 > 0 then B.key else -1 end) as s_u_deal_car_color_0_365
    from user_convert_max_ratio A
    left join user_convert_by_id_ratio B on A.user_id = B.user_id and A.cluster = B.cluster and A.max_ratio_0_365 = B.ratio_0_365
    group by A.user_id
)
insert overwrite table g3_feature_dev.c2b_dssm_user_offline_features partition(dt='${date_y_m_d}')
select T1.user_id
    ,T1.d_u_beseen_cnt_0_7
    ,T1.d_u_beseen_cnt_7_14
    ,T1.d_u_beseen_cnt_14_30
    ,COALESCE(T1.d_u_click_cnt_0_7, 0.0) AS d_u_click_cnt_0_7
    ,COALESCE(T1.d_u_click_cnt_7_14, 0.0) AS d_u_click_cnt_7_14
    ,COALESCE(T1.d_u_click_cnt_14_30, 0.0) AS d_u_click_cnt_14_30
    ,COALESCE(T1.d_u_bid_cnt_0_7, 0.0) AS d_u_bid_cnt_0_7
    ,COALESCE(T1.d_u_bid_cnt_7_14, 0.0) AS d_u_bid_cnt_7_14
    ,COALESCE(T1.d_u_bid_cnt_14_30, 0.0) AS d_u_bid_cnt_14_30
    ,COALESCE(T2.d_u_contract_cnt_30, 0.0) AS d_u_contract_cnt_30
    ,COALESCE(T2.d_u_contract_cnt_365, 0.0) AS d_u_contract_cnt_365
    ,T3.d_u_click_road_haul_max_0_7
    ,T3.d_u_click_road_haul_min_0_7
    ,T3.d_u_click_road_haul_median_0_7
    ,T3.d_u_click_evaluate_score_max_0_7
    ,T3.d_u_click_evaluate_score_min_0_7
    ,T3.d_u_click_evaluate_score_median_0_7
    ,T4.d_u_bid_road_haul_max_0_7
    ,T4.d_u_bid_road_haul_min_0_7
    ,T4.d_u_bid_road_haul_median_0_7
    ,T4.d_u_bid_evaluate_score_max_0_7
    ,T4.d_u_bid_evaluate_score_min_0_7
    ,T4.d_u_bid_evaluate_score_median_0_7
    ,T5.d_u_click_road_haul_max_0_30
    ,T5.d_u_click_road_haul_min_0_30
    ,T5.d_u_click_road_haul_median_0_30
    ,T5.d_u_click_evaluate_score_max_0_30
    ,T5.d_u_click_evaluate_score_min_0_30
    ,T5.d_u_click_evaluate_score_median_0_30
    ,T6.d_u_bid_road_haul_max_0_30
    ,T6.d_u_bid_road_haul_min_0_30
    ,T6.d_u_bid_road_haul_median_0_30
    ,T6.d_u_bid_evaluate_score_max_0_30
    ,T6.d_u_bid_evaluate_score_min_0_30
    ,T6.d_u_bid_evaluate_score_median_0_30
    ,T7.d_u_deal_road_haul_max_0_30
    ,T7.d_u_deal_road_haul_min_0_30
    ,T7.d_u_deal_road_haul_median_0_30
    ,T7.d_u_deal_evaluate_score_max_0_30
    ,T7.d_u_deal_evaluate_score_min_0_30
    ,T7.d_u_deal_evaluate_score_median_0_30
    ,T8.d_u_deal_road_haul_max_0_365
    ,T8.d_u_deal_road_haul_min_0_365
    ,T8.d_u_deal_road_haul_median_0_365
    ,T8.d_u_deal_evaluate_score_max_0_365
    ,T8.d_u_deal_evaluate_score_min_0_365
    ,T8.d_u_deal_evaluate_score_median_0_365
    -- sparse类特征
    -- 点击特征
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
    ,if(L12.encode_id is null, 1, L12.encode_id) as s_u_click_c2b_evaluate_level_segment_0_7
    ,if(L13.encode_id is null, 1, L13.encode_id) as s_u_click_c2b_evaluate_score_segment_0_7
    ,if(L14.encode_id is null, 1, L14.encode_id) as s_u_click_model_price_bin_0_7
    -- 出价特征
    ,if(L15.encode_id is null, 1, L15.encode_id) as s_u_bid_tag_id_0_7
    ,if(L16.encode_id is null, 1, L16.encode_id) as s_u_bid_seats_0_7
    ,if(L17.encode_id is null, 1, L17.encode_id) as s_u_bid_minor_category_id_0_7
    ,if(L18.encode_id is null, 1, L18.encode_id) as s_u_bid_guobie_0_7
    ,if(L19.encode_id is null, 1, L19.encode_id) as s_u_bid_gearbox_0_7
    ,if(L20.encode_id is null, 1, L20.encode_id) as s_u_bid_fuel_type_0_7
    ,if(L21.encode_id is null, 1, L21.encode_id) as s_u_bid_emission_standard_0_7
    ,if(L22.encode_id is null, 1, L22.encode_id) as s_u_bid_city_id_0_7
    ,if(L23.encode_id is null, 1, L23.encode_id) as s_u_bid_car_year_0_7
    ,if(L24.encode_id is null, 1, L24.encode_id) as s_u_bid_auto_type_0_7
    ,if(L25.encode_id is null, 1, L25.encode_id) as s_u_bid_car_color_0_7
    ,if(L26.encode_id is null, 1, L26.encode_id) as s_u_bid_c2b_evaluate_level_segment_0_7
    ,if(L27.encode_id is null, 1, L27.encode_id) as s_u_bid_c2b_evaluate_score_segment_0_7
    ,if(L28.encode_id is null, 1, L28.encode_id) as s_u_bid_model_price_bin_0_7
    -- 下单特征
    ,if(L29.encode_id is null, 1, L29.encode_id) as s_u_deal_tag_id_0_30
    ,if(L30.encode_id is null, 1, L30.encode_id) as s_u_deal_seats_0_30
    ,if(L31.encode_id is null, 1, L31.encode_id) as s_u_deal_minor_category_id_0_30
    ,if(L32.encode_id is null, 1, L32.encode_id) as s_u_deal_guobie_0_30
    ,if(L33.encode_id is null, 1, L33.encode_id) as s_u_deal_gearbox_0_30
    ,if(L34.encode_id is null, 1, L34.encode_id) as s_u_deal_fuel_type_0_30
    ,if(L35.encode_id is null, 1, L35.encode_id) as s_u_deal_emission_standard_0_30
    ,if(L36.encode_id is null, 1, L36.encode_id) as s_u_deal_city_id_0_30
    ,if(L37.encode_id is null, 1, L37.encode_id) as s_u_deal_car_year_0_30
    ,if(L38.encode_id is null, 1, L38.encode_id) as s_u_deal_auto_type_0_30
    ,if(L39.encode_id is null, 1, L39.encode_id) as s_u_deal_car_color_0_30
    ,if(L40.encode_id is null, 1, L40.encode_id) as s_u_deal_tag_id_0_365
    ,if(L41.encode_id is null, 1, L41.encode_id) as s_u_deal_seats_0_365
    ,if(L42.encode_id is null, 1, L42.encode_id) as s_u_deal_minor_category_id_0_365
    ,if(L43.encode_id is null, 1, L43.encode_id) as s_u_deal_guobie_0_365
    ,if(L44.encode_id is null, 1, L44.encode_id) as s_u_deal_gearbox_0_365
    ,if(L45.encode_id is null, 1, L45.encode_id) as s_u_deal_fuel_type_0_365
    ,if(L46.encode_id is null, 1, L46.encode_id) as s_u_deal_emission_standard_0_365
    ,if(L47.encode_id is null, 1, L47.encode_id) as s_u_deal_city_id_0_365
    ,if(L48.encode_id is null, 1, L48.encode_id) as s_u_deal_car_year_0_365
    ,if(L49.encode_id is null, 1, L49.encode_id) as s_u_deal_auto_type_0_365
    ,if(L50.encode_id is null, 1, L50.encode_id) as s_u_deal_car_color_0_365
from user_action_cnt T1
left join user_convert_cnt T2 on T1.user_id = T2.user_id
left join user_click_statis_7d T3 on T1.user_id = T3.user_id
left join user_bid_statis_7d T4 on T1.user_id = T4.user_id
left join user_click_statis_30d T5 on T1.user_id = T5.user_id
left join user_bid_statis_30d T6 on T1.user_id = T6.user_id
left join user_deal_statis_30d T7 on T1.user_id = T7.user_id
left join user_deal_statis_365d T8 on T1.user_id = T8.user_id
-- sparse类特征
left join user_action_by_id_ratio_0_7 T9 on T1.user_id = T9.user_id
left join user_convert_by_id_ratio_0_30 T10 on T1.user_id = T10.user_id
left join user_convert_by_id_ratio_0_365 T11 on T1.user_id = T11.user_id
-- 点击
left join (select fea_val, encode_id from lookup_table where fea_name = 'tag_id' ) L1 on L1.fea_val = cast(T9.s_u_click_tag_id_0_7 as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'seats' ) L2 on L2.fea_val = cast(T9.s_u_click_seats_0_7 as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'minor_category_id' ) L3 on L3.fea_val = cast(T9.s_u_click_minor_category_id_0_7 as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'guobie' ) L4 on L4.fea_val = cast(T9.s_u_click_guobie_0_7 as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'gearbox' ) L5 on L5.fea_val = cast(T9.s_u_click_gearbox_0_7 as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'fuel_type' ) L6 on L6.fea_val = cast(T9.s_u_click_fuel_type_0_7 as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'emission_standard' ) L7 on L7.fea_val = cast(T9.s_u_click_emission_standard_0_7 as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'city_id' ) L8 on L8.fea_val = cast(T9.s_u_click_city_id_0_7 as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'car_year' ) L9 on L9.fea_val = cast(T9.s_u_click_car_year_0_7 as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'auto_type' ) L10 on L10.fea_val = cast(T9.s_u_click_auto_type_0_7 as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'car_color' ) L11 on L11.fea_val = cast(T9.s_u_click_car_color_0_7 as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'c2b_evaluate_level_segment' ) L12 on L12.fea_val = cast(T9.s_u_click_c2b_evaluate_level_segment_0_7 as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'c2b_evaluate_score_segment' ) L13 on L13.fea_val = cast(T9.s_u_click_c2b_evaluate_score_segment_0_7 as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'model_price_bin' ) L14 on L14.fea_val = cast(T9.s_u_click_model_price_bin_0_7 as string)
-- 出价
left join (select fea_val, encode_id from lookup_table where fea_name = 'tag_id' ) L15 on L15.fea_val = cast(T9.s_u_bid_tag_id_0_7 as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'seats' ) L16 on L16.fea_val = cast(T9.s_u_bid_seats_0_7 as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'minor_category_id' ) L17 on L17.fea_val = cast(T9.s_u_bid_minor_category_id_0_7 as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'guobie' ) L18 on L18.fea_val = cast(T9.s_u_bid_guobie_0_7 as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'gearbox' ) L19 on L19.fea_val = cast(T9.s_u_bid_gearbox_0_7 as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'fuel_type' ) L20 on L20.fea_val = cast(T9.s_u_bid_fuel_type_0_7 as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'emission_standard' ) L21 on L21.fea_val = cast(T9.s_u_bid_emission_standard_0_7 as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'city_id' ) L22 on L22.fea_val = cast(T9.s_u_bid_city_id_0_7 as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'car_year' ) L23 on L23.fea_val = cast(T9.s_u_bid_car_year_0_7 as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'auto_type' ) L24 on L24.fea_val = cast(T9.s_u_bid_auto_type_0_7 as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'car_color' ) L25 on L25.fea_val = cast(T9.s_u_bid_car_color_0_7 as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'c2b_evaluate_level_segment' ) L26 on L26.fea_val = cast(T9.s_u_bid_c2b_evaluate_level_segment_0_7 as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'c2b_evaluate_score_segment' ) L27 on L27.fea_val = cast(T9.s_u_bid_c2b_evaluate_score_segment_0_7 as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'model_price_bin' ) L28 on L28.fea_val = cast(T9.s_u_bid_model_price_bin_0_7 as string)
-- 下单
left join (select fea_val, encode_id from lookup_table where fea_name = 'tag_id' ) L29 on L29.fea_val = cast(T10.s_u_deal_tag_id_0_30 as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'seats' ) L30 on L30.fea_val = cast(T10.s_u_deal_seats_0_30 as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'minor_category_id' ) L31 on L31.fea_val = cast(T10.s_u_deal_minor_category_id_0_30 as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'guobie' ) L32 on L32.fea_val = cast(T10.s_u_deal_guobie_0_30 as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'gearbox' ) L33 on L33.fea_val = cast(T10.s_u_deal_gearbox_0_30 as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'fuel_type' ) L34 on L34.fea_val = cast(T10.s_u_deal_fuel_type_0_30 as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'emission_standard' ) L35 on L35.fea_val = cast(T10.s_u_deal_emission_standard_0_30 as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'city_id' ) L36 on L36.fea_val = cast(T10.s_u_deal_city_id_0_30 as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'car_year' ) L37 on L37.fea_val = cast(T10.s_u_deal_car_year_0_30 as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'auto_type' ) L38 on L38.fea_val = cast(T10.s_u_deal_auto_type_0_30 as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'car_color' ) L39 on L39.fea_val = cast(T10.s_u_deal_car_color_0_30 as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'tag_id' ) L40 on L40.fea_val = cast(T11.s_u_deal_tag_id_0_365 as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'seats' ) L41 on L41.fea_val = cast(T11.s_u_deal_seats_0_365 as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'minor_category_id' ) L42 on L42.fea_val = cast(T11.s_u_deal_minor_category_id_0_365 as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'guobie' ) L43 on L43.fea_val = cast(T11.s_u_deal_guobie_0_365 as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'gearbox' ) L44 on L44.fea_val = cast(T11.s_u_deal_gearbox_0_365 as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'fuel_type' ) L45 on L45.fea_val = cast(T11.s_u_deal_fuel_type_0_365 as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'emission_standard' ) L46 on L46.fea_val = cast(T11.s_u_deal_emission_standard_0_365 as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'city_id' ) L47 on L47.fea_val = cast(T11.s_u_deal_city_id_0_365 as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'car_year' ) L48 on L48.fea_val = cast(T11.s_u_deal_car_year_0_365 as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'auto_type' ) L49 on L49.fea_val = cast(T11.s_u_deal_auto_type_0_365 as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'car_color' ) L50 on L50.fea_val = cast(T11.s_u_deal_car_color_0_365 as string)