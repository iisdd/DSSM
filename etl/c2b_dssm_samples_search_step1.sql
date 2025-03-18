--item特征:dense & sparse
with samples as (
    select user_id
        ,clue_id
        ,recommend_id
        ,label
        ,ts
    from g3_feature_dev.c2b_dssm_train_samples
    where dt = '${date_y_m_d}'
    and scene = 'SEARCH'
    and action = 'bid'
    and clue_id is not null
)
,car_info_today as (
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
            ,cast(datediff('${date_y_m_d}', create_time) as int) as on_shelf_days
            ,road_haul
            ,transfer_num
            ,license_year
            ,air_displacement
            ,evaluate_score
            ,evaluate_level
        from guazi_dw_dwd.dim_com_car_source_ymd
        where dt = '${date_y_m_d}'
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
            where dt >= date_add('${date_y_m_d}', -90) and dt <= '${date_y_m_d}'
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
    where dt = date_add('${date_y_m_d}', -1)
)

,bid_percent_ratio_yesterday as (
    select cast(clue_id as int) as clue_id
        ,cast(c2b_offline_car_tag_id_bid_percent_ratio_0_7_d as double) as c2b_offline_car_tag_id_bid_percent_ratio_0_7_d
        ,cast(c2b_offline_car_tag_id_bid_percent_ratio_7_14_d as double) as c2b_offline_car_tag_id_bid_percent_ratio_7_14_d
        ,cast(c2b_offline_car_tag_id_bid_percent_ratio_14_30_d as double) as c2b_offline_car_tag_id_bid_percent_ratio_14_30_d
    from g3_feature_dev.c2b_offline_car_attr_bid_percent
    where dt = date_add('${date_y_m_d}', -1)
)

,attr_rate_yesterday as (
    select cast(clue_id as int) as clue_id
        ,cast(c2b_offline_car_tag_id_click_rate_0_7_d as double) as c2b_offline_car_tag_id_click_rate_0_7_d
        ,cast(c2b_offline_car_tag_id_click_rate_7_14_d as double) as c2b_offline_car_tag_id_click_rate_7_14_d
        ,cast(c2b_offline_car_tag_id_bid_rate_0_7_d as double) as c2b_offline_car_tag_id_bid_rate_0_7_d
        ,cast(c2b_offline_car_tag_id_bid_rate_7_14_d as double) as c2b_offline_car_tag_id_bid_rate_7_14_d
        ,cast(c2b_offline_car_minor_category_id_click_rate_0_7_d as double) as c2b_offline_car_minor_category_id_click_rate_0_7_d
    from g3_feature_dev.c2b_offline_car_attr_rate
    where dt = date_add('${date_y_m_d}', -1)
)
,car_features as (
    select A.clue_id as clue_id
        -- sparse特征
        ,evaluate_score_segment as s_i_c2b_evaluate_score_segment
        ,city_id as s_i_city_id
        ,emission_standard as s_i_emission_standard
        ,minor_category_id as s_i_minor_category_id
        ,model_price_bin as s_i_model_price_bin
        ,car_year as s_i_car_year
        ,gearbox as s_i_gearbox
        ,air_displacement as s_i_air_displacement
        ,tag_id as s_i_tag_id
        ,car_color as s_i_car_color
        ,guobie as s_i_guobie
        ,evaluate_level as s_i_c2b_evaluate_level
        ,auto_type as s_i_auto_type
        ,fuel_type as s_i_fuel_type
        ,seats as s_i_seats
        ,car_level as s_i_c2b_ctob_car_level
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
)
,lookup_table as (
    select *
    from g3_feature_dev.c2b_encoding_lookup_table
    where dt = '2024-11-01'
)

insert overwrite table g3_feature_dev.c2b_dssm_samples_search_step1 partition(dt='${date_y_m_d}')
select a.* 
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
    ,b.d_i_c2b_offline_car_click_bid_rate_0_3_d
    ,b.d_i_c2b_offline_car_click_bid_rate_0_30_d
    ,b.d_i_c2b_offline_car_beseen_click_rate_0_3_d
    ,b.d_i_c2b_offline_car_beseen_click_rate_0_30_d
    ,b.d_i_c2b_offline_car_tag_id_bid_percent_ratio_0_7_d
    ,b.d_i_c2b_offline_car_tag_id_bid_percent_ratio_7_14_d
    ,b.d_i_c2b_offline_car_tag_id_bid_percent_ratio_14_30_d
    ,b.d_i_c2b_offline_car_tag_id_click_rate_0_7_d
    ,b.d_i_c2b_offline_car_tag_id_click_rate_7_14_d
    ,b.d_i_c2b_offline_car_tag_id_bid_rate_0_7_d
    ,b.d_i_c2b_offline_car_tag_id_bid_rate_7_14_d
    ,b.d_i_c2b_offline_car_minor_category_id_click_rate_0_7_d
    ,b.d_i_transfer_num
    ,b.d_i_c2b_evaluate_score
    ,b.d_i_c2b_ctob_model_price
    ,b.d_i_c2b_seller_price
    ,b.d_i_c2b_ctob_diff_price
    ,b.d_i_road_haul
from samples a
left join car_features b
on a.clue_id = b.clue_id
left join (select fea_val, encode_id from lookup_table where fea_name = 'c2b_evaluate_score_segment') L1 on L1.fea_val = cast(b.s_i_c2b_evaluate_score_segment as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'city_id') L2 on L2.fea_val = cast(b.s_i_city_id as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'emission_standard') L3 on L3.fea_val = cast(b.s_i_emission_standard as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'minor_category_id') L4 on L4.fea_val = cast(b.s_i_minor_category_id as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'model_price_bin') L5 on L5.fea_val = cast(b.s_i_model_price_bin as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'car_year') L6 on L6.fea_val = cast(b.s_i_car_year as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'gearbox') L7 on L7.fea_val = cast(b.s_i_gearbox as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'air_displacement') L8 on L8.fea_val = cast(b.s_i_air_displacement as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'tag_id') L9 on L9.fea_val = cast(b.s_i_tag_id as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'car_color') L10 on L10.fea_val = cast(b.s_i_car_color as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'guobie') L11 on L11.fea_val = cast(b.s_i_guobie as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'c2b_evaluate_level') L12 on L12.fea_val = cast(b.s_i_c2b_evaluate_level as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'auto_type') L13 on L13.fea_val = cast(b.s_i_auto_type as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'fuel_type') L14 on L14.fea_val = cast(b.s_i_fuel_type as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'seats') L15 on L15.fea_val = cast(b.s_i_seats as string)
left join (select fea_val, encode_id from lookup_table where fea_name = 'c2b_ctob_car_level') L16 on L16.fea_val = cast(b.s_i_c2b_ctob_car_level as string)