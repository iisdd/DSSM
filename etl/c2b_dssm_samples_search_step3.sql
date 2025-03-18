--user序列特征 & 部分dense统计特征
with samples as (
    select *
    from g3_feature_dev.c2b_dssm_samples_search_step2
    where dt = '${date_y_m_d}'
)

,last_click_cars_history as ( -- 过去29天历史点击
    select user_id
        ,clue_id
        ,cast(ts as int) as ts
        ,row_number() over (partition by user_id order by cast(ts as int) desc) as rn
    from gzlc_real.fact_ctob_user_behavior
    where dt between date_add('${date_y_m_d}', -29)
    and date_add('${date_y_m_d}', -1)
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
    where dt = '${date_y_m_d}'
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
    where dt between date_add('${date_y_m_d}', -29)
    and date_add('${date_y_m_d}', -1)
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
    where dt = '${date_y_m_d}'
    and action = 'bid'
    and user_id != ''
    and clue_id != ''
)
,last_contract_cars_90d as ( -- 过去90天最近的10次下单
    select user_id
        ,clue_id
        ,unix_timestamp(to_timestamp(create_time, 'yyyy-MM-dd HH:mm:ss')) as create_time_ts
    from guazi_dw_dw.dw_ctob_appoint_prepay_detail_ymd
    where dt = '${date_y_m_d}'
    and substr(create_time, 1, 10) between date_add('${date_y_m_d}', -90) and date_add('${date_y_m_d}', -1)
    and user_id is not null
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
    where a.ts > b.ts+60
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
    where a.ts > b.ts+60
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
        ,row_number() over (partition by a.user_id, a.ts order by b.create_time_ts desc) as rn
    from cur_time_user a
    join last_contract_cars_90d b
    on a.user_id = b.user_id
    where a.ts > b.create_time_ts+60
)
,last_contract_top10 as ( -- 过去89天最近的10次下单
    select user_id
        ,clue_id
        ,ts
        ,rn
    from last_contract_cars_real
    where rn <= 10
)
,lookup_table as (
    select *
    from g3_feature_dev.c2b_encoding_lookup_table
    where dt = '2024-11-01'
)
,cars_origin as (
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
        where dt = '${date_y_m_d}'
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
,cars as (
    select co.clue_id
        ,if(L1.encode_id is null, 1, L1.encode_id) as fuel_type
        ,if(L2.encode_id is null, 1, L2.encode_id) as car_year
        ,if(L3.encode_id is null, 1, L3.encode_id) as gearbox
        ,if(L4.encode_id is null, 1, L4.encode_id) as emission_standard
        ,if(L5.encode_id is null, 1, L5.encode_id) as tag_id
        ,if(L6.encode_id is null, 1, L6.encode_id) as city_id
        ,if(L7.encode_id is null, 1, L7.encode_id) as car_color
        ,if(L8.encode_id is null, 1, L8.encode_id) as guobie
        ,if(L9.encode_id is null, 1, L9.encode_id) as minor_category_id
        ,if(L10.encode_id is null, 1, L10.encode_id) as auto_type
        ,if(L11.encode_id is null, 1, L11.encode_id) as seats
        ,co.seller_price
        ,co.road_haul
        ,co.evaluate_score
        ,co.transfer_num
        ,co.license_year
        ,co.license_month
        ,if(L12.encode_id is null, 1, L12.encode_id) as evaluate_level_segment
        ,if(L13.encode_id is null, 1, L13.encode_id) as evaluate_score_segment
        ,if(L14.encode_id is null, 1, L14.encode_id) as model_price_bin
        ,co.diff_price
        ,if(L15.encode_id is null, 1, L15.encode_id) as evaluate_level  -- 添加映射
        ,if(L16.encode_id is null, 1, L16.encode_id) as car_level       -- 添加映射
        ,co.model_price
    from cars_origin co
    left join (select fea_val, encode_id from lookup_table where fea_name = 'fuel_type') L1 on L1.fea_val = cast(co.fuel_type as string)
    left join (select fea_val, encode_id from lookup_table where fea_name = 'car_year') L2 on L2.fea_val = cast(co.car_year as string)
    left join (select fea_val, encode_id from lookup_table where fea_name = 'gearbox') L3 on L3.fea_val = cast(co.gearbox as string)
    left join (select fea_val, encode_id from lookup_table where fea_name = 'emission_standard') L4 on L4.fea_val = cast(co.emission_standard as string)
    left join (select fea_val, encode_id from lookup_table where fea_name = 'tag_id') L5 on L5.fea_val = cast(co.tag_id as string)
    left join (select fea_val, encode_id from lookup_table where fea_name = 'city_id') L6 on L6.fea_val = cast(co.city_id as string)
    left join (select fea_val, encode_id from lookup_table where fea_name = 'car_color') L7 on L7.fea_val = cast(co.car_color as string)
    left join (select fea_val, encode_id from lookup_table where fea_name = 'guobie') L8 on L8.fea_val = cast(co.guobie as string)
    left join (select fea_val, encode_id from lookup_table where fea_name = 'minor_category_id') L9 on L9.fea_val = cast(co.minor_category_id as string)
    left join (select fea_val, encode_id from lookup_table where fea_name = 'auto_type') L10 on L10.fea_val = cast(co.auto_type as string)
    left join (select fea_val, encode_id from lookup_table where fea_name = 'seats') L11 on L11.fea_val = cast(co.seats as string)
    left join (select fea_val, encode_id from lookup_table where fea_name = 'c2b_evaluate_level_segment') L12 on L12.fea_val = cast(co.evaluate_level_segment as string)
    left join (select fea_val, encode_id from lookup_table where fea_name = 'c2b_evaluate_score_segment') L13 on L13.fea_val = cast(co.evaluate_score_segment as string)
    left join (select fea_val, encode_id from lookup_table where fea_name = 'model_price_bin') L14 on L14.fea_val = cast(co.model_price_bin as string)
    left join (select fea_val, encode_id from lookup_table where fea_name = 'evaluate_level') L15 on L15.fea_val = cast(co.evaluate_level as string)  -- 添加映射
    left join (select fea_val, encode_id from lookup_table where fea_name = 'car_level') L16 on L16.fea_val = cast(co.car_level as string)  
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
insert overwrite table g3_feature_dev.c2b_dssm_samples_search_step3 partition(dt='${date_y_m_d}')

select a.user_id
    ,a.clue_id
    ,a.recommend_id
    ,a.label
    ,a.ts
    ,a.s_i_c2b_evaluate_score_segment
    ,a.s_i_city_id
    ,a.s_i_emission_standard
    ,a.s_i_minor_category_id
    ,a.s_i_model_price_bin
    ,a.s_i_car_year
    ,a.s_i_gearbox
    ,a.s_i_air_displacement
    ,a.s_i_tag_id
    ,a.s_i_car_color
    ,a.s_i_guobie
    ,a.s_i_c2b_evaluate_level
    ,a.s_i_auto_type
    ,a.s_i_fuel_type
    ,a.s_i_seats
    ,a.s_i_c2b_ctob_car_level
    ,a.d_i_c2b_offline_car_click_bid_rate_0_3_d
    ,a.d_i_c2b_offline_car_click_bid_rate_0_30_d
    ,a.d_i_c2b_offline_car_beseen_click_rate_0_3_d
    ,a.d_i_c2b_offline_car_beseen_click_rate_0_30_d
    ,a.d_i_c2b_offline_car_tag_id_bid_percent_ratio_0_7_d
    ,a.d_i_c2b_offline_car_tag_id_bid_percent_ratio_7_14_d
    ,a.d_i_c2b_offline_car_tag_id_bid_percent_ratio_14_30_d
    ,a.d_i_c2b_offline_car_tag_id_click_rate_0_7_d
    ,a.d_i_c2b_offline_car_tag_id_click_rate_7_14_d
    ,a.d_i_c2b_offline_car_tag_id_bid_rate_0_7_d
    ,a.d_i_c2b_offline_car_tag_id_bid_rate_7_14_d
    ,a.d_i_c2b_offline_car_minor_category_id_click_rate_0_7_d
    ,a.d_i_transfer_num
    ,a.d_i_c2b_evaluate_score
    ,a.d_i_c2b_ctob_model_price
    ,a.d_i_c2b_seller_price
    ,a.d_i_c2b_ctob_diff_price
    ,a.d_i_road_haul
    ,a.d_u_beseen_cnt_0_7
    ,a.d_u_beseen_cnt_7_14
    ,a.d_u_beseen_cnt_14_30
    ,a.d_u_click_cnt_0_7
    ,a.d_u_click_cnt_7_14
    ,a.d_u_click_cnt_14_30
    ,a.d_u_bid_cnt_0_7
    ,a.d_u_bid_cnt_7_14
    ,a.d_u_bid_cnt_14_30
    ,a.d_u_contract_cnt_30
    ,a.d_u_contract_cnt_365
    ,a.d_u_click_road_haul_max_0_7
    ,a.d_u_click_road_haul_min_0_7
    ,a.d_u_click_road_haul_median_0_7
    ,a.d_u_click_evaluate_score_max_0_7
    ,a.d_u_click_evaluate_score_min_0_7
    ,a.d_u_click_evaluate_score_median_0_7
    ,a.d_u_bid_road_haul_max_0_7
    ,a.d_u_bid_road_haul_min_0_7
    ,a.d_u_bid_road_haul_median_0_7
    ,a.d_u_bid_evaluate_score_max_0_7
    ,a.d_u_bid_evaluate_score_min_0_7
    ,a.d_u_bid_evaluate_score_median_0_7
    ,a.d_u_click_road_haul_max_0_30
    ,a.d_u_click_road_haul_min_0_30
    ,a.d_u_click_road_haul_median_0_30
    ,a.d_u_click_evaluate_score_max_0_30
    ,a.d_u_click_evaluate_score_min_0_30
    ,a.d_u_click_evaluate_score_median_0_30
    ,a.d_u_bid_road_haul_max_0_30
    ,a.d_u_bid_road_haul_min_0_30
    ,a.d_u_bid_road_haul_median_0_30
    ,a.d_u_bid_evaluate_score_max_0_30
    ,a.d_u_bid_evaluate_score_min_0_30
    ,a.d_u_bid_evaluate_score_median_0_30
    ,a.d_u_deal_road_haul_max_0_30
    ,a.d_u_deal_road_haul_min_0_30
    ,a.d_u_deal_road_haul_median_0_30
    ,a.d_u_deal_evaluate_score_max_0_30
    ,a.d_u_deal_evaluate_score_min_0_30
    ,a.d_u_deal_evaluate_score_median_0_30
    ,a.d_u_deal_road_haul_max_0_365
    ,a.d_u_deal_road_haul_min_0_365
    ,a.d_u_deal_road_haul_median_0_365
    ,a.d_u_deal_evaluate_score_max_0_365
    ,a.d_u_deal_evaluate_score_min_0_365
    ,a.d_u_deal_evaluate_score_median_0_365
    ,a.s_u_click_tag_id_0_7
    ,a.s_u_click_seats_0_7
    ,a.s_u_click_minor_category_id_0_7
    ,a.s_u_click_guobie_0_7
    ,a.s_u_click_gearbox_0_7
    ,a.s_u_click_fuel_type_0_7
    ,a.s_u_click_emission_standard_0_7
    ,a.s_u_click_city_id_0_7
    ,a.s_u_click_car_year_0_7
    ,a.s_u_click_auto_type_0_7
    ,a.s_u_click_car_color_0_7
    ,a.s_u_click_c2b_evaluate_level_segment_0_7
    ,a.s_u_click_c2b_evaluate_score_segment_0_7
    ,a.s_u_click_model_price_bin_0_7
    ,a.s_u_bid_tag_id_0_7
    ,a.s_u_bid_seats_0_7
    ,a.s_u_bid_minor_category_id_0_7
    ,a.s_u_bid_guobie_0_7
    ,a.s_u_bid_gearbox_0_7
    ,a.s_u_bid_fuel_type_0_7
    ,a.s_u_bid_emission_standard_0_7
    ,a.s_u_bid_city_id_0_7
    ,a.s_u_bid_car_year_0_7
    ,a.s_u_bid_auto_type_0_7
    ,a.s_u_bid_car_color_0_7
    ,a.s_u_bid_c2b_evaluate_level_segment_0_7
    ,a.s_u_bid_c2b_evaluate_score_segment_0_7
    ,a.s_u_bid_model_price_bin_0_7
    ,a.s_u_deal_tag_id_0_30
    ,a.s_u_deal_seats_0_30
    ,a.s_u_deal_minor_category_id_0_30
    ,a.s_u_deal_guobie_0_30
    ,a.s_u_deal_gearbox_0_30
    ,a.s_u_deal_fuel_type_0_30
    ,a.s_u_deal_emission_standard_0_30
    ,a.s_u_deal_city_id_0_30
    ,a.s_u_deal_car_year_0_30
    ,a.s_u_deal_auto_type_0_30
    ,a.s_u_deal_car_color_0_30
    ,a.s_u_deal_tag_id_0_365
    ,a.s_u_deal_seats_0_365
    ,a.s_u_deal_minor_category_id_0_365
    ,a.s_u_deal_guobie_0_365
    ,a.s_u_deal_gearbox_0_365
    ,a.s_u_deal_fuel_type_0_365
    ,a.s_u_deal_emission_standard_0_365
    ,a.s_u_deal_city_id_0_365
    ,a.s_u_deal_car_year_0_365
    ,a.s_u_deal_auto_type_0_365
    ,a.s_u_deal_car_color_0_365
    ,concat_ws(',', 
        case 
            when b.s_seq_c2b_click_clue_id_seq is null or size(b.s_seq_c2b_click_clue_id_seq) = 0
            then concat_ws(',', array_repeat('1', 10))
            when size(b.s_seq_c2b_click_clue_id_seq) < 10 
            then concat_ws(',', b.s_seq_c2b_click_clue_id_seq, array_repeat('1', 10 - size(b.s_seq_c2b_click_clue_id_seq)))
            else concat_ws(',', b.s_seq_c2b_click_clue_id_seq)
        end
    ) as s_seq_c2b_click_clue_id_seq
    ,concat_ws(',', 
        case 
            when b.s_seq_c2b_click_fuel_type_seq is null or size(b.s_seq_c2b_click_fuel_type_seq) = 0
            then concat_ws(',', array_repeat('1', 10))
            when size(b.s_seq_c2b_click_fuel_type_seq) < 10 
            then concat_ws(',', b.s_seq_c2b_click_fuel_type_seq, array_repeat('1', 10 - size(b.s_seq_c2b_click_fuel_type_seq)))
            else concat_ws(',', b.s_seq_c2b_click_fuel_type_seq)
        end
    ) as s_seq_c2b_click_fuel_type_seq
    ,concat_ws(',', 
        case 
            when b.s_seq_c2b_click_car_year_seq is null or size(b.s_seq_c2b_click_car_year_seq) = 0
            then concat_ws(',', array_repeat('1', 10))
            when size(b.s_seq_c2b_click_car_year_seq) < 10 
            then concat_ws(',', b.s_seq_c2b_click_car_year_seq, array_repeat('1', 10 - size(b.s_seq_c2b_click_car_year_seq)))
            else concat_ws(',', b.s_seq_c2b_click_car_year_seq)
        end
    ) as s_seq_c2b_click_car_year_seq
    ,concat_ws(',', 
        case 
            when b.s_seq_c2b_click_gearbox_seq is null or size(b.s_seq_c2b_click_gearbox_seq) = 0
            then concat_ws(',', array_repeat('1', 10))
            when size(b.s_seq_c2b_click_gearbox_seq) < 10 
            then concat_ws(',', b.s_seq_c2b_click_gearbox_seq, array_repeat('1', 10 - size(b.s_seq_c2b_click_gearbox_seq)))
            else concat_ws(',', b.s_seq_c2b_click_gearbox_seq)
        end
    ) as s_seq_c2b_click_gearbox_seq
    ,concat_ws(',', 
        case 
            when b.s_seq_c2b_click_emission_standard_seq is null or size(b.s_seq_c2b_click_emission_standard_seq) = 0
            then concat_ws(',', array_repeat('1', 10))
            when size(b.s_seq_c2b_click_emission_standard_seq) < 10 
            then concat_ws(',', b.s_seq_c2b_click_emission_standard_seq, array_repeat('1', 10 - size(b.s_seq_c2b_click_emission_standard_seq)))
            else concat_ws(',', b.s_seq_c2b_click_emission_standard_seq)
        end
    ) as s_seq_c2b_click_emission_standard_seq
    ,concat_ws(',', 
        case 
            when b.s_seq_c2b_click_tag_id_seq is null or size(b.s_seq_c2b_click_tag_id_seq) = 0
            then concat_ws(',', array_repeat('1', 10))
            when size(b.s_seq_c2b_click_tag_id_seq) < 10 
            then concat_ws(',', b.s_seq_c2b_click_tag_id_seq, array_repeat('1', 10 - size(b.s_seq_c2b_click_tag_id_seq)))
            else concat_ws(',', b.s_seq_c2b_click_tag_id_seq)
        end
    ) as s_seq_c2b_click_tag_id_seq
    ,concat_ws(',', 
        case 
            when b.s_seq_c2b_click_city_id_seq is null or size(b.s_seq_c2b_click_city_id_seq) = 0
            then concat_ws(',', array_repeat('1', 10))
            when size(b.s_seq_c2b_click_city_id_seq) < 10 
            then concat_ws(',', b.s_seq_c2b_click_city_id_seq, array_repeat('1', 10 - size(b.s_seq_c2b_click_city_id_seq)))
            else concat_ws(',', b.s_seq_c2b_click_city_id_seq)
        end
    ) as s_seq_c2b_click_city_id_seq
    ,concat_ws(',', 
        case 
            when b.s_seq_c2b_click_car_color_seq is null or size(b.s_seq_c2b_click_car_color_seq) = 0
            then concat_ws(',', array_repeat('1', 10))
            when size(b.s_seq_c2b_click_car_color_seq) < 10 
            then concat_ws(',', b.s_seq_c2b_click_car_color_seq, array_repeat('1', 10 - size(b.s_seq_c2b_click_car_color_seq)))
            else concat_ws(',', b.s_seq_c2b_click_car_color_seq)
        end
    ) as s_seq_c2b_click_car_color_seq
    ,concat_ws(',', 
        case 
            when b.s_seq_c2b_click_guobie_seq is null or size(b.s_seq_c2b_click_guobie_seq) = 0
            then concat_ws(',', array_repeat('1', 10))
            when size(b.s_seq_c2b_click_guobie_seq) < 10 
            then concat_ws(',', b.s_seq_c2b_click_guobie_seq, array_repeat('1', 10 - size(b.s_seq_c2b_click_guobie_seq)))
            else concat_ws(',', b.s_seq_c2b_click_guobie_seq)
        end
    ) as s_seq_c2b_click_guobie_seq
    ,concat_ws(',', 
        case 
            when b.s_seq_c2b_click_minor_category_id_seq is null or size(b.s_seq_c2b_click_minor_category_id_seq) = 0
            then concat_ws(',', array_repeat('1', 10))
            when size(b.s_seq_c2b_click_minor_category_id_seq) < 10 
            then concat_ws(',', b.s_seq_c2b_click_minor_category_id_seq, array_repeat('1', 10 - size(b.s_seq_c2b_click_minor_category_id_seq)))
            else concat_ws(',', b.s_seq_c2b_click_minor_category_id_seq)
        end
    ) as s_seq_c2b_click_minor_category_id_seq
    ,concat_ws(',', 
        case 
            when b.s_seq_c2b_click_auto_type_seq is null or size(b.s_seq_c2b_click_auto_type_seq) = 0
            then concat_ws(',', array_repeat('1', 10))
            when size(b.s_seq_c2b_click_auto_type_seq) < 10 
            then concat_ws(',', b.s_seq_c2b_click_auto_type_seq, array_repeat('1', 10 - size(b.s_seq_c2b_click_auto_type_seq)))
            else concat_ws(',', b.s_seq_c2b_click_auto_type_seq)
        end
    ) as s_seq_c2b_click_auto_type_seq
    ,concat_ws(',', 
        case 
            when b.s_seq_c2b_click_seats_seq is null or size(b.s_seq_c2b_click_seats_seq) = 0
            then concat_ws(',', array_repeat('1', 10))
            when size(b.s_seq_c2b_click_seats_seq) < 10 
            then concat_ws(',', b.s_seq_c2b_click_seats_seq, array_repeat('1', 10 - size(b.s_seq_c2b_click_seats_seq)))
            else concat_ws(',', b.s_seq_c2b_click_seats_seq)
        end
    ) as s_seq_c2b_click_seats_seq
    ,concat_ws(',', 
        case 
            when b.s_seq_c2b_click_evaluate_level_seq is null or size(b.s_seq_c2b_click_evaluate_level_seq) = 0
            then concat_ws(',', array_repeat('1', 10))
            when size(b.s_seq_c2b_click_evaluate_level_seq) < 10 
            then concat_ws(',', b.s_seq_c2b_click_evaluate_level_seq, array_repeat('1', 10 - size(b.s_seq_c2b_click_evaluate_level_seq)))
            else concat_ws(',', b.s_seq_c2b_click_evaluate_level_seq)
        end
    ) as s_seq_c2b_click_evaluate_level_seq
    ,concat_ws(',', 
        case 
            when b.s_seq_c2b_click_c2b_evaluate_score_segment_seq is null or size(b.s_seq_c2b_click_c2b_evaluate_score_segment_seq) = 0
            then concat_ws(',', array_repeat('1', 10))
            when size(b.s_seq_c2b_click_c2b_evaluate_score_segment_seq) < 10 
            then concat_ws(',', b.s_seq_c2b_click_c2b_evaluate_score_segment_seq, array_repeat('1', 10 - size(b.s_seq_c2b_click_c2b_evaluate_score_segment_seq)))
            else concat_ws(',', b.s_seq_c2b_click_c2b_evaluate_score_segment_seq)
        end
    ) as s_seq_c2b_click_c2b_evaluate_score_segment_seq
    ,concat_ws(',', 
        case 
            when b.s_seq_c2b_click_c2b_ctob_car_level_seq is null or size(b.s_seq_c2b_click_c2b_ctob_car_level_seq) = 0
            then concat_ws(',', array_repeat('1', 10))
            when size(b.s_seq_c2b_click_c2b_ctob_car_level_seq) < 10 
            then concat_ws(',', b.s_seq_c2b_click_c2b_ctob_car_level_seq, array_repeat('1', 10 - size(b.s_seq_c2b_click_c2b_ctob_car_level_seq)))
            else concat_ws(',', b.s_seq_c2b_click_c2b_ctob_car_level_seq)
        end
    ) as s_seq_c2b_click_c2b_ctob_car_level_seq
    ,concat_ws(',', 
        case 
            when b.s_seq_c2b_click_model_price_bin_seq is null or size(b.s_seq_c2b_click_model_price_bin_seq) = 0
            then concat_ws(',', array_repeat('1', 10))
            when size(b.s_seq_c2b_click_model_price_bin_seq) < 10 
            then concat_ws(',', b.s_seq_c2b_click_model_price_bin_seq, array_repeat('1', 10 - size(b.s_seq_c2b_click_model_price_bin_seq)))
            else concat_ws(',', b.s_seq_c2b_click_model_price_bin_seq)
        end
    ) as s_seq_c2b_click_model_price_bin_seq
    ,concat_ws(',', b.d_seq_c2b_click_c2b_seller_price_seq) as d_seq_c2b_click_c2b_seller_price_seq
    ,concat_ws(',', b.d_seq_c2b_click_road_haul_seq) as d_seq_c2b_click_road_haul_seq
    ,concat_ws(',', b.d_seq_c2b_click_c2b_evaluate_score_seq) as d_seq_c2b_click_c2b_evaluate_score_seq
    ,concat_ws(',', b.d_seq_c2b_click_transfer_num_seq) as d_seq_c2b_click_transfer_num_seq
    ,concat_ws(',', b.d_seq_c2b_click_c2b_ctob_model_price_seq) as d_seq_c2b_click_c2b_ctob_model_price_seq
    ,concat_ws(',', b.d_seq_c2b_click_c2b_ctob_diff_price_seq) as d_seq_c2b_click_c2b_ctob_diff_price_seq
    ,b.d_u_c2b_realtime_user_road_haul_avg_fix
    ,b.d_u_c2b_realtime_user_evaluate_score_avg
    ,b.d_u_c2b_realtime_user_seller_price_avg
    ,b.d_u_c2b_realtime_user_model_price_avg
    ,b.d_u_c2b_realtime_user_diff_price_avg
    ,b.d_u_c2b_realtime_user_road_haul_max_fix
    ,b.d_u_c2b_realtime_user_evaluate_score_max
    ,b.d_u_c2b_realtime_user_seller_price_max
    ,b.d_u_c2b_realtime_user_model_price_max
    ,b.d_u_c2b_realtime_user_diff_price_max
    ,b.d_u_c2b_realtime_user_road_haul_min_fix
    ,b.d_u_c2b_realtime_user_evaluate_score_min
    ,b.d_u_c2b_realtime_user_seller_price_min
    ,b.d_u_c2b_realtime_user_model_price_min
    ,b.d_u_c2b_realtime_user_diff_price_min
    ,b.d_u_c2b_realtime_user_road_haul_p50_fix
    ,b.d_u_c2b_realtime_user_evaluate_score_p50
    ,b.d_u_c2b_realtime_user_seller_price_p50
    ,b.d_u_c2b_realtime_user_model_price_p50
    ,b.d_u_c2b_realtime_user_diff_price_p50
    ,concat_ws(',', 
        case 
            when c.s_seq_c2b_bid_clue_id_seq is null or size(c.s_seq_c2b_bid_clue_id_seq) = 0
            then concat_ws(',', array_repeat('1', 10))
            when size(c.s_seq_c2b_bid_clue_id_seq) < 10 
            then concat_ws(',', c.s_seq_c2b_bid_clue_id_seq, array_repeat('1', 10 - size(c.s_seq_c2b_bid_clue_id_seq)))
            else concat_ws(',', c.s_seq_c2b_bid_clue_id_seq)
        end
    ) as s_seq_c2b_bid_clue_id_seq
    ,concat_ws(',', 
        case 
            when c.s_seq_c2b_bid_fuel_type_seq is null or size(c.s_seq_c2b_bid_fuel_type_seq) = 0
            then concat_ws(',', array_repeat('1', 10))
            when size(c.s_seq_c2b_bid_fuel_type_seq) < 10 
            then concat_ws(',', c.s_seq_c2b_bid_fuel_type_seq, array_repeat('1', 10 - size(c.s_seq_c2b_bid_fuel_type_seq)))
            else concat_ws(',', c.s_seq_c2b_bid_fuel_type_seq)
        end
    ) as s_seq_c2b_bid_fuel_type_seq
    ,concat_ws(',', 
        case 
            when c.s_seq_c2b_bid_car_year_seq is null or size(c.s_seq_c2b_bid_car_year_seq) = 0
            then concat_ws(',', array_repeat('1', 10))
            when size(c.s_seq_c2b_bid_car_year_seq) < 10 
            then concat_ws(',', c.s_seq_c2b_bid_car_year_seq, array_repeat('1', 10 - size(c.s_seq_c2b_bid_car_year_seq)))
            else concat_ws(',', c.s_seq_c2b_bid_car_year_seq)
        end
    ) as s_seq_c2b_bid_car_year_seq
    ,concat_ws(',', 
        case 
            when c.s_seq_c2b_bid_gearbox_seq is null or size(c.s_seq_c2b_bid_gearbox_seq) = 0
            then concat_ws(',', array_repeat('1', 10))
            when size(c.s_seq_c2b_bid_gearbox_seq) < 10 
            then concat_ws(',', c.s_seq_c2b_bid_gearbox_seq, array_repeat('1', 10 - size(c.s_seq_c2b_bid_gearbox_seq)))
            else concat_ws(',', c.s_seq_c2b_bid_gearbox_seq)
        end
    ) as s_seq_c2b_bid_gearbox_seq
    ,concat_ws(',', 
        case 
            when c.s_seq_c2b_bid_emission_standard_seq is null or size(c.s_seq_c2b_bid_emission_standard_seq) = 0
            then concat_ws(',', array_repeat('1', 10))
            when size(c.s_seq_c2b_bid_emission_standard_seq) < 10 
            then concat_ws(',', c.s_seq_c2b_bid_emission_standard_seq, array_repeat('1', 10 - size(c.s_seq_c2b_bid_emission_standard_seq)))
            else concat_ws(',', c.s_seq_c2b_bid_emission_standard_seq)
        end
    ) as s_seq_c2b_bid_emission_standard_seq
    ,concat_ws(',', 
        case 
            when c.s_seq_c2b_bid_tag_id_seq is null or size(c.s_seq_c2b_bid_tag_id_seq) = 0
            then concat_ws(',', array_repeat('1', 10))
            when size(c.s_seq_c2b_bid_tag_id_seq) < 10 
            then concat_ws(',', c.s_seq_c2b_bid_tag_id_seq, array_repeat('1', 10 - size(c.s_seq_c2b_bid_tag_id_seq)))
            else concat_ws(',', c.s_seq_c2b_bid_tag_id_seq)
        end
    ) as s_seq_c2b_bid_tag_id_seq
    ,concat_ws(',', 
        case 
            when c.s_seq_c2b_bid_city_id_seq is null or size(c.s_seq_c2b_bid_city_id_seq) = 0
            then concat_ws(',', array_repeat('1', 10))
            when size(c.s_seq_c2b_bid_city_id_seq) < 10 
            then concat_ws(',', c.s_seq_c2b_bid_city_id_seq, array_repeat('1', 10 - size(c.s_seq_c2b_bid_city_id_seq)))
            else concat_ws(',', c.s_seq_c2b_bid_city_id_seq)
        end
    ) as s_seq_c2b_bid_city_id_seq
    ,concat_ws(',', 
        case 
            when c.s_seq_c2b_bid_car_color_seq is null or size(c.s_seq_c2b_bid_car_color_seq) = 0
            then concat_ws(',', array_repeat('1', 10))
            when size(c.s_seq_c2b_bid_car_color_seq) < 10 
            then concat_ws(',', c.s_seq_c2b_bid_car_color_seq, array_repeat('1', 10 - size(c.s_seq_c2b_bid_car_color_seq)))
            else concat_ws(',', c.s_seq_c2b_bid_car_color_seq)
        end
    ) as s_seq_c2b_bid_car_color_seq
    ,concat_ws(',', 
        case 
            when c.s_seq_c2b_bid_guobie_seq is null or size(c.s_seq_c2b_bid_guobie_seq) = 0
            then concat_ws(',', array_repeat('1', 10))
            when size(c.s_seq_c2b_bid_guobie_seq) < 10 
            then concat_ws(',', c.s_seq_c2b_bid_guobie_seq, array_repeat('1', 10 - size(c.s_seq_c2b_bid_guobie_seq)))
            else concat_ws(',', c.s_seq_c2b_bid_guobie_seq)
        end
    ) as s_seq_c2b_bid_guobie_seq
    ,concat_ws(',', 
        case 
            when c.s_seq_c2b_bid_minor_category_id_seq is null or size(c.s_seq_c2b_bid_minor_category_id_seq) = 0
            then concat_ws(',', array_repeat('1', 10))
            when size(c.s_seq_c2b_bid_minor_category_id_seq) < 10 
            then concat_ws(',', c.s_seq_c2b_bid_minor_category_id_seq, array_repeat('1', 10 - size(c.s_seq_c2b_bid_minor_category_id_seq)))
            else concat_ws(',', c.s_seq_c2b_bid_minor_category_id_seq)
        end
    ) as s_seq_c2b_bid_minor_category_id_seq
    ,concat_ws(',', 
        case 
            when c.s_seq_c2b_bid_auto_type_seq is null or size(c.s_seq_c2b_bid_auto_type_seq) = 0
            then concat_ws(',', array_repeat('1', 10))
            when size(c.s_seq_c2b_bid_auto_type_seq) < 10 
            then concat_ws(',', c.s_seq_c2b_bid_auto_type_seq, array_repeat('1', 10 - size(c.s_seq_c2b_bid_auto_type_seq)))
            else concat_ws(',', c.s_seq_c2b_bid_auto_type_seq)
        end
    ) as s_seq_c2b_bid_auto_type_seq
    ,concat_ws(',', 
        case 
            when c.s_seq_c2b_bid_seats_seq is null or size(c.s_seq_c2b_bid_seats_seq) = 0
            then concat_ws(',', array_repeat('1', 10))
            when size(c.s_seq_c2b_bid_seats_seq) < 10 
            then concat_ws(',', c.s_seq_c2b_bid_seats_seq, array_repeat('1', 10 - size(c.s_seq_c2b_bid_seats_seq)))
            else concat_ws(',', c.s_seq_c2b_bid_seats_seq)
        end
    ) as s_seq_c2b_bid_seats_seq
    ,concat_ws(',', 
        case 
            when c.s_seq_c2b_bid_evaluate_level_seq is null or size(c.s_seq_c2b_bid_evaluate_level_seq) = 0
            then concat_ws(',', array_repeat('1', 10))
            when size(c.s_seq_c2b_bid_evaluate_level_seq) < 10 
            then concat_ws(',', c.s_seq_c2b_bid_evaluate_level_seq, array_repeat('1', 10 - size(c.s_seq_c2b_bid_evaluate_level_seq)))
            else concat_ws(',', c.s_seq_c2b_bid_evaluate_level_seq)
        end
    ) as s_seq_c2b_bid_evaluate_level_seq
    ,concat_ws(',', 
        case 
            when c.s_seq_c2b_bid_c2b_evaluate_score_segment_seq is null or size(c.s_seq_c2b_bid_c2b_evaluate_score_segment_seq) = 0
            then concat_ws(',', array_repeat('1', 10))
            when size(c.s_seq_c2b_bid_c2b_evaluate_score_segment_seq) < 10 
            then concat_ws(',', c.s_seq_c2b_bid_c2b_evaluate_score_segment_seq, array_repeat('1', 10 - size(c.s_seq_c2b_bid_c2b_evaluate_score_segment_seq)))
            else concat_ws(',', c.s_seq_c2b_bid_c2b_evaluate_score_segment_seq)
        end
    ) as s_seq_c2b_bid_c2b_evaluate_score_segment_seq
    ,concat_ws(',', 
        case 
            when c.s_seq_c2b_bid_c2b_ctob_car_level_seq is null or size(c.s_seq_c2b_bid_c2b_ctob_car_level_seq) = 0
            then concat_ws(',', array_repeat('1', 10))
            when size(c.s_seq_c2b_bid_c2b_ctob_car_level_seq) < 10 
            then concat_ws(',', c.s_seq_c2b_bid_c2b_ctob_car_level_seq, array_repeat('1', 10 - size(c.s_seq_c2b_bid_c2b_ctob_car_level_seq)))
            else concat_ws(',', c.s_seq_c2b_bid_c2b_ctob_car_level_seq)
        end
    ) as s_seq_c2b_bid_c2b_ctob_car_level_seq
    ,concat_ws(',', 
        case 
            when c.s_seq_c2b_bid_model_price_bin_seq is null or size(c.s_seq_c2b_bid_model_price_bin_seq) = 0
            then concat_ws(',', array_repeat('1', 10))
            when size(c.s_seq_c2b_bid_model_price_bin_seq) < 10 
            then concat_ws(',', c.s_seq_c2b_bid_model_price_bin_seq, array_repeat('1', 10 - size(c.s_seq_c2b_bid_model_price_bin_seq)))
            else concat_ws(',', c.s_seq_c2b_bid_model_price_bin_seq)
        end
    ) as s_seq_c2b_bid_model_price_bin_seq
    ,concat_ws(',', c.d_seq_c2b_bid_c2b_seller_price_seq) as d_seq_c2b_bid_c2b_seller_price_seq
    ,concat_ws(',', c.d_seq_c2b_bid_road_haul_seq) as d_seq_c2b_bid_road_haul_seq
    ,concat_ws(',', c.d_seq_c2b_bid_c2b_evaluate_score_seq) as d_seq_c2b_bid_c2b_evaluate_score_seq
    ,concat_ws(',', c.d_seq_c2b_bid_transfer_num_seq) as d_seq_c2b_bid_transfer_num_seq
    ,concat_ws(',', c.d_seq_c2b_bid_c2b_ctob_model_price_seq) as d_seq_c2b_bid_c2b_ctob_model_price_seq
    ,concat_ws(',', c.d_seq_c2b_bid_c2b_ctob_diff_price_seq) as d_seq_c2b_bid_c2b_ctob_diff_price_seq
    ,concat_ws(',', 
        case 
            when d.s_seq_c2b_contract_clue_id_seq is null or size(d.s_seq_c2b_contract_clue_id_seq) = 0
            then concat_ws(',', array_repeat('1', 10))
            when size(d.s_seq_c2b_contract_clue_id_seq) < 10 
            then concat_ws(',', d.s_seq_c2b_contract_clue_id_seq, array_repeat('1', 10 - size(d.s_seq_c2b_contract_clue_id_seq)))
            else concat_ws(',', d.s_seq_c2b_contract_clue_id_seq)
        end
    ) as s_seq_c2b_contract_clue_id_seq
    ,concat_ws(',', 
        case 
            when d.s_seq_c2b_contract_fuel_type_seq is null or size(d.s_seq_c2b_contract_fuel_type_seq) = 0
            then concat_ws(',', array_repeat('1', 10))
            when size(d.s_seq_c2b_contract_fuel_type_seq) < 10 
            then concat_ws(',', d.s_seq_c2b_contract_fuel_type_seq, array_repeat('1', 10 - size(d.s_seq_c2b_contract_fuel_type_seq)))
            else concat_ws(',', d.s_seq_c2b_contract_fuel_type_seq)
        end
    ) as s_seq_c2b_contract_fuel_type_seq
    ,concat_ws(',', 
        case 
            when d.s_seq_c2b_contract_car_year_seq is null or size(d.s_seq_c2b_contract_car_year_seq) = 0
            then concat_ws(',', array_repeat('1', 10))
            when size(d.s_seq_c2b_contract_car_year_seq) < 10 
            then concat_ws(',', d.s_seq_c2b_contract_car_year_seq, array_repeat('1', 10 - size(d.s_seq_c2b_contract_car_year_seq)))
            else concat_ws(',', d.s_seq_c2b_contract_car_year_seq)
        end
    ) as s_seq_c2b_contract_car_year_seq
    ,concat_ws(',', 
        case 
            when d.s_seq_c2b_contract_gearbox_seq is null or size(d.s_seq_c2b_contract_gearbox_seq) = 0
            then concat_ws(',', array_repeat('1', 10))
            when size(d.s_seq_c2b_contract_gearbox_seq) < 10 
            then concat_ws(',', d.s_seq_c2b_contract_gearbox_seq, array_repeat('1', 10 - size(d.s_seq_c2b_contract_gearbox_seq)))
            else concat_ws(',', d.s_seq_c2b_contract_gearbox_seq)
        end
    ) as s_seq_c2b_contract_gearbox_seq
    ,concat_ws(',', 
        case 
            when d.s_seq_c2b_contract_emission_standard_seq is null or size(d.s_seq_c2b_contract_emission_standard_seq) = 0
            then concat_ws(',', array_repeat('1', 10))
            when size(d.s_seq_c2b_contract_emission_standard_seq) < 10 
            then concat_ws(',', d.s_seq_c2b_contract_emission_standard_seq, array_repeat('1', 10 - size(d.s_seq_c2b_contract_emission_standard_seq)))
            else concat_ws(',', d.s_seq_c2b_contract_emission_standard_seq)
        end
    ) as s_seq_c2b_contract_emission_standard_seq
    ,concat_ws(',', 
        case 
            when d.s_seq_c2b_contract_tag_id_seq is null or size(d.s_seq_c2b_contract_tag_id_seq) = 0
            then concat_ws(',', array_repeat('1', 10))
            when size(d.s_seq_c2b_contract_tag_id_seq) < 10 
            then concat_ws(',', d.s_seq_c2b_contract_tag_id_seq, array_repeat('1', 10 - size(d.s_seq_c2b_contract_tag_id_seq)))
            else concat_ws(',', d.s_seq_c2b_contract_tag_id_seq)
        end
    ) as s_seq_c2b_contract_tag_id_seq
    ,concat_ws(',', 
        case 
            when d.s_seq_c2b_contract_city_id_seq is null or size(d.s_seq_c2b_contract_city_id_seq) = 0
            then concat_ws(',', array_repeat('1', 10))
            when size(d.s_seq_c2b_contract_city_id_seq) < 10 
            then concat_ws(',', d.s_seq_c2b_contract_city_id_seq, array_repeat('1', 10 - size(d.s_seq_c2b_contract_city_id_seq)))
            else concat_ws(',', d.s_seq_c2b_contract_city_id_seq)
        end
    ) as s_seq_c2b_contract_city_id_seq
    ,concat_ws(',', 
        case 
            when d.s_seq_c2b_contract_car_color_seq is null or size(d.s_seq_c2b_contract_car_color_seq) = 0
            then concat_ws(',', array_repeat('1', 10))
            when size(d.s_seq_c2b_contract_car_color_seq) < 10 
            then concat_ws(',', d.s_seq_c2b_contract_car_color_seq, array_repeat('1', 10 - size(d.s_seq_c2b_contract_car_color_seq)))
            else concat_ws(',', d.s_seq_c2b_contract_car_color_seq)
        end
    ) as s_seq_c2b_contract_car_color_seq
    ,concat_ws(',', 
        case 
            when d.s_seq_c2b_contract_guobie_seq is null or size(d.s_seq_c2b_contract_guobie_seq) = 0
            then concat_ws(',', array_repeat('1', 10))
            when size(d.s_seq_c2b_contract_guobie_seq) < 10 
            then concat_ws(',', d.s_seq_c2b_contract_guobie_seq, array_repeat('1', 10 - size(d.s_seq_c2b_contract_guobie_seq)))
            else concat_ws(',', d.s_seq_c2b_contract_guobie_seq)
        end
    ) as s_seq_c2b_contract_guobie_seq
    ,concat_ws(',', 
        case 
            when d.s_seq_c2b_contract_minor_category_id_seq is null or size(d.s_seq_c2b_contract_minor_category_id_seq) = 0
            then concat_ws(',', array_repeat('1', 10))
            when size(d.s_seq_c2b_contract_minor_category_id_seq) < 10 
            then concat_ws(',', d.s_seq_c2b_contract_minor_category_id_seq, array_repeat('1', 10 - size(d.s_seq_c2b_contract_minor_category_id_seq)))
            else concat_ws(',', d.s_seq_c2b_contract_minor_category_id_seq)
        end
    ) as s_seq_c2b_contract_minor_category_id_seq
    ,concat_ws(',', 
        case 
            when d.s_seq_c2b_contract_auto_type_seq is null or size(d.s_seq_c2b_contract_auto_type_seq) = 0
            then concat_ws(',', array_repeat('1', 10))
            when size(d.s_seq_c2b_contract_auto_type_seq) < 10 
            then concat_ws(',', d.s_seq_c2b_contract_auto_type_seq, array_repeat('1', 10 - size(d.s_seq_c2b_contract_auto_type_seq)))
            else concat_ws(',', d.s_seq_c2b_contract_auto_type_seq)
        end
    ) as s_seq_c2b_contract_auto_type_seq
    ,concat_ws(',', 
        case 
            when d.s_seq_c2b_contract_seats_seq is null or size(d.s_seq_c2b_contract_seats_seq) = 0
            then concat_ws(',', array_repeat('1', 10))
            when size(d.s_seq_c2b_contract_seats_seq) < 10 
            then concat_ws(',', d.s_seq_c2b_contract_seats_seq, array_repeat('1', 10 - size(d.s_seq_c2b_contract_seats_seq)))
            else concat_ws(',', d.s_seq_c2b_contract_seats_seq)
        end
    ) as s_seq_c2b_contract_seats_seq
    ,concat_ws(',', 
        case 
            when d.s_seq_c2b_contract_evaluate_level_seq is null or size(d.s_seq_c2b_contract_evaluate_level_seq) = 0
            then concat_ws(',', array_repeat('1', 10))
            when size(d.s_seq_c2b_contract_evaluate_level_seq) < 10 
            then concat_ws(',', d.s_seq_c2b_contract_evaluate_level_seq, array_repeat('1', 10 - size(d.s_seq_c2b_contract_evaluate_level_seq)))
            else concat_ws(',', d.s_seq_c2b_contract_evaluate_level_seq)
        end
    ) as s_seq_c2b_contract_evaluate_level_seq
    ,concat_ws(',', 
        case 
            when d.s_seq_c2b_contract_c2b_evaluate_score_segment_seq is null or size(d.s_seq_c2b_contract_c2b_evaluate_score_segment_seq) = 0
            then concat_ws(',', array_repeat('1', 10))
            when size(d.s_seq_c2b_contract_c2b_evaluate_score_segment_seq) < 10 
            then concat_ws(',', d.s_seq_c2b_contract_c2b_evaluate_score_segment_seq, array_repeat('1', 10 - size(d.s_seq_c2b_contract_c2b_evaluate_score_segment_seq)))
            else concat_ws(',', d.s_seq_c2b_contract_c2b_evaluate_score_segment_seq)
        end
    ) as s_seq_c2b_contract_c2b_evaluate_score_segment_seq
    ,concat_ws(',', 
        case 
            when d.s_seq_c2b_contract_c2b_ctob_car_level_seq is null or size(d.s_seq_c2b_contract_c2b_ctob_car_level_seq) = 0
            then concat_ws(',', array_repeat('1', 10))
            when size(d.s_seq_c2b_contract_c2b_ctob_car_level_seq) < 10 
            then concat_ws(',', d.s_seq_c2b_contract_c2b_ctob_car_level_seq, array_repeat('1', 10 - size(d.s_seq_c2b_contract_c2b_ctob_car_level_seq)))
            else concat_ws(',', d.s_seq_c2b_contract_c2b_ctob_car_level_seq)
        end
    ) as s_seq_c2b_contract_c2b_ctob_car_level_seq
    ,concat_ws(',', 
        case 
            when d.s_seq_c2b_contract_model_price_bin_seq is null or size(d.s_seq_c2b_contract_model_price_bin_seq) = 0
            then concat_ws(',', array_repeat('1', 10))
            when size(d.s_seq_c2b_contract_model_price_bin_seq) < 10 
            then concat_ws(',', d.s_seq_c2b_contract_model_price_bin_seq, array_repeat('1', 10 - size(d.s_seq_c2b_contract_model_price_bin_seq)))
            else concat_ws(',', d.s_seq_c2b_contract_model_price_bin_seq)
        end
    ) as s_seq_c2b_contract_model_price_bin_seq
    ,concat_ws(',', d.d_seq_c2b_contract_c2b_seller_price_seq) as d_seq_c2b_contract_c2b_seller_price_seq
    ,concat_ws(',', d.d_seq_c2b_contract_road_haul_seq) as d_seq_c2b_contract_road_haul_seq
    ,concat_ws(',', d.d_seq_c2b_contract_c2b_evaluate_score_seq) as d_seq_c2b_contract_c2b_evaluate_score_seq
    ,concat_ws(',', d.d_seq_c2b_contract_transfer_num_seq) as d_seq_c2b_contract_transfer_num_seq
    ,concat_ws(',', d.d_seq_c2b_contract_c2b_ctob_model_price_seq) as d_seq_c2b_contract_c2b_ctob_model_price_seq
    ,concat_ws(',', d.d_seq_c2b_contract_c2b_ctob_diff_price_seq) as d_seq_c2b_contract_c2b_ctob_diff_price_seq
from samples a
left join user_click_feature b
on a.user_id = b.user_id
and a.ts = b.ts
left join user_bid_feature c
on a.user_id = c.user_id
and a.ts = c.ts
left join user_contract_feature d
on a.user_id = d.user_id
and a.ts = d.ts