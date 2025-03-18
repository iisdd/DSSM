-- 表分区：part('test'),page_type('SEARCH'),dt
with repeated_base_table as (
    select a.*
        ,model_price
    from (
        select cast(clue_id as int) clue_id
            ,cast(user_id as int) user_id
            ,recommend_id
            ,cast(amount as int) amount
            ,cast(ts as int) ts
            ,action
            ,dt
            ,row_number() over(partition by clue_id, user_id, recommend_id, action order by ts asc) rn -- 每个action在一个recommend_id下只留ts最小的一条
        from gzlc_real.fact_ctob_user_behavior
        where dt = '${date_y_m_d}'
        and page_type like '%SEARCH%' -- 今日秒杀
        and page_type not like '%INVITATION%'
        and user_id != ''
        and user_id != '0'
        and user_id != 'unknown'
        and recommend_id!= ''
        and clue_id != ''
        and action in ('beseen','click','quick_collection','bid')
    ) a
    left join (
        select cast(clue_id as int) clue_id
              ,model_price
        from (
            select clue_id
                  ,ts
                  ,cast(model_price as int) as model_price
                  ,row_number() OVER (PARTITION BY clue_id ORDER BY ts desc) as rn
            from gzlc_real.fact_ctob_car_level
            where dt >= date_add('${date_y_m_d}', -90) and dt <= '${date_y_m_d}'
        ) model_price_tb
        where rn = 1
    ) b
    on a.clue_id = b.clue_id
)
,behavior_table as (
    select clue_id
          ,user_id
          ,recommend_id
          ,amount
          ,model_price
          ,ts
          ,action
          ,dt
    from repeated_base_table
    where rn = 1
)
,beseen_table as (
    select clue_id
        ,user_id
        ,recommend_id
        ,ts
    from behavior_table
    where action = 'beseen'
)
,click_table as (
    select clue_id
        ,recommend_id
        ,1 is_click
    from behavior_table
    where action = 'click'
)
,quick_collection_table as (
    select clue_id
        ,recommend_id
        ,1 is_quick_collection
    from behavior_table
    where action = 'quick_collection'
)
,bid_table as (
    select clue_id
        ,recommend_id
        ,1 is_bid
    from behavior_table
    where action = 'bid'
)
,daojia80_table as (
    select clue_id
        ,recommend_id
        ,1 is_daojia80
    from behavior_table
    where action = 'bid'
    and model_price > 0
    and amount > model_price*0.8
)
,daojia90_table as (
    select clue_id
        ,recommend_id
        ,1 is_daojia90
    from behavior_table
    where action = 'bid'
    and model_price > 0
    and amount > model_price*0.9
)
,daojia100_table as (
    select clue_id
        ,recommend_id
        ,1 is_daojia100
    from behavior_table
    where action = 'bid'
    and model_price > 0
    and amount > model_price
)
,order_table as ( -- 三天内下单
    select clue_id
        ,user_id
        ,1 as is_order
    from guazi_dw_dw.dw_ctob_appoint_prepay_detail_ymd
    where dt = date_add('${date_y_m_d}', 3)
    and substr(create_time, 1, 10) between '${date_y_m_d}' and date_add('${date_y_m_d}', 3)
    group by clue_id,user_id
)
,res_table as ( -- join只看recommend_id不看时间,时间上报有误差,可能beseen_ts会比click_ts大
    select a.*
        ,coalesce(is_click, 0) as is_click
        ,coalesce(is_quick_collection, 0) as is_quick_collection
        ,coalesce(is_bid, 0) as is_bid
        ,coalesce(is_daojia80, 0) as is_daojia80
        ,coalesce(is_daojia90, 0) as is_daojia90
        ,coalesce(is_daojia100, 0) as is_daojia100
        ,coalesce(is_order, 0) as is_order
    from beseen_table a
    left join click_table b
    on a.clue_id = b.clue_id and a.recommend_id = b.recommend_id
    left join quick_collection_table c
    on a.clue_id = c.clue_id and a.recommend_id = c.recommend_id
    left join bid_table d
    on a.clue_id = d.clue_id and a.recommend_id = d.recommend_id    
    left join daojia80_table e
    on a.clue_id = e.clue_id and a.recommend_id = e.recommend_id    
    left join daojia90_table f
    on a.clue_id = f.clue_id and a.recommend_id = f.recommend_id
    left join daojia100_table g
    on a.clue_id = g.clue_id and a.recommend_id = g.recommend_id
    left join order_table h
    on a.clue_id = h.clue_id and a.user_id = h.user_id    
)

insert overwrite table g3_feature_dev.c2b_mtl_sample_v1 partition(part = 'test', page_type = 'SEARCH', dt = '${date_y_m_d}')
select *
from res_table