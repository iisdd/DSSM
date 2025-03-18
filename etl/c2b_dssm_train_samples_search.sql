--生成今日秒杀场景<user_id, clue_id, label>三元组
--正样本: 出价人车对
--负样本: 每个recommend_id、page有30%的概率采一条负样本
--(按user_id活跃程度等比例采负样本,保证每个user_id都能有负样本,而不只是出过价的或者点击过的车商)
with valid_cars as ( -- 约89260
    select cast(clue_id as int) clue_id
        ,min(cast(ts as int)) on_sale_ts
    from (
        select clue_id
            ,ts
        from gzlc_real.fact_ctob_user_behavior  
        where dt = '${date_y_m_d}'
        and page_type like '%%SEARCH%%'
        and page_type not like '%%INVITATION%%'
        and clue_id > '0' and clue_id != '' and clue_id is not null
    ) t
    group by clue_id
)
,query_tb as ( -- recommend_id、page对,127942条(出价人车对的9倍)
    select user_id
        ,recommend_id
        ,page
        ,rand() as sample_prob
        ,min(cast(ts as int)) query_ts
    from gzlc_real.fact_ctob_user_behavior
    where dt = '${date_y_m_d}'
    and action = 'beseen'
    and page_type like '%%SEARCH%%'
    and page_type not like '%%INVITATION%%'
    and recommend_id != ''
    and user_id > '0' and user_id != '' and user_id is not null
    and clue_id > '0' and clue_id != '' and clue_id is not null
    group by user_id, recommend_id, page
)
,join_tb as (
    select user_id
        ,clue_id
        ,recommend_id
        ,cast(query_ts as int) as ts
        ,on_sale_ts
        ,sample_prob
        ,row_number() over (partition by a.recommend_id, a.page order by rand()) as rn
    from query_tb a
    left join valid_cars b
    on a.query_ts > b.on_sale_ts -- 已上架车源
)

,pos_tb as ( -- 正样本:出价人车对,13725条
    select cast(user_id as int) user_id
        ,cast(clue_id as int) clue_id
        ,recommend_id
        ,1 as label
        ,cast(ts as int) ts
    from gzlc_real.fact_ctob_user_behavior
    where dt = '${date_y_m_d}'
    and action = 'bid'
    and page_type like '%%SEARCH%%'
    and page_type not like '%%INVITATION%%'
    and recommend_id != ''
    and user_id > '0' and user_id != '' and user_id is not null
    and clue_id > '0' and clue_id != '' and clue_id is not null
    group by user_id,clue_id,recommend_id,ts
)
,neg_tb as (
    select cast(user_id as int) user_id
        ,cast(clue_id as int) clue_id
        ,recommend_id
        ,0 as label
        ,cast(ts as int) ts
    from join_tb
    where rn = 1 
    and sample_prob < 0.3-- 采样比例
)

,res_tb as (
    select *
    from pos_tb
    union all
    select *
    from neg_tb
)

insert overwrite table g3_feature_dev.c2b_dssm_train_samples partition(dt='${date_y_m_d}', scene = 'SEARCH', action='bid')
select *
from res_tb