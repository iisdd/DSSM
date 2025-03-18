insert overwrite table g3_feature_dev.c2b_dssm_train_samples partition(dt='2024-12-15', scene = 'SEARCH', action='bid')
select cast(user_id as int) user_id
	,cast(clue_id as int) clue_id
	,recommend_id
	,is_bid as label
	,cast(ts as int) ts
from g3_feature_dev.c2b_mtl_sample_v1  
where part='test' 
and page_type='SEARCH' 
and dt = '2024-12-15'