# 计算线上样本auc/gauc
import pandas as pd
import numpy as np
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
from sklearn.model_selection import train_test_split
from tensorflow.keras.layers import Flatten
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import roc_auc_score
from collections import defaultdict
from tensorflow.keras.models import load_model

import joblib
import os
import json
import time


# 记录开始时间
start_time = time.time()

# 定义样本日期范围
start_date = '2024-12-15'
last_date = '2024-12-15'
model_epoch = 'epoch2'
model_date = '2024-12-06'
model_save_path = f'/home/jupyterhub/daiyuxuan/dssm_models_search/model_file/{model_date}_allfea'

############################################################### 加载样本 ###############################################################
date_range = pd.date_range(start=start_date, end=last_date)
input_dir = '/home/jupyterhub/daiyuxuan/dssm_samples_search/data_processed/'
dataframes = []
for single_date in date_range:
    date_str = single_date.strftime('%Y-%m-%d')
    file_path = os.path.join(input_dir, f'samples_data_{date_str}.csv')
    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
        dataframes.append(df)
        print(f"Loaded data for {date_str}")
samples_data = pd.concat(dataframes, ignore_index=True)

print("All data has been loaded and concatenated.")
############################################################### 加载样本 ###############################################################

############################################################### 加载模型 ###############################################################
# 记录加载数据时间
load_data_time = time.time()
print(f"Data loading time: {load_data_time - start_time:.2f} seconds")
# 加载模型
dssm_model = load_model(f'{model_save_path}/dssm_model_{model_epoch}_{model_date}')
user_layer_model = load_model(f'{model_save_path}/user_layer_model_{model_epoch}_{model_date}')
item_layer_model = load_model(f'{model_save_path}/item_layer_model_{model_epoch}_{model_date}')

# 记录模型加载时间
load_model_time = time.time()
print(f"Model loading time: {load_model_time - load_data_time:.2f} seconds")
############################################################### 加载模型 ###############################################################

fea_list = list(samples_data.columns)[5:-1]
sparse_user_feature = [i for i in fea_list if i.startswith('s_u')] + ["user_id"]
dense_user_feature = [i for i in fea_list if i.startswith('d_u')]
sparse_item_feature = [i for i in fea_list if i.startswith('s_i')]
dense_item_feature = [i for i in fea_list if i.startswith('d_i')]
sparse_sequence_features = [i for i in fea_list if i.startswith('s_seq') and 'clue_id' not in i]
dense_sequence_features = [i for i in fea_list if i.startswith('d_seq')]
sparse_fea = sparse_user_feature + sparse_item_feature
dense_fea = dense_user_feature + dense_item_feature
seq_fea = sparse_sequence_features + dense_sequence_features
user_fea = sparse_user_feature + dense_user_feature + sparse_sequence_features + dense_sequence_features
item_fea = sparse_item_feature + dense_item_feature
total_fea = sparse_user_feature + dense_user_feature + sparse_sequence_features + dense_sequence_features + sparse_item_feature + dense_item_feature  # 注意按顺序
# 计算并打印各类型特征的数量
print("Sparse User Features:", len(sparse_user_feature))
print("Dense User Features:", len(dense_user_feature))
print("Sparse Item Features:", len(sparse_item_feature))
print("Dense Item Features:", len(dense_item_feature))
print("Sparse Sequence Features:", len(sparse_sequence_features))
print("Dense Sequence Features:", len(dense_sequence_features))
print("User Features:", len(user_fea))
print("Item Features:", len(item_fea))
print("Total Features:", len(total_fea))

############################################################### 数据类型转换 ###############################################################
# 记录开始时间
start_time = time.time()

samples_data.loc[:, sparse_fea] = samples_data[sparse_fea].astype('int32', copy=False)
samples_data.loc[:, dense_fea] = samples_data[dense_fea].astype('float32', copy=False)
samples_data.loc[:, 'label'] = samples_data['label'].astype('int32', copy=False)
def convert_from_json(series):
    return series.apply(lambda x: json.loads(x) if isinstance(x, str) else x)
samples_data[sparse_sequence_features] = samples_data[sparse_sequence_features].apply(convert_from_json)
samples_data[dense_sequence_features] = samples_data[dense_sequence_features].apply(convert_from_json)

# 记录特征类型转换时间
convert_feature_type_time = time.time()
print(f"Converting feature types time: {convert_feature_type_time - start_time:.2f} seconds")
############################################################### 数据类型转换 ###############################################################


############################################################### 用户塔计算 ###############################################################
# User塔
# 获取 user_id 和 user_embedding
user_id = samples_data["original_user_id"]
recommend_id = samples_data["recommend_id"]
ts = samples_data["ts"]
user_input = [np.array(samples_data[fea].tolist()) for fea in user_fea]

user_embedding = user_layer_model(user_input)
# 将 user_id 和 user_embedding 放到一个 DataFrame 中
user_embedding_list = [embedding.numpy().tolist() for embedding in user_embedding]
df_user_embedding = pd.DataFrame({"user_id": user_id.values, "recommend_id": recommend_id.values, 
                                "ts": ts.values, "user_embedding": user_embedding_list})
# 导出 DataFrame
df_user_embedding.to_csv("/home/jupyterhub/daiyuxuan/dssm_models_search/inference_data/user_embeddings1215.csv", index=False)
print(df_user_embedding.head())

# 记录用户塔处理时间
user_tower_time = time.time()
print(f"User tower processing time: {user_tower_time - convert_feature_type_time:.2f} seconds")
############################################################### 用户塔计算 ###############################################################

############################################################### 物品塔计算 ###############################################################
# Item塔
clue_id = samples_data["clue_id"]
recommend_id = samples_data["recommend_id"]
item_input = [np.array(samples_data[fea].tolist()).reshape(-1, 1) for fea in item_fea]

item_embedding = item_layer_model(item_input)
item_embedding_list = [embedding.numpy().tolist() for embedding in item_embedding]
df_item_embedding = pd.DataFrame({"clue_id": clue_id.values, "recommend_id": recommend_id.values,
                                  "item_embedding": item_embedding_list})
# 导出 DataFrame
df_item_embedding.to_csv(f"/home/jupyterhub/daiyuxuan/dssm_models_search/inference_data/item_embeddings1215.csv", index=False)
print(df_item_embedding.head())

# 记录物品塔处理时间
item_tower_time = time.time()
print(f"Item tower processing time: {item_tower_time - user_tower_time:.2f} seconds")
############################################################### 物品塔计算 ###############################################################

# ############################################################### 交叉存储 ###############################################################
# # 加载用户和物品的嵌入向量
# df_user_embedding = pd.read_csv(f"/home/jupyterhub/daiyuxuan/dssm_models_search/inference_data/user_embeddings.csv")
# df_item_embedding = pd.read_csv(f"/home/jupyterhub/daiyuxuan/dssm_models_search/inference_data/item_embeddings.csv")

# # 将嵌入向量从字符串转换为列表
# df_user_embedding['user_embedding'] = df_user_embedding['user_embedding'].apply(eval)
# df_item_embedding['item_embedding'] = df_item_embedding['item_embedding'].apply(eval)

# # 计算内积
# inner_products = []
# for user_emb, item_emb in zip(df_user_embedding['user_embedding'], df_item_embedding['item_embedding']):
#     inner_product = np.dot(user_emb, item_emb)
#     inner_products.append(inner_product)

# # 将内积结果添加到 DataFrame 中
# df_inner_product = pd.DataFrame({
#     'user_id': df_user_embedding['user_id'],
#     'item_id': df_item_embedding['clue_id'],  # 假设 item_id 存储在 df_item_embedding 的 'user_id' 列
#     'recommend_id': df_item_embedding['recommend_id'],
#     'inner_product': inner_products
# })

# dssm_predictions = dssm_model(user_input + item_input)
# dssm_predictions = [pred.numpy() for pred in dssm_predictions]
# df_inner_product['dssm_prediction'] = dssm_predictions
# # 导出 DataFrame
# df_inner_product.to_csv(f"/home/jupyterhub/daiyuxuan/dssm_models_search/inference_data/inner_products1215.csv", index=False)
# print(df_inner_product.head())
# # 记录内积计算和预测时间
# inner_product_time = time.time()
# print(f"Inner product and prediction time: {inner_product_time - item_tower_time:.2f} seconds")
# ############################################################### 交叉存储 ###############################################################