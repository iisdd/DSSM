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

# 定义样本日期范围
start_date = '2024-10-27'
last_date = '2024-10-27'
date_range = pd.date_range(start=start_date, end=last_date)
input_dir = '/home/jupyterhub/daiyuxuan/dssm_samples_search/data_new_fea/'
dataframes = []

# 记录开始时间
start_time = time.time()

for single_date in date_range:
    date_str = single_date.strftime('%Y-%m-%d')
    file_path = os.path.join(input_dir, f'samples_data_{date_str}.csv')
    
    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
        dataframes.append(df)
        print(f"Loaded data for {date_str}")

# 记录加载数据时间
load_data_time = time.time()
print(f"Data loading time: {load_data_time - start_time:.2f} seconds")

samples_data = pd.concat(dataframes, ignore_index=True)
print("All data has been loaded and concatenated.")

# random_seed = 42  # 你可以根据需要更改种子值
# samples_data = samples_data.sample(n=10000, random_state=random_seed)

# print("Sampled 10,000 data points.")


# 将seq特征空字符串替换为 NaN
samples_data.replace('', np.nan, inplace=True)

fea_list = list(samples_data.columns)[5:-1]
sparse_user_feature = [i for i in fea_list if i.startswith('s_u')] + ["user_id"]
dense_user_feature = [i for i in fea_list if i.startswith('d_u')]
sparse_item_feature = [i for i in fea_list if i.startswith('s_i')] + ["clue_id"]
dense_item_feature = [i for i in fea_list if i.startswith('d_i')]
sparse_sequence_features = [i for i in fea_list if i.startswith('s_seq')]
dense_sequence_features = [i for i in fea_list if i.startswith('d_seq')]
sparse_fea = sparse_user_feature + sparse_item_feature
dense_fea = dense_user_feature + dense_item_feature
seq_fea = sparse_sequence_features + dense_sequence_features
user_fea = sparse_user_feature + dense_user_feature + sparse_sequence_features + dense_sequence_features
# user_fea = sparse_user_feature + dense_user_feature 

item_fea = sparse_item_feature + dense_item_feature
total_fea = user_fea + item_fea  # 注意按顺序
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

# 自动被读成float了
samples_data[sparse_fea] = samples_data[sparse_fea].astype('object')

# 加载模型
model_epoch = 'epoch2'
model_date = '2024-11-25'

dssm_model = load_model(f'/home/jupyterhub/daiyuxuan/dssm_models_search/model_file/{model_date}/dssm_model_{model_epoch}_{model_date}')
user_layer_model = load_model(f'/home/jupyterhub/daiyuxuan/dssm_models_search/model_file/{model_date}/user_layer_model_{model_epoch}_{model_date}')
item_layer_model = load_model(f'/home/jupyterhub/daiyuxuan/dssm_models_search/model_file/{model_date}/item_layer_model_{model_epoch}_{model_date}')

# 记录模型加载时间
load_model_time = time.time()
print(f"Model loading time: {load_model_time - load_data_time:.2f} seconds")


# print(samples_data.columns)

# 记录开始时间
start_preprocessing_time = time.time()

# 预处理
# 加载保存的中位数和scaler
dense_medians = joblib.load(f'/home/jupyterhub/daiyuxuan/dssm_models_search/model_file/{model_date}/dense_medians_{model_date}.pkl')
seq_medians = joblib.load(f'/home/jupyterhub/daiyuxuan/dssm_models_search/model_file/{model_date}/seq_medians_{model_date}.pkl')
dense_scaler = joblib.load(f'/home/jupyterhub/daiyuxuan/dssm_models_search/model_file/{model_date}/dense_scaler_{model_date}.pkl')
# 加载 dense 序列特征的 scaler
dense_seq_scalers = joblib.load(f'/home/jupyterhub/daiyuxuan/dssm_models_search/model_file/{model_date}/dense_seq_scalers_{model_date}.pkl')
# 记录加载中位数和scaler的时间
load_medians_scaler_time = time.time()
print(f"Loading medians and scaler time: {load_medians_scaler_time - start_preprocessing_time:.2f} seconds")


# 将dense序列特征字符串转换为数值列表
def convert_to_numeric_list(series):
    return series.apply(lambda x: list(map(float, x.split(','))) if isinstance(x, str) else x)

samples_data[dense_sequence_features] = samples_data[dense_sequence_features].apply(convert_to_numeric_list)
# 记录转换dense序列特征的时间
convert_dense_sequence_time = time.time()
print(f"Converting dense sequence features time: {convert_dense_sequence_time - load_medians_scaler_time:.2f} seconds")


def fill_dense_sequence_feature_optimized(series, median, length=10):
    """
    对于特征为null的:补成10个中位数
    对于特征不够10个的:在后面用当前列表的中位数补满10个
    """
    def fill(x):
        if not isinstance(x, list) or not x:  # null
            return [median] * length
        x_median = np.median(x)  # 计算x的中位数
        return x + [x_median] * (length - len(x))
    
    return series.apply(fill)

# 使用 fill_dense_sequence_feature_optimized 函数
for col in dense_sequence_features:
    samples_data[col] = fill_dense_sequence_feature_optimized(samples_data[col], seq_medians[col])

# 记录填补dense序列特征的时间
fill_dense_sequence_time = time.time()
print(f"Filling dense sequence features time: {fill_dense_sequence_time - convert_dense_sequence_time:.2f} seconds")

# 处理sparse序列特征
def convert_to_str_list(series):
    return series.apply(lambda x: list(map(str, x.split(','))) if isinstance(x, str) else x)
# 字符串转成列表
samples_data[sparse_sequence_features] = samples_data[sparse_sequence_features].apply(convert_to_str_list)

def fill_sparse_sequence_feature(x, fill_value='-1', length=10):
    """
    对于特征为null的:补成10个'-1'
    对于特征不够10个的:在后面用'-1'补满10个
    """
    if not isinstance(x, list) or not x:  # 如果是空的或不是列表
        return [fill_value] * length
    return x + [fill_value] * (length - len(x))

# 填补稀疏序列特征缺失值到长度为10
for col in sparse_sequence_features:
    samples_data[col] = samples_data[col].apply(lambda x: fill_sparse_sequence_feature(x))


# 记录转换sparse序列特征的时间
convert_sparse_sequence_time = time.time()
print(f"Converting sparse sequence features time: {convert_sparse_sequence_time - fill_dense_sequence_time:.2f} seconds")


# 列表内值映射: str -> int
lookup_table_path = '/home/jupyterhub/daiyuxuan/dssm_server_search/lookup.json'
# 读取 JSON 文件
with open(lookup_table_path, 'r') as file:
    mappings = json.load(file)

# 记录加载映射文件的时间
load_mappings_time = time.time()
print(f"Loading mappings time: {load_mappings_time - convert_sparse_sequence_time:.2f} seconds")
print(samples_data[sparse_sequence_features].head())

for col in sparse_sequence_features:
    attr = '_'.join(col.split('_')[4:-1])  # 提取属性名
    if attr not in mappings:
        print(f"Warning: Mapping file for {attr} not found.")
for col in sparse_fea:
    if col.startswith('s_u_'):
        attr = '_'.join(col.split('_')[3:-2])  # 提取属性名
    elif col.startswith('s_i_'):
        attr = '_'.join(col.split('_')[2:])
    else:
        attr = col
    if attr not in mappings:
        print(f"Warning: Mapping file for {attr} not found.") 

# 定义一个函数来处理特征列
def process_feature(value_list, mapping, is_clue_id=False):
    # 确保 value_list 是一个列表
    if not isinstance(value_list, list):
        value_list = [value_list]
    # 映射值并对 clue_id 取余
    return [
        1 if pd.isna(value) else (int(value) % 20001 if is_clue_id else mapping.get(str(value), 1))
        for value in value_list
    ]

# 对每个特征列进行处理
for col in sparse_sequence_features:
    attr = '_'.join(col.split('_')[4:-1])
    is_clue_id = 'clue_id' in col
    if is_clue_id:
        samples_data[col] = samples_data[col].apply(lambda x: process_feature(x, {}, is_clue_id))
    elif attr in mappings:
        samples_data[col] = samples_data[col].apply(lambda x: process_feature(x, mappings[attr], is_clue_id))

# 记录处理sparse序列特征的时间
process_sparse_sequence_time = time.time()
print(f"Processing sparse sequence features time: {process_sparse_sequence_time - load_mappings_time:.2f} seconds")


# 记录开始时间
start_mapping_time = time.time()


# 定义矢量化的 map_values 函数
vectorized_map_values = np.vectorize(lambda x, mapping: mapping.get(str(x), 1))
for col in sparse_fea:
    if col.startswith('s_u_'):
        attr = '_'.join(col.split('_')[3:-2])  # 提取属性名
    elif col.startswith('s_i_'):
        attr = '_'.join(col.split('_')[2:])
    else:
        attr = col
    if attr not in mappings:
        print(f"Warning: Mapping file for {attr} not found.") 
    else:
        samples_data[col] = vectorized_map_values(samples_data[col], mappings[attr])


# 记录映射特征的时间
map_sparse_features_time = time.time()
print(f"Mapping sparse features time: {map_sparse_features_time - start_mapping_time:.2f} seconds")


for col in dense_fea:
    samples_data[col].fillna(dense_medians[col], inplace=True)
samples_data[sparse_fea] = samples_data[sparse_fea].fillna(1)

# 记录填充缺失值的时间
fill_missing_values_time = time.time()
print(f"Filling missing values time: {fill_missing_values_time - map_sparse_features_time:.2f} seconds")

# # 对 user_id 进行哈希处理
# samples_data.loc[:, 'user_id'] = samples_data['user_id'].apply(lambda x: hash(x) % 8001)
# # 对 clue_id 进行哈希处理
# samples_data.loc[:, 'clue_id'] = samples_data['clue_id'].apply(lambda x: hash(x) % 20001)
# 确保所有特征都是数值类型

# 记录开始时间
start_time = time.time()

samples_data.loc[:, sparse_fea] = samples_data[sparse_fea].astype('int32', copy=False)
samples_data.loc[:, dense_fea] = samples_data[dense_fea].astype('float32', copy=False)
samples_data.loc[:, 'label'] = samples_data['label'].astype('int32', copy=False)

# 记录特征类型转换时间
convert_feature_type_time = time.time()
print(f"Converting feature types time: {convert_feature_type_time - start_time:.2f} seconds")


# 归一化
dense_data = samples_data[dense_fea].values
dense_data_normalized = dense_scaler.transform(dense_data)
samples_data[dense_fea] = dense_data_normalized

# 对 dense 序列特征进行归一化
for col in dense_sequence_features:
    scaler = dense_seq_scalers[col]
    # 对每个序列特征的值进行归一化
    samples_data[col] = samples_data[col].apply(lambda x: scaler.transform(np.array(x).reshape(-1, 1)).flatten())

# 记录归一化时间
normalize_time = time.time()
print(f"Normalization time: {normalize_time - convert_feature_type_time:.2f} seconds")



# User塔
# 获取 user_id 和 user_embedding
samples_data["original_user_id"] = samples_data["user_id"]
user_id = samples_data["original_user_id"]
samples_data["user_id"] = samples_data["user_id"].apply(lambda x: hash(x) % 8001)  # 对user_id进行哈希处理
# user_input = [samples_data[fea].values.reshape(-1, 1).astype(np.float32) for fea in user_fea]
# 确保 user_input 的形状符合模型的期望
user_input = [np.array(samples_data[fea].tolist()) for fea in user_fea]
# user_input = []
# for fea in user_fea:
#     if fea in dense_sequence_features or fea in sparse_sequence_features:
#         # 对于序列特征，reshape 成 (-1, 10)
#         reshaped_input = np.array(samples_data[fea].tolist()).reshape(-1, 10)
#     else:
#         # 对于非序列特征，reshape 成 (-1, 1)
#         reshaped_input = np.array(samples_data[fea].tolist()).reshape(-1, 1)
#     user_input.append(reshaped_input)
# 使用 model.predict 获取 user_embedding
user_embedding = user_layer_model(user_input)
# 将 user_id 和 user_embedding 放到一个 DataFrame 中
user_embedding_list = [embedding.numpy().tolist() for embedding in user_embedding]
df_user_embedding = pd.DataFrame({"user_id": user_id.values, "user_embedding": user_embedding_list})
# 导出 DataFrame
df_user_embedding.to_csv("/home/jupyterhub/daiyuxuan/dssm_models_search/inference_data/user_embeddings.csv", index=False)
print(df_user_embedding.head())

# 记录用户塔处理时间
user_tower_time = time.time()
print(f"User tower processing time: {user_tower_time - normalize_time:.2f} seconds")


# Item塔
samples_data["original_clue_id"] = samples_data["clue_id"]
clue_id = samples_data["original_clue_id"]
recommend_id = samples_data["recommend_id"]
samples_data["clue_id"] = samples_data["clue_id"].apply(lambda x: hash(x) % 20001)  # 对clue_id进行哈希处理
item_input = [np.array(samples_data[fea].tolist()).reshape(-1, 1) for fea in item_fea]
# item_input = [samples_data[fea].values.reshape(-1, 1).astype(np.float32) for fea in item_fea]


# # 确认 item_layer_model 的输入张量规格
# for input_tensor in item_layer_model.signatures['serving_default'].inputs:
#     print(input_tensor.name, input_tensor.shape)

# item_embedding = item_layer_model.predict(item_input)
item_embedding = item_layer_model(item_input)
item_embedding_list = [embedding.numpy().tolist() for embedding in item_embedding]
df_item_embedding = pd.DataFrame({"clue_id": clue_id.values, "recommend_id": recommend_id.values,
                                  "item_embedding": item_embedding_list})
# 导出 DataFrame
df_item_embedding.to_csv(f"/home/jupyterhub/daiyuxuan/dssm_models_search/inference_data/item_embeddings.csv", index=False)
print(df_item_embedding.head())

# 记录物品塔处理时间
item_tower_time = time.time()
print(f"Item tower processing time: {item_tower_time - user_tower_time:.2f} seconds")



# 加载用户和物品的嵌入向量
df_user_embedding = pd.read_csv(f"/home/jupyterhub/daiyuxuan/dssm_models_search/inference_data/user_embeddings.csv")
df_item_embedding = pd.read_csv(f"/home/jupyterhub/daiyuxuan/dssm_models_search/inference_data/item_embeddings.csv")

# 将嵌入向量从字符串转换为列表
df_user_embedding['user_embedding'] = df_user_embedding['user_embedding'].apply(eval)
df_item_embedding['item_embedding'] = df_item_embedding['item_embedding'].apply(eval)

# 计算内积
inner_products = []
for user_emb, item_emb in zip(df_user_embedding['user_embedding'], df_item_embedding['item_embedding']):
    inner_product = np.dot(user_emb, item_emb)
    inner_products.append(inner_product)

# 将内积结果添加到 DataFrame 中
df_inner_product = pd.DataFrame({
    'user_id': df_user_embedding['user_id'],
    'item_id': df_item_embedding['clue_id'],  # 假设 item_id 存储在 df_item_embedding 的 'user_id' 列
    'recommend_id': df_item_embedding['recommend_id'],
    'inner_product': inner_products
})

dssm_predictions = dssm_model(user_input + item_input)
dssm_predictions = [pred.numpy() for pred in dssm_predictions]
df_inner_product['dssm_prediction'] = dssm_predictions
# 导出 DataFrame
df_inner_product.to_csv(f"/home/jupyterhub/daiyuxuan/dssm_models_search/inference_data/inner_products.csv", index=False)
print(df_inner_product.head())
# 记录内积计算和预测时间
inner_product_time = time.time()
print(f"Inner product and prediction time: {inner_product_time - item_tower_time:.2f} seconds")