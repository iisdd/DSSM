# 对样本预处理并保存
# 2024-12-05：取消对sparse类特征的填补缺失值&映射
import os
from datetime import datetime
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
from tensorflow.keras.callbacks import ModelCheckpoint, CSVLogger, Callback
import joblib
import glob
import re
import json
import time

# 定义样本日期范围
# start_date = '2024-09-01'
# last_date = '2024-11-30'
# mode = 'train' # test就加载train的median和scalar
start_date = '2024-12-15'
last_date = '2024-12-15'
mode = 'test' # test就加载train的median和scalar
date_range = pd.date_range(start=start_date, end=last_date)
input_dir = '/home/jupyterhub/daiyuxuan/dssm_samples_search/data_new_fea/'
output_dir = '/home/jupyterhub/daiyuxuan/dssm_samples_search/data_processed/'
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

dataframes = []
for single_date in date_range:
    date_str = single_date.strftime('%Y-%m-%d')
    file_path = os.path.join(input_dir, f'samples_data_{date_str}.csv')
    
    # 跳过10-27的数据
    if date_str == '2024-10-27' and mode == 'train':
        continue
    
    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
        dataframes.append(df)
        print(f"Loaded data for {date_str}")
samples_data = pd.concat(dataframes, ignore_index=True)

print("All data has been loaded and concatenated.")

# 将seq特征空字符串替换为 NaN
samples_data.replace('', np.nan, inplace=True)

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

# # 自动被读成float了
# samples_data[sparse_fea] = samples_data[sparse_fea].astype('object')

# 预处理：1.填补缺失值
# 将dense序列特征字符串转换为float列表
start_time = time.time()  # 开始计时

def convert_to_numeric_list(series):
    return series.apply(lambda x: list(map(float, x.split(','))) if isinstance(x, str) else x)

samples_data[dense_sequence_features] = samples_data[dense_sequence_features].apply(convert_to_numeric_list)

end_time = time.time()  # 结束计时
print(f"Converting dense sequence features to numeric lists took {end_time - start_time:.2f} seconds.")

# 将sparse序列特征字符串转换为int列表
start_time = time.time()  # 开始计时

def convert_to_str_list(series):
    return series.apply(lambda x: list(map(int, x.split(','))) if isinstance(x, str) else x)

samples_data[sparse_sequence_features] = samples_data[sparse_sequence_features].apply(convert_to_str_list)

end_time = time.time()  # 结束计时
print(f"Converting sparse sequence features to int lists took {end_time - start_time:.2f} seconds.")

# 在转换为 int32 之前，先填充缺失值
samples_data[sparse_fea] = samples_data[sparse_fea].fillna(1)

# 确保所有特征都是数值类型
samples_data.loc[:, sparse_fea] = samples_data[sparse_fea].astype('int32', copy=False)
samples_data.loc[:, dense_fea] = samples_data[dense_fea].astype('float32', copy=False)
samples_data.loc[:, 'label'] = samples_data['label'].astype('int32', copy=False)
# user_id取余
samples_data["original_user_id"] = samples_data["user_id"]
user_id = samples_data["original_user_id"]
samples_data["user_id"] = samples_data["user_id"].apply(lambda x: hash(x) % 8001)  # 对user_id进行哈希处理

if mode == 'train':
    seq_medians = {}
    for col in dense_sequence_features:
        all_values = [item for sublist in samples_data[col] if isinstance(sublist, list) for item in sublist]
        seq_medians[col] = np.median(all_values)

    def fill_dense_sequence_feature(x, median, length=10):
        """
            补中位数到长度为10
        """
        if not isinstance(x, list) or not x: # null
            return [median] * length
        return x + [median] * (length - len(x))
    # 填补dense序列特征缺失值到长度为10
    for col in dense_sequence_features:
        samples_data[col] = samples_data[col].apply(lambda x: fill_dense_sequence_feature(x, seq_medians[col]))

    dense_medians = samples_data[dense_fea].median()
    # dense补中位数
    for col in dense_fea:
        samples_data[col].fillna(dense_medians[col], inplace=True)

    dense_scaler = StandardScaler()
    dense_values = samples_data[dense_fea].values
    dense_values_normalized = dense_scaler.fit_transform(dense_values)
    samples_data[dense_fea] = dense_values_normalized

    dense_seq_scalers = {}
    for col in dense_sequence_features:
        all_values = np.concatenate(samples_data[col].values)
        scaler = StandardScaler()
        scaler.fit(all_values.reshape(-1, 1))
        dense_seq_scalers[col] = scaler
        samples_data[col] = samples_data[col].apply(lambda x: scaler.transform(np.array(x).reshape(-1, 1)).flatten())

    joblib.dump(seq_medians, f'{output_dir}seq_medians.pkl')
    joblib.dump(dense_medians, f'{output_dir}dense_medians.pkl')
    joblib.dump(dense_scaler, f'{output_dir}dense_scaler.pkl')
    joblib.dump(dense_seq_scalers, f'{output_dir}dense_seq_scalers.pkl')

elif mode == 'test':
    seq_medians = joblib.load(f'{output_dir}seq_medians.pkl')
    def fill_dense_sequence_feature(x, median, length=10):
        """
            补中位数到长度为10
        """
        if not isinstance(x, list) or not x: # null
            return [median] * length
        return x + [median] * (length - len(x))
    # 填补dense序列特征缺失值到长度为10
    for col in dense_sequence_features:
        samples_data[col] = samples_data[col].apply(lambda x: fill_dense_sequence_feature(x, seq_medians[col]))

    dense_medians = joblib.load(f'{output_dir}dense_medians.pkl')
    # dense补中位数
    for col in dense_fea:
        samples_data[col].fillna(dense_medians[col], inplace=True)

    dense_scaler = joblib.load(f'{output_dir}dense_scaler.pkl')
    # 对 dense 特征进行归一化
    dense_values = samples_data[dense_fea].values
    dense_values_normalized = dense_scaler.transform(dense_values)
    samples_data[dense_fea] = dense_values_normalized

    dense_seq_scalers = joblib.load(f'{output_dir}dense_seq_scalers.pkl')
    # 对 dense 序列特征进行归一化
    for col in dense_sequence_features:
        scaler = dense_seq_scalers[col]
        # 对每个序列特征的值进行归一化
        samples_data[col] = samples_data[col].apply(lambda x: scaler.transform(np.array(x).reshape(-1, 1)).flatten())

samples_data[sparse_sequence_features] = samples_data[sparse_sequence_features].apply(
    lambda x: ast.literal_eval(x) if isinstance(x, str) else x
)
samples_data[dense_sequence_features] = samples_data[dense_sequence_features].apply(
    lambda x: ast.literal_eval(x) if isinstance(x, str) else x
)
def convert_to_json(series):
    return series.apply(lambda x: json.dumps(x.tolist() if isinstance(x, np.ndarray) else x))

samples_data[sparse_sequence_features] = samples_data[sparse_sequence_features].apply(convert_to_json)
samples_data[dense_sequence_features] = samples_data[dense_sequence_features].apply(convert_to_json)

# 确保 'dt' 列存在于数据中
if 'dt' in samples_data.columns:
    # 按 'dt' 列分组并保存到不同的文件
    for dt_value, group in samples_data.groupby('dt'):
        output_file_path = os.path.join(output_dir, f'samples_data_{dt_value}.csv')
        group.to_csv(output_file_path, index=False)
        print(f"Saved data for {dt_value} to {output_file_path}")
else:
    print("Error: 'dt' column not found in samples_data.")