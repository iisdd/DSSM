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
import math

def cal_gauc(labels, preds, user_id_list):
    labels = labels.tolist()
    preds = preds.tolist()
    user_id_list = user_id_list.tolist()
    if len(user_id_list) != len(labels):
        raise ValueError(
            "impression id num should equal to the sample num,impression id num is {0},but sample num is {1}".format(
                len(user_id_list), len(labels)))
    group_score = defaultdict(lambda: [])
    group_truth = defaultdict(lambda: [])
    for idx, truth in enumerate(labels):
        user_id = user_id_list[idx]
        score = preds[idx]
        truth = labels[idx]
        group_score[user_id].append(score)
        group_truth[user_id].append(truth)
    group_flag = defaultdict(lambda: False)
    for user_id in set(user_id_list):
        truths = group_truth[user_id]
        flag = False
        for i in range(len(truths) - 1):
            if truths[i] != truths[i + 1]:
                flag = True
                break
        group_flag[user_id] = flag
    impression_total = 0
    total_auc = 0
    for user_id in group_flag:
        if group_flag[user_id]:
            auc = roc_auc_score(np.asarray(group_truth[user_id]), np.asarray(group_score[user_id]))
            total_auc += auc * len(group_truth[user_id])
            impression_total += len(group_truth[user_id])
    group_auc = float(total_auc) / impression_total
    group_auc = round(group_auc, 4)
    return group_auc

# 定义样本日期范围
start_date = '2024-09-01'
last_date = '2024-11-30'
# 模型保存路径
today_date = datetime.now().strftime("%Y-%m-%d")
model_save_path = f'/home/jupyterhub/daiyuxuan/dssm_models_search/model_file/{today_date}_allfea/'
if not os.path.exists(model_save_path):
    os.makedirs(model_save_path)
log_save_path = f'/home/jupyterhub/daiyuxuan/dssm_outputs_search/training_log_{today_date}.csv'

############################################################### 加载样本 ###############################################################
date_range = pd.date_range(start=start_date, end=last_date)
input_dir = '/home/jupyterhub/daiyuxuan/dssm_samples_search/data_processed/'
dataframes = []
for single_date in date_range:
    date_str = single_date.strftime('%Y-%m-%d')
    file_path = os.path.join(input_dir, f'samples_data_{date_str}.csv')
    
    # 跳过10-27的数据
    if date_str == '2024-10-27':
        continue
    
    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
        dataframes.append(df)
        print(f"Loaded data for {date_str}")
samples_data = pd.concat(dataframes, ignore_index=True)

print("All data has been loaded and concatenated.")

############################################################### 加载样本 ###############################################################
############################################################### 特征列表 ###############################################################
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
############################################################### 特征列表 ###############################################################
############################################################### 构建模型 ###############################################################
def dssm_model(samples_data):
    """函数式API搭建DSSM双塔DNN模型"""
    vocab_size = {
        'air_displacement': 72 + 1,
        'auto_type': 25 + 1,
        'c2b_ctob_car_level': 10 + 1,
        'c2b_evaluate_level': 5 + 1,
        'c2b_evaluate_level_segment': 5 + 1,
        'c2b_evaluate_score_segment': 5 + 1,
        'car_color': 15 + 1,
        'car_level': 10 + 1,
        'car_year': 26 + 1,
        'city_id': 302 + 1,
        'emission_standard': 8 + 1,
        'evaluate_level': 5 + 1,
        'fuel_type': 19 + 1,
        'gearbox': 4 + 1,
        'guobie': 8 + 1,
        'minor_category_id': 381 + 1,
        'model_price_bin': 8 + 1,
        'seats': 23 + 1,
        'tag_id': 3913 + 1,
        'user_id': 8001 + 1,
        'weekday': 7 + 1,
        'hour': 24 + 1,
    }
    # 调参点1：emb维度
    emb_dims = {key: int(math.log2(v)) + 1 for key, v in vocab_size.items()}
    s_u_Inputs = [keras.layers.Input(shape=(1,), name=fea) for fea in sparse_user_feature]
    d_u_Inputs = [keras.layers.Input(shape=(1,), name=fea) for fea in dense_user_feature]
    s_i_Inputs = [keras.layers.Input(shape=(1,), name=fea) for fea in sparse_item_feature]
    d_i_Inputs = [keras.layers.Input(shape=(1,), name=fea) for fea in dense_item_feature]
    s_seq_Inputs = [keras.layers.Input(shape=(10,), name=fea) for fea in sparse_sequence_features]
    d_seq_Inputs = [keras.layers.Input(shape=(10,), name=fea) for fea in dense_sequence_features]
    # user塔
    s_u_emb = []
    for i, fea in enumerate(sparse_user_feature):
        if fea in ["user_id"]:
            s_u_emb.append(layers.Embedding(input_dim=vocab_size[fea], output_dim=emb_dims[fea])(s_u_Inputs[i]))
        else:
            attr = '_'.join(fea.split('_')[3:-2])
            s_u_emb.append(layers.Embedding(input_dim=vocab_size[attr], output_dim=emb_dims[attr])(s_u_Inputs[i]))
    s_u_emb_flatten = [Flatten()(s) for s in s_u_emb]  # 3维变2维
    d_u_combined = tf.keras.layers.concatenate(d_u_Inputs)
    print('d_u_combined:', d_u_combined.shape)
    d_u_output = layers.Dense(64, activation='relu')(d_u_combined)
    # 调参点2：稀疏序列特征要不要接全连接
    # 稀疏序列特征的embedding并取平均
    s_seq_emb = []
    total_shape_sum = 0  # 初始化总和变量
    for i, fea in enumerate(sparse_sequence_features):
        attr = '_'.join(fea.split('_')[4:-1])  # 修改为从第5个元素开始
        emb = layers.Embedding(input_dim=vocab_size[attr], output_dim=emb_dims[attr])(s_seq_Inputs[i])
        emb_mean = tf.reduce_mean(emb, axis=1)  # 对embedding取平均
        s_seq_emb.append(emb_mean)
        total_shape_sum += emb_mean.shape[1]
    print('s_seq_emb: ', total_shape_sum)
    # 密集序列特征直接拼接
    d_seq_combined = tf.keras.layers.concatenate(d_seq_Inputs)
    d_seq_combined = Flatten()(d_seq_combined)  # 添加这一行
    print('d_seq_combined: ', d_seq_combined.shape)
    d_seq_output = layers.Dense(64, activation='relu')(d_seq_combined)
    
    # 检查每个特征的形状
    print(d_u_output.shape)  # 打印密集用户特征的输出形状
    print('稀疏特征:')
    # print('d_seq_output', d_seq_output)
    print(d_seq_output.shape)  # 打印密集序列特征的输出形状

    # # 确保拼接的特征形状是正确的
    # user_vector = tf.keras.layers.concatenate(
    #     s_u_emb_flatten + [d_u_output] + s_seq_emb + [d_seq_output],
    # )
    # 调参点3：用户用多少特征
    user_vector = tf.keras.layers.concatenate(
        s_u_emb_flatten + [d_u_output] + s_seq_emb + [d_seq_output],
    )

    # 检查拼接后的形状
    print('user输入:', user_vector.shape)
    
    user_vector = layers.Dense(256, activation='relu')(user_vector)
    user_vector = layers.Dense(128, activation='relu')(user_vector)
    # 最后一层不要激活函数
    user_vector = layers.Dense(32, name="user_embedding")(user_vector)

    # item塔
    s_i_emb = []
    for i, fea in enumerate(sparse_item_feature):
        if fea == "clue_id":
            s_i_emb.append(layers.Embedding(input_dim=vocab_size['clue_id'], output_dim=emb_dims['clue_id'])(s_i_Inputs[i]))
        else:
            attr = '_'.join(fea.split('_')[2:])
            s_i_emb.append(layers.Embedding(input_dim=vocab_size[attr], output_dim=emb_dims[attr])(s_i_Inputs[i]))
    s_i_emb_flatten = [Flatten()(s) for s in s_i_emb]
    d_i_combined = tf.keras.layers.concatenate(d_i_Inputs)
    d_i_output = layers.Dense(64, activation='relu')(d_i_combined)
    item_vector = tf.keras.layers.concatenate(
        s_i_emb_flatten + [d_i_output],
    )
    print('item输入:', item_vector.shape)
    item_vector = layers.Dense(256, activation='relu')(item_vector)
    item_vector = layers.Dense(128, activation='relu')(item_vector)
    # 最后一层不要激活函数
    item_vector = layers.Dense(32, name="item_embedding")(item_vector)

    # # 计算每个用户的embedding向量和item的embedding向量的cosine余弦相似度
    # user_vector = tf.math.l2_normalize(user_vector, axis=1)
    # item_vector = tf.math.l2_normalize(item_vector, axis=1)

    # 不要Normalize
    dot_user_item = tf.reduce_sum(user_vector * item_vector, axis=1)
    dot_user_item = tf.expand_dims(dot_user_item, 1)
    print(dot_user_item.shape)
    output = layers.Activation('sigmoid')(dot_user_item)

    return keras.models.Model(inputs=s_u_Inputs + d_u_Inputs + s_seq_Inputs + d_seq_Inputs 
                                        + s_i_Inputs + d_i_Inputs, outputs=output), \
        keras.models.Model(inputs=s_u_Inputs + d_u_Inputs +
                           s_seq_Inputs + d_seq_Inputs, outputs=user_vector), \
        keras.models.Model(inputs=s_i_Inputs + d_i_Inputs, outputs=item_vector)
model, user_layer_model, item_layer_model = dssm_model(samples_data)

class CustomModelCheckpoint(Callback):
    def __init__(self, model, user_layer_model, item_layer_model, filepath, monitor='val_loss', verbose=0,
                 save_best_only=False, mode='auto'):
        super(CustomModelCheckpoint, self).__init__()
        self.model = model
        self.user_layer_model = user_layer_model
        self.item_layer_model = item_layer_model
        self.filepath = filepath
        self.monitor = monitor
        self.verbose = verbose
        self.save_best_only = save_best_only
        self.mode = mode
        self.best = None

    def on_epoch_end(self, epoch, logs=None):
        logs = logs or {}
        current = logs.get(self.monitor)
        if current is None:
            if self.verbose > 0:
                print(f'\nEpoch {epoch + 1}: {self.monitor} is not available in logs.')
            return

        current = round(float(current), 4)
        if self.best is None or self._is_improvement(current, self.best):
            self.best = current
            if self.verbose > 0:
                print(
                    f'\nEpoch {epoch + 1}: {self.monitor} improved to {current}, saving model weights to {self.filepath}')
            # 添加 epoch 编号到文件名
            timestamp = datetime.now().strftime("%Y-%m-%d")
            model_save_dir = f"{os.path.splitext(self.filepath)[0]}dssm_model_epoch{epoch + 1}_{timestamp}"
            user_layer_save_dir = f"{os.path.splitext(self.filepath)[0]}user_layer_model_epoch{epoch + 1}_{timestamp}"
            item_layer_save_dir = f"{os.path.splitext(self.filepath)[0]}item_layer_model_epoch{epoch + 1}_{timestamp}"
            # 保存模型为 .pb 文件
            tf.keras.models.save_model(self.model, model_save_dir, save_format='tf')
            tf.keras.models.save_model(self.user_layer_model, user_layer_save_dir, save_format='tf')
            tf.keras.models.save_model(self.item_layer_model, item_layer_save_dir, save_format='tf')
        else:
            if self.verbose > 0:
                print(f'\nEpoch {epoch + 1}: {self.monitor} did not improve from {self.best}')

    def _is_improvement(self, current, best):
        if self.mode == 'min':
            return current < best
        else:
            return current > best

# 创建回调函数
checkpoint_callback = CustomModelCheckpoint(
    model=model,
    user_layer_model=user_layer_model,
    item_layer_model=item_layer_model,
    filepath=model_save_path,
    monitor='val_auc',
    save_best_only=True,
    mode='max',
    verbose=1
)

class PrintGAUC(tf.keras.callbacks.Callback):
    def __init__(self, validation_data, user_id_list):
        super().__init__()
        self.validation_data = validation_data
        self.user_id_list = user_id_list

    def on_epoch_end(self, epoch, logs=None):
        y_pred = self.model.predict(self.validation_data[0])
        y_true = self.validation_data[1]
        user_id_list = self.user_id_list

        # 将 y_true 转换为一维张量
        y_true = tf.reshape(y_true, [-1])
        # 将 y_pred 转换为一维张量
        y_pred = tf.reshape(y_pred, [-1])
        # 将 user_id_list 转换为一维张量
        user_id_list = tf.reshape(user_id_list, [-1])

        unique_classes = tf.unique(y_true)[0]
        if tf.size(unique_classes) < 2:
            gauc = 0.5  # 如果只有一个类别，返回0.5作为默认值
        else:
            gauc = cal_gauc(y_true.numpy(), y_pred.numpy(), user_id_list.numpy())
        logs['val_gauc'] = gauc  # 将GAUC添加到logs中
        print(f'Epoch {epoch + 1}, GAUC: {gauc}')

csv_logger = CSVLogger(log_save_path, append=True)
# 模型编译
model.compile(
    optimizer=tf.keras.optimizers.Adam(learning_rate=0.003),
    loss='binary_crossentropy',  # 假设你的任务是二分类问题
    metrics=['accuracy', tf.keras.metrics.AUC()]  # 假设你想要跟踪准确率
)
############################################################### 构建模型 ###############################################################

# # 打印模型结构
# model.summary()

# 预处理：1.填补缺失值
# 确保所有特征都是数值类型
samples_data.loc[:, sparse_fea] = samples_data[sparse_fea].astype('int32', copy=False)
samples_data.loc[:, dense_fea] = samples_data[dense_fea].astype('float32', copy=False)
samples_data.loc[:, 'label'] = samples_data['label'].astype('int32', copy=False)
def convert_from_json(series):
    return series.apply(lambda x: json.loads(x) if isinstance(x, str) else x)
samples_data[sparse_sequence_features] = samples_data[sparse_sequence_features].apply(convert_from_json)
samples_data[dense_sequence_features] = samples_data[dense_sequence_features].apply(convert_from_json)


# 按label_ts划分数据集，比例：[0.8, 0.1, 0.1]
samples_data = samples_data.sort_values(by='ts')
total_size = len(samples_data)
split1 = int(total_size * 0.8)  # 80% 的数据作为第一份
split2 = int(total_size * 0.9)  # 90% 的数据作为前两份
# 确保在切片后创建副本
train_data = samples_data.iloc[:split1].copy()
valid_data = samples_data.iloc[split1:split2].copy()
test_data = samples_data.iloc[split2:].copy()

# 将每个特征的数据转换为 NumPy 数组
x_train = [np.array(train_data[fea].tolist()) for fea in total_fea]
x_valid = [np.array(valid_data[fea].tolist()) for fea in total_fea]
x_test = [np.array(test_data[fea].tolist()) for fea in total_fea]
y_train = train_data['label']
y_valid = valid_data['label']
y_test = test_data['label']


print('数据集条数:{}, 训练集:{}, 验证集:{}, 测试集:{}'.format(samples_data.count()[0], train_data.count()[0], 
                       valid_data.count()[0], test_data.count()[0]))
label_0_count = samples_data[samples_data['label'] == 0].shape[0]
print(f"Label为0的数量: {label_0_count}")

# 打印samples_data中label为1的数量
label_1_count = samples_data[samples_data['label'] == 1].shape[0]
print(f"Label为1的数量: {label_1_count}")

memory_usage = samples_data.memory_usage(deep=True).sum() / 1024**2  # 转换为MB
print(f"samples_data占用内存大小: {memory_usage:.2f} MB")

# 模型训练
history = model.fit(
    x=x_train,
    y=y_train,
    batch_size=1024,
    epochs=5,
    verbose=1,
    validation_data=(x_valid, y_valid),
    callbacks=[PrintGAUC((x_valid, y_valid), valid_data['recommend_id']), checkpoint_callback, csv_logger]
)

# 评估模型在测试集上的表现
import os
import re

model_dirs = [d for d in os.listdir(model_save_path) if re.match(f'dssm_model_epoch\\d+_{today_date}', d)]
if model_dirs:
    best_model_dir = max(model_dirs, key=lambda x: int(re.search(r'epoch(\d+)', x).group(1)))
    best_model_path = os.path.join(model_save_path, best_model_dir)
else:
    raise FileNotFoundError(f"No model directories found for date {today_date}")
best_model = keras.models.load_model(best_model_path)

# 使用模型进行预测
y_pred = best_model.predict(x_test)
test_data['pred'] = y_pred
auc = round(roc_auc_score(test_data['label'], test_data['pred']), 4)
gauc = cal_gauc(test_data['label'], test_data['pred'], test_data['recommend_id'])

# 计算测试集的准确率
test_data['pred_label'] = (test_data['pred'] > 0.5).astype(int)
test_acc_calculated = (test_data['pred_label'] == test_data['label']).mean()
print('计算的测试集acc:', round(test_acc_calculated, 3))

print('测试集acc: {}, auc: {}, gauc: {}'.format(round(test_acc_calculated, 3), auc, gauc))