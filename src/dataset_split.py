# -*- coding:utf-8 -*-
"""
Author: HanXiuLong
Email: aluka_hxl@gmail.com
Time: 2019-05-27
Reference: None
功能：使用pyspark实现基本的训练集和测试集分割。
读取数据并按照比例分割为训练和测试
"""
from __future__ import print_function

import os
import random

import numpy as np
from pyspark.sql import SparkSession


def read_data(file_path, sep=',', app_name='split_dataset',
              master='local', random_state=None,
              test_size=0.25):
    """
    读取数据文件,数据最后一列为标签列，名称为label，1为正样本，0位负样本
    :param file_path:文件路径
    :param sep:分隔符
    :return:
    """
    spark = SparkSession\
        .builder\
        .appName(app_name)\
        .master(master)\
        .getOrCreate()
    sample_df = spark.read.csv(file_path, sep=sep, header=True)
    # 正负样本随机切割
    # 基于随机数种子进行正负样本的切割
    # 转换为带index的rdd
    sample_rdd = sample_df.rdd.zipWithIndex()
    pos_index = sample_rdd.filter(
        lambda x: int(
            x[0]['label']) == 1).map(
        lambda x: x[1])
    neg_index = sample_rdd.filter(
        lambda x: int(
            x[0]['label']) == 0).map(
        lambda x: x[1])
    # 筛选
    random.seed(random_state)
    pos_test_index = random.sample(
        pos_index.collect(), int(
            pos_index.count() * test_size))
    pos_train_index = list(
        np.setdiff1d(
            np.array(
                pos_index.collect()),
            np.array(pos_test_index)))

    neg_test_index = random.sample(
        neg_index.collect(), int(
            neg_index.count() * test_size))
    neg_train_index = list(
        np.setdiff1d(
            np.array(
                neg_index.collect()),
            np.array(neg_test_index)))
    pos_test_index.extend(neg_test_index)
    pos_train_index.extend(neg_train_index)

    test_sample = sample_rdd.filter(
        lambda x: x[1] in pos_test_index).map(
        lambda x: x[0])
    train_sample = sample_rdd.filter(
        lambda x: x[1] in pos_train_index).map(
        lambda x: x[0])
    return test_sample, train_sample


if __name__ == "__main__":
    path = os.path.abspath('..')
    file_path = path + '//data//test.csv'
    test, train = read_data(file_path, sep='\t')
    print(test.take(10))
