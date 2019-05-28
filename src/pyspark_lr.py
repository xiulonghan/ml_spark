# -*- coding:utf-8 -*-
"""
Author: HanXiuLong
Email: aluka_hxl@gmail.com
Time: 2019-05-27
Reference: None
功能：使用pyspark实现基本的LR模型。
读取数据，分割数据，训练数据，预测数据
"""
from __future__ import print_function

import os
import random

import numpy as np
from pyspark.sql import SparkSession


def read_data(file_path, sep=',', app_name='spark_app',
              master='local', random_state=None,
              test_size=0.25):
    """
    读取数据文件,数据最后一列为标签列，仅限1和0,1为正样本，0位负样本
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
    pos_index = sample_rdd.filter(lambda x: int(x[0]['label']) == 1).map(lambda x: x[1])
    neg_index = sample_rdd.filter(lambda x: int(x[0]['label']) == 0).map(lambda x: x[1])
    # 筛选
    random.seed(random_state)
    pos_test_index = random.sample(pos_index.collect(), int(pos_index.count()*test_size))
    print(pos_test_index)
    print(neg_index.count())
    # print(df.rdd.zipWithIndex().filter(lambda x: x[1] in index_list).map(lambda x: x[0]).collect())
    # my_rdd = df.rdd [36, 2, 27, 30, 56, 0, 13]
    # print(my_rdd.collect())


if __name__ == "__main__":
    path = os.path.abspath('..')
    file_path = path+'//data//test.csv'
    read_data(file_path, sep='\t')

