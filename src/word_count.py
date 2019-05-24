# -*- coding:utf-8 -*-
"""
Author: HanXiuLong
Email: aluka_hxl@gmail.com
Time: 2019-05-24
Reference: https://github.com/fxsjy/jieba
功能：实现中英文文本单词计数，应为直接计算，中文使用jieba分词以后计算。
"""

from __future__ import print_function

import os
import re
from operator import add

import jieba
from pyspark.sql import SparkSession


class WordCount(object):
    """
    计算中英文文章中每个词语或者单词出现的次数
    """
    def __init__(self, doc, app_name='spark_app', master='local', isEnglish=True):
        """
        传入的spark参数及文本类型
        :param app_name: application name
        :param master: spark master
        :param isEnglish: type of doc
        """
        self.doc = doc
        self.isEnglish = isEnglish
        self.app_name = app_name
        self.master = master

    def get_word_count(self):
        """
        计算文章中的每个词语出现的个数
        :return: 单词字典，每个单词出现的次数，降序
        """
        spark = SparkSession\
                .builder\
                .appName(self.app_name)\
                .master(self.master)\
                .getOrCreate()
        if self.isEnglish:
            lines = spark.read.text(self.doc).rdd.map(lambda x: x[0])
            counts = lines.flatMap(lambda x: x.split(' ')).map(lambda x: (x, 1)).reduceByKey(add)
            return sorted(counts.collect(), key=lambda x: x[1], reverse=True)
        else:
            lines = spark.read.text(self.doc).rdd.map(lambda x: x[0])
            # 去除各种异常标点
            lines = lines.map(lambda x: re.sub("[\s+\.\!\/_,$%^*(+\"\']+|[+——！，。？、~@#”“￥：%……&*（）]+",
                                               "", x)).map(lambda x: ' '.join(jieba.cut(x)))
            counts = lines.flatMap(lambda x: x.split(' ')).map(lambda x: (x, 1)).reduceByKey(add)
            return sorted(counts.collect(), key=lambda x: x[1], reverse=True)


if __name__ == '__main__':
    root_path = os.path.abspath('../')
    doc = root_path+'//data//word_count.txt'
    app_name = 'word_count'
    master = 'local'
    wc = WordCount(doc, app_name=app_name, master=master, isEnglish=False)
    print(wc.get_word_count())





