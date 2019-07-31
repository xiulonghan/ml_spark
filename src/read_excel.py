# -*- coding:utf-8 -*-
"""
# Author:aluka_han
# Email:aluka_han@163.com
# Datetime:2019/7/31
# Reference: None
# Description:SPARK2.3读取excel文件,并将文件结果保存
"""

# Standard library
from __future__ import print_function
import os

# Third-party libraries
from pyspark.sql import SparkSession


def read_excel_file(file_path):
    spark = SparkSession \
        .builder \
        .master('local') \
        .appName('read_excel') \
        .getOrCreate()
    student = spark.read.format("com.crealytics.spark.excel") \
        .option("location", file_path+'\\data\\student.xlsx')\
        .option("useHeader", "true")\
        .option("treatEmptyValuesAsNulls", "true")\
        .option("inferSchema", "False")\
        .option("addColorColumns", "False")\
        .load()
    print(student.show())
    # 保存文件
    # coalesce(1)保证保存后的文件是一个
    student.coalesce(1).write.csv(path=file_path+'\\resultData\\student',
                                  sep='\t', mode='overwrite', ignoreLeadingWhiteSpace=False,
                                  ignoreTrailingWhiteSpace=False)


if __name__ == '__main__':
    file_path = os.path.abspath('../')
    read_excel_file(file_path)

