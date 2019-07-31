# 基于pyspark，实现常用ML功能
## 代码结构

```buildoutcfg
├── data
|  ├── test.csv
|  └── word_count.txt
├── readme.MD
├── resultData
├── src
|  ├── dataset_split.py
|  ├── spark-warehouse
|  ├── word_count.py
```
## 各个文件夹说明
- src
    - dataset_split.py 分割数据集
    - read_excel.py spark读取excel文件
    - word_count.py 中英文单词计数，使用中文使用jieba分词
    
