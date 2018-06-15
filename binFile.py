from pyspark import SparkConf
from pyspark import SparkContext
import numpy as np

conf = SparkConf()
conf.set('master', 'spark://hadoop-maste:7077')
context = SparkContext(conf=conf)

rdd = context.binaryFiles('/data/pics')
print('applicationId:', context.applicationId)

result = rdd.collect()
for data in result:
    print(data[0], data[1][:10])

context.stop()
