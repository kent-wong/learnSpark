from pyspark import SparkContext, SparkConf
import numpy as np

conf = SparkConf()
conf.set('master', 'spark://hadoop-maste:7077')
context = SparkContext(conf=conf)

acc = context.accumulator(0)
print('initial value:', type(acc), acc.value)

rdd = context.parallelize(np.arange(101), 5)
def acc_add(x):
    acc.add(x)
    return x**2

rdd2 = rdd.map(acc_add)
print(rdd2.collect())
print('final value:', acc.value)

context.stop()
