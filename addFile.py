from pyspark import SparkContext
from pyspark import SparkConf
from pyspark import SparkFiles
import os
import numpy as np

# create a file
tempdir = '/root/workspace/'

#path = os.path.join(tempdir, 'num_data')
#with open(path, 'w') as f:
    #f.write('100')
path = 'hdfs://hadoop-maste:9000/data/num_data'

print(path)
conf = SparkConf()
conf.set('master', 'spark://hadoop-maste:7077')
context = SparkContext(conf=conf)

acc = context.accumulator(0)
acc2 = context.accumulator(0)
context.addFile(path)
rdd = context.parallelize(np.arange(10))

def func(iterable):
    acc.add(1)
    with open(SparkFiles.get('num_data')) as f:
        value = int(f.readline())
        #return [x*value for x in iterable]
        a = [x*value for x in iterable]
        if len(a) > 0:
            acc2.add(1)
        return [len(a)]

rdd2 = rdd.mapPartitions(func)
print(rdd2.collect())
print(acc)
print(acc2)

context.stop()
