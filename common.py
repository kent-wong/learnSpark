from pyspark import SparkContext, SparkConf
import numpy as np

conf = SparkConf()
conf.set('master', 'spark://hadoop-maste:7077')
context = SparkContext(conf=conf)


