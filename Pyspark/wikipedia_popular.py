## Name: Mohammad Ullah
## Stuent id: 301369145

from pyspark import SparkConf, SparkContext
import sys
import operator
import re, string

inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('wikipedia popular')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+
maxval = 0


def words_once(line):
    line = list(line.split())
    line[3] = int(line[3])
    line = tuple(line) 
    return line

def get_key(kv):
    return kv[0]

def tab_separated(kv):
    return "%s\t%s" % (kv[0], kv[1])

def find_max(x,y):
    maxval = max(x[0], y[0])
    if (x[0] >= y[0]):
        page = x[1]
    else:
        page = y[1]
    
    return (maxval, page)


text = sc.textFile(inputs)
words = text.map(words_once)
words = words.filter(lambda x: x[1] == 'en')               ## Take only english entries
words = words.filter(lambda x: "Main_Page" not in x[2])    ## remove Main_page
words = words.filter(lambda x: "Special:" not in x[2])     ## remove Special
words = words.map(lambda x: (x[0], (x[3], x[2])))          ## make the (key, value) pair
words = words.reduceByKey(find_max)                        ## Find the max and reduce

outdata = words.sortBy(get_key).map(tab_separated)
outdata.saveAsTextFile(output)
