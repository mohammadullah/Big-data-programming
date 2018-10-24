

from pyspark import SparkConf, SparkContext
import sys
import operator
import re, string

inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('wordcount improved')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+


def words_once(line):
    regex = re.compile(r'[%s\s]+' % re.escape(string.punctuation))  
    word = regex.sub(" ", line)                                      ## remove punctuation
    wordsep = word.lower()                                           ## to lower
    for w in wordsep.split():                                        ## split lines into words   
        yield (w, 1)

def get_key(kv):
    return kv[0]

def output_format(kv):
    k, v = kv
    return '%s %i' % (k, v)

text = sc.textFile(inputs)
words = text.flatMap(words_once)
words1 = words.filter(lambda x: x[0] != '')                          ## filter empty space 
wordcount = words1.reduceByKey(operator.add)                         ## sum

outdata = wordcount.sortBy(get_key).map(output_format)
outdata.saveAsTextFile(output)
