

from pyspark import SparkConf, SparkContext
import sys
import json
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+


def raddit_kv(line):                                              ## Function to get key_value pair
    key = line['subreddit']
    value = int(line['score'])
    count = 1

    return (key, (value, count))

def add_pairs(x,y):                                               ## Function to sum value and count 
    sum_value = x[0] + y[0]
    sum_count = x[1] + y[1]

    return (sum_value, sum_count)

def get_key(kv):
    return kv[0]

def main(inputs, output):

    text = sc.textFile(inputs).map(json.loads)                     ## load and map json files 
    key_value = text.map(raddit_kv)                                ## map and function call to get key-value pairs
    reduced = key_value.reduceByKey(add_pairs)                     ## Reduce operation
    average = reduced.mapValues(lambda x: x[0]/x[1])               ## Get the average

    outdata = average.sortBy(get_key).map(json.dumps)              ## Sort and dump files
    outdata.saveAsTextFile(output)

if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit averages')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)




