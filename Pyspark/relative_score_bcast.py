

from pyspark import SparkConf, SparkContext
import sys
import json
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+


def raddit_kv(line):                                    ## Function to get key_value pair
    key = line['subreddit']
    value = int(line['score'])
    count = 1

    return (key, (value, count))


def add_pairs(x,y):                                    ## Function to sum value and count 
    sum_value = x[0] + y[0]
    sum_count = x[1] + y[1]

    return (sum_value, sum_count)

def get_key(kv):
    return kv[0]

def bcastfun(bcast, commentbysub):                    ## Function to join and get relative score
    
    join_rdd = commentbysub.map(lambda x: (x[0], (bcast.value.get(x[0]), x[1])))
    rscore = join_rdd.mapValues(lambda x: [x[1]['score']/x[0], x[1]['author']]).values()
    
    return rscore



def main(inputs, output):

    text = sc.textFile(inputs).map(json.loads).cache()             ## load and map json files 
    key_value = text.map(raddit_kv)                                ## map and function call to get key-value pairs
    reduced = key_value.reduceByKey(add_pairs)                     ## Reduce operation
    average = reduced.mapValues(lambda x: x[0]/x[1])               ## Get the average
    positive = average.filter(lambda x: x[1] > 0)                  ## get only positive avg
    avgcollect = positive.collectAsMap()                           
    bcast = sc.broadcast(avgcollect)                               ## broadcast average


    commentbysub = text.map(lambda x: (x['subreddit'], x))
    rscore = bcastfun(bcast, commentbysub)                        ## function to the rlative score

    outdata = rscore.sortBy(get_key, ascending = False).map(json.dumps)     ## Sort and dump files
    outdata.saveAsTextFile(output)

if __name__ == '__main__':
    conf = SparkConf().setAppName('relative score broadcast')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)




