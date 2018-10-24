

from pyspark import SparkConf, SparkContext
import sys
import json
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+


def raddit_kv(line):                                            ## Function to get required values
    subreddit = line['subreddit']
    score = int(line['score'])
    author = line['author']

    return [subreddit, score, author]

def main(inputs, output):                                       ## Main function

    text = sc.textFile(inputs).map(json.loads)                     ## load and map json files 
    j_string = text.map(raddit_kv).cache()                         ## map and cache
    j_string = j_string.filter(lambda x: 'e' in x[0])              ## subreddit that contains 'e'
    positive = j_string.filter(lambda x: x[1] > 0)                 ## positive score 
    negative = j_string.filter(lambda x: x[1] <= 0)                ## zero or negative score 
  

    positive.map(json.dumps).saveAsTextFile(output+ '/positive')   ## positive output
    negative.map(json.dumps).saveAsTextFile(output+ '/negative')   ## negative output           
    
if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit etl')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)




