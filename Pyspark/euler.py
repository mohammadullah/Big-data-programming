

from pyspark import SparkConf, SparkContext
import sys
import operator, random
import re, string
assert sys.version_info >= (3, 5)  

def get_euler(s_values):                             ## Function for iteration 

    total_itr = 0
    random.seed()
    for i in range(int(s_values)):
        sum = 0.0
        while sum < 1:
            sum += random.random()
            total_itr += 1

    return total_itr


def main(inputs):                                    
    sample = sc.range(8, numSlices = 8)               ## Making RDD
    sample = sample.map(lambda x : inputs/8)          ## My laptop has 8 cores so I used 8

    itr = sample.map(get_euler) 
    itr1 = itr.reduce(operator.add)                   ## sum

    outdata = itr1/inputs
    print(outdata)


if __name__ == '__main__':
    conf = SparkConf().setAppName('wordcount improved')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'  
    inputs = int(sys.argv[1])
    main(inputs)

