

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types, SQLContext, Row
import sys
import operator, math
import re, string
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

# Regular expression for log seperation
regex = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')

# Function to make tuples
def log_line(line):
     
    line_re = regex.split(line)
    line_re1 = ''.join(line_re[1:2])
    line_re2 = ''.join(line_re[4:5])

    yield (line_re1, line_re2)


def main(inputs):

    text = sc.textFile(inputs)
    line = text.flatMap(log_line)
    # Filter empty space
    line = line.filter(lambda x: x[1] != '') 
    # Make dataframe
    line = line.map(lambda x: Row(host = x[0], v_bytes = int(x[1])))
    df = sqlContext.createDataFrame(line)
    # add Count column with value 1
    df = df.withColumn('count', functions.lit(1))
    # groupby host and sum 
    df_sum = df.groupBy('host').agg({'v_bytes' : 'sum', 'count' : 'sum'})
    # calculate x^2, y^2, xy and n
    df_sum = df_sum.withColumn('n', functions.lit(1)).\
                   withColumn('x^2', df_sum['sum(count)']*df_sum['sum(count)']).\
                   withColumn('y^2', df_sum['sum(v_bytes)']*df_sum['sum(v_bytes)']).\
                   withColumn('xy', df_sum['sum(count)']*df_sum['sum(v_bytes)'])
    
    # Keep required columns
    df_new = df_sum.select('n', 'sum(count)', 'x^2', 'sum(v_bytes)', 'y^2', 'xy')
    # rename columns
    df_new = df_new.withColumnRenamed('sum(count)', 'x').\
                    withColumnRenamed('sum(v_bytes)', 'y') 
    # Sum all values   
    df_sum = df_new.groupBy().sum('n', 'x', 'x^2', 'y', 'y^2', 'xy')
    # extract from dataframe
    sum_n = df_sum.head()[0]
    sum_x = df_sum.head()[1]
    sum_xx = df_sum.head()[2]
    sum_y = df_sum.head()[3]
    sum_yy = df_sum.head()[4]
    sum_xy = df_sum.head()[5]

    # calculate correlation
    r = ((sum_n*sum_xy - sum_x*sum_y)/
        (math.sqrt(sum_n*sum_xx-sum_x*sum_x)*math.sqrt(sum_n*sum_yy-sum_y*sum_y)))

    print('\n')
    print(f'r = {r}')
    print(f'r^2 = {r*r}')
    print('############### Six Values #########################')
    print(f'sum_n = {sum_n}, sum_x = {sum_x}, sum_y = {sum_y}')
    print(f'sum_xx = {sum_xx}, sum_yy = {sum_yy}, sum_xy = {sum_xy}')
    print('\n')


if __name__ == '__main__':
    conf = SparkConf().setAppName('wordcount improved')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    main(inputs)




