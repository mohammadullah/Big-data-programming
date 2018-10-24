

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types, SQLContext, Row
import sys
import operator, math
import re, string
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+


## Function to strip node and pointing paths

def map_line(line):

    line = line.strip()
    line_re = line.replace(':', '').split(' ')
    lin1 = line_re[0]
    lin2 = line_re[1:]

    return (lin1, lin2)


def main(inputs, output, source, dest):
    
    text = sc.textFile(inputs)
    line = text.map(map_line)
    line = line.flatMap(lambda x: map(lambda e: (e, x[0]), x[1]))                   # make all pairs of nodes
    line = line.map(lambda x: Row(node = int(x[1]), v_node = int(x[0])))            # map and create dataframe
    df = sqlContext.createDataFrame(line).cache()                                   
    first = [(source, 0, 0)]                                                        # the source dataframe
    df1 = sqlContext.createDataFrame(first, ['node', 'source', 'distance'])
    
    sourceflag = 1;      #  Flag for error print                                        

    for i in range(6):

        # Match and Join the pair data frame with path dataframe based on node and distance
        joined_df = df.join(df1, (df.node == df1.node) & (df1.distance == i)).drop(df1.node).\
                    select('v_node', 'node', 'distance')
        
        # Check whether source value is correct or available in the graph
        if (joined_df.count() == 0 and i == 0):
            print("\n")
            print("##########Given source is not found############")
            print("\n")
            sourceflag = 0;
            break;
        else:            # If source is available then continue

            # add distance column
            joined_df = joined_df.withColumn('value', functions.lit(i+1))  
            # rename columns for clarity
            joined_df = joined_df.selectExpr('v_node as node', 'node as source', 'value as distance')  
            # add dataframes
            joined_df = df1.unionAll(joined_df)    
            # Find the minimum distance is there is duplicate nodes        
            joined_df1 = joined_df.groupBy('node').agg({'distance' : 'min'})   
            
            # Join again after Agg.
            joined_df = joined_df.join(joined_df1, joined_df.node == joined_df1.node).drop(joined_df1.node)
            # rename column
            joined_df = joined_df.withColumnRenamed('min(distance)', 'mdistance')  

            # Filter and keep only minimum distination
            joined_df = joined_df.filter('distance <= mdistance').select('node', 'source', 'distance')
            df1 = joined_df
         
             # covert dataframe to rdd for printing
            rdd = df1.rdd.map(tuple) 
            rdd.saveAsTextFile(output + '/iter-' + str(i))
            
            # check whether path contains the destination
            # if contains then break from the loop
            end1 = df1.filter(df1.node == dest)             
            if (end1.count() > 0):                         
                break;
            
            # Check if reached the final loop and destination is not found
            if (end1 != dest and i == 5):                  
                print("\n")
                print("####### The destination is not found within six iterations #################")
                print("\n")
                sourceflag = 0;
    

   ## make the path list

    if (sourceflag == 1):
        result = []
        result.append(dest)

        ## Reverse loop to create the path
        for i in range(6):
            dest = df1.filter(df1.node == dest).collect()[0][1]
            result.append(dest)
            if dest == source:
                break;
 
        result.reverse()
        result = sc.parallelize(result)
        result.saveAsTextFile(output + '/path')
    

 
    
if __name__ == '__main__':
    conf = SparkConf().setAppName('wordcount improved')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    output = sys.argv[2]
    source = sys.argv[3]
    dest   = sys.argv[4]
    main(inputs, output, int(source), int(dest))




