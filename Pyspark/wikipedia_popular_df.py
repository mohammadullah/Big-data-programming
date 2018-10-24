import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('wikipedia_popular').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
# sc = spark.sparkContext

# add more functions as necessary

## UDF for file path
@functions.udf(returnType=types.StringType())
def path_to_hour(path):
    path = path.split('/')[-1][11:22]

    return path


special = functions.udf(lambda title: "Special:" not in title, types.BooleanType())  
mainpage = functions.udf(lambda title: "Main_Page" not in title, types.BooleanType())


def main(inputs, output):

   # main logic starts here


    columns_schema = types.StructType([
    types.StructField('language', types.StringType(), True),
    types.StructField('title', types.StringType(), True),
    types.StructField('views', types.IntegerType(), True),
    types.StructField('size', types.IntegerType(), True),
    
])

    wikipage = spark.read.csv(inputs, schema=columns_schema, sep = ' ')            ## Read the file
    wikipage = wikipage.withColumn('filename', functions.input_file_name())        ## Take the file path
    wikipage = wikipage.withColumn('hour', path_to_hour(wikipage['filename']))     ## Extract only time
    wikipage = wikipage.select('hour', 'language', 'title', 'views')               ## Select required columns
    wikipage = wikipage.filter(wikipage.language.isin('en'))                       ## take only 'en' language
    
    ## Remove tiltes with special and mainpage 
    wikipage = wikipage.filter(special(wikipage.title))                                
    wikipage = wikipage.filter(mainpage(wikipage.title)).drop('language').cache()
 
    wikimax = wikipage.groupBy('hour').agg({'views' : 'max'})                      ## Calculate max view per hour                          
    
    #wikijoin = wikipage.join(wikimax, (wikipage.hour == wikimax.hour)             ## join without broadcast (please uncomment)
    #          & (wikipage.views == wikimax['max(views)'])).drop(wikimax.hour)
    
    ## Use broadcast join
    wikijoin = wikipage.join(functions.broadcast(wikimax), (wikipage.hour == wikimax.hour) 
              & (wikipage.views == wikimax['max(views)'])).drop(wikimax.hour)

    
    wikijoin = wikijoin.select('hour', 'title', 'views')                           ## Select required columns
    wikijoin = wikijoin.orderBy(['hour', 'title'], ascending=[1,1])                ## sort by time and title


    wikijoin.write.json(output, mode='overwrite')
    wikijoin.explain()
    

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
