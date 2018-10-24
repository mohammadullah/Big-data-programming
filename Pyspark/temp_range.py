import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('temp_range').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
# sc = spark.sparkContext

# add more functions as necessary

def main(inputs, output):

     # main logic starts here
 
    observation_schema = types.StructType([
    types.StructField('station', types.StringType(), False),
    types.StructField('date', types.StringType(), False),
    types.StructField('observation', types.StringType(), False),
    types.StructField('value', types.IntegerType(), False),
    types.StructField('mflag', types.StringType(), False),
    types.StructField('qflag', types.StringType(), False),
    types.StructField('sflag', types.StringType(), False),
    types.StructField('obstime', types.StringType(), False),
    ])

    weather = spark.read.csv(inputs, schema=observation_schema)
    weather = weather.filter(weather.qflag.isNull())                                     ## keep if qflag in null
    weather = weather.select('date', 'station', 'observation', 'value').cache()          ## Select required columns
    
    t_max = weather.filter(weather.observation.isin('TMAX')).drop(weather.observation)      ## filter only TMAX observations
    t_min = weather.filter(weather.observation.isin('TMIN')).drop(weather.observation)      ## filter only TMIN observations
    t_min = t_min.selectExpr("date as date", "station as St", "value as val")               ## change columns name

    t_join = t_max.join(t_min, (t_max.date == t_min.date) & (t_max.station == t_min.St)).drop(t_min.date)   ## join min and max
    
    ## calculate range and add in a new column
    t_rang = t_join.withColumn('range', (t_join.value - t_join.val)/10).cache()               
    rang_max = t_rang.groupBy('date').agg({'range' : 'max'})                           ## groupby date nd get the maximum range                       
    
    ## Join maximum range values with the associated station and time
    rang_join = t_rang.join(rang_max, (t_rang.range == rang_max['max(range)']) & 
                (t_rang.date == rang_max.date)).select(t_rang.date, t_rang.station, t_rang.range)
    final = rang_join.orderBy(['date', 'station'], ascending=[1,1])                 


    final.write.csv(output, mode='overwrite')


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)