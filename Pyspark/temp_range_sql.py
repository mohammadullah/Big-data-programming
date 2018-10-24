import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('temp_range_sql').getOrCreate()
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
    weather.createOrReplaceTempView("weather_sql")
    w_qflag = spark.sql("SELECT * FROM weather_sql WHERE qflag IS NULL")
    w_qflag.createOrReplaceTempView("w_qflag")
    w_qflag = spark.sql("SELECT date, station, observation, value FROM w_qflag")
    w_qflag.createOrReplaceTempView("w_qflag")
    t_max = spark.sql("SELECT * FROM w_qflag WHERE observation = 'TMAX'")
    t_max.createOrReplaceTempView("t_max")
 

    t_min = spark.sql("SELECT * FROM w_qflag WHERE observation = 'TMIN'")
    t_min.createOrReplaceTempView("t_min")
    t_min = t_min.selectExpr("date as date", "station as St", "value as val")
    t_min.createOrReplaceTempView("t_min")


    t_join = spark.sql("""SELECT t_max.date, t_max.station, t_max.value, t_min.val 
                          FROM t_max INNER JOIN t_min
                          ON t_max.date = t_min.date AND t_max.station = t_min.St""")
    t_join.createOrReplaceTempView("t_join")
    t_join = spark.sql("SELECT date, station, value, val, (value - val)/10 AS range FROM t_join")
    t_join.createOrReplaceTempView("t_join")
    tj_max = spark.sql("SELECT date, MAX(range) AS maxrange FROM t_join GROUP BY date")
    tj_max.createOrReplaceTempView("tj_max")
    
    final = spark.sql("""SELECT t_join.date, t_join.station, tj_max.maxrange
                         FROM t_join INNER JOIN tj_max
                         ON t_join.date = tj_max.date AND t_join.range = tj_max.maxrange""")
    final.createOrReplaceTempView("final")
    final = spark.sql("SELECT * FROM final ORDER BY date, station")


    final.write.csv(output, mode='overwrite')


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)