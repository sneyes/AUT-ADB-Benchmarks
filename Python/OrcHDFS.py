from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext(appName="CSV2Parquet")
sqlContext = SQLContext(sc)


sqlContext.setConf('spark.sql.orc.impl', 'native')
region = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/region.parquet")

region.write.save("hdfs://namenode:8020/bardia-orc-data/region.orc", mode='overwrite', format='orc')


