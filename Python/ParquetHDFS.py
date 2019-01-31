from pyspark import SparkContext
from pyspark.sql import SQLContext


sc = SparkContext(appName="CSV2Parquet")
sqlContext = SQLContext(sc)
from pyspark.sql.types import *

fields = [StructField("r_regionkey", LongType(), False),
          StructField("r_name", StringType(), True),
          StructField("r_comment", StringType(), True)]
schema = StructType(fields)


rdd = sc.textFile("/data/OLAP_Benchmark_data/region.tbl")

region_df = rdd.map(lambda x: x.split("|")) \
    .map(lambda x: {'r_regionkey': int(x[0]),
                    'r_name': x[1],
                    'r_comment': x[2]}).toDF(schema)

region_df.write.parquet("hdfs://namenode:8020/bardia-parquet-data/region.parquet")


#################################################################################################################################################
from pyspark import SparkContext
from pyspark.sql import SQLContext


sc = SparkContext(appName="CSV2Parquet")
sqlContext = SQLContext(sc)
from pyspark.sql.types import *

fields = [StructField("n_nationkey", LongType(), False),
          StructField("n_name", StringType(), True),
          StructField("n_regionkey", LongType(), True),
          StructField("n_comment", StringType(), True)
          
          ]
schema = StructType(fields)

rdd = sc.textFile("/data/OLAP_Benchmark_data/nation.tbl")

region_df = rdd.map(lambda x: x.split("|")) \
    .map(lambda x: {'n_nationkey': int(x[0]),
                    'n_name': x[1],
                    'n_regionkey': int(x[2]),
                    'n_comment' : x[3]
                    
                    }).toDF(schema)

region_df.write.parquet("hdfs://namenode:8020/bardia-parquet-data/nation.parquet")


#################################################################################################################################################
from pyspark import SparkContext
from pyspark.sql import SQLContext


sc = SparkContext(appName="CSV2Parquet")
sqlContext = SQLContext(sc)
from pyspark.sql.types import *

fields = [StructField("c_custkey", LongType(), False),
          StructField("c_name", StringType(), True),
          StructField("c_address", StringType(), True),
          StructField("c_nationkey", LongType(), True),
          StructField("c_phone", StringType(), True),
          StructField("c_acctbal", DoubleType(), True),
          StructField("c_mktsegment", StringType(), True),
          StructField("c_comment", StringType(), True)
          
          ]
schema = StructType(fields)


rdd = sc.textFile("/data/OLAP_Benchmark_data/customer.tbl")

region_df = rdd.map(lambda x: x.split("|")) \
    .map(lambda x: {'c_custkey': int(x[0]),
                    'c_name': x[1],
                    'c_address': x[2],
                    'c_nationkey' : int(x[3]),
                    'c_phone' : x[4],
                    'c_acctbal' : float(x[5]),
                    'c_mktsegment' : x[6],
                    'c_comment' : x[7]
                    
                    }).toDF(schema)

region_df.write.parquet("hdfs://namenode:8020/bardia-parquet-data/customer.parquet")


#################################################################################################################################################
from pyspark import SparkContext
from pyspark.sql import SQLContext


sc = SparkContext(appName="CSV2Parquet")
sqlContext = SQLContext(sc)
from pyspark.sql.types import *

fields = [StructField("l_orderkey", LongType(), False),
          StructField("l_partkey", LongType(), True),
          StructField("l_suppkey", LongType(), True),
          StructField("l_linenumber", DoubleType(), True),
          StructField("l_quantity", DoubleType(), True),
          StructField("l_extendedprice", DoubleType(), True),
          StructField("l_discount", DoubleType(), True),
          StructField("l_tax", StringType(), True),
          StructField("l_returnflag", StringType(), True),
          StructField("l_linestatus", StringType(), True),
          StructField("l_shipdate", StringType(), True),
          StructField("l_commitdate", StringType(), True),
          StructField("l_receiptdate", StringType(), True),
          StructField("l_shipinstruct", StringType(), True),
          StructField("l_shipmode", StringType(), True),
          StructField("l_comment", StringType(), True)        
          ]
schema = StructType(fields)


rdd = sc.textFile("/data/OLAP_Benchmark_data/lineitem.tbl")

region_df = rdd.map(lambda x: x.split("|")) \
    .map(lambda x: {'l_orderkey': int(x[0]),
                    'l_partkey': int(x[1]),
                    'l_suppkey': int(x[2]),
                    'l_linenumber' : float(x[3]),
                    'l_quantity' : float(x[4]),
                    'l_extendedprice' : float(x[5]),
                    'l_discount' : float(x[6]),
                    'l_tax' : x[7],
                    'l_returnflag' : x[8],
                    'l_linestatus' : x[9],
                    'l_shipdate' : x[10],
                    'l_commitdate' : x[11],
                    'l_receiptdate' : x[12],
                    'l_shipinstruct' : x[13],
                    'l_shipmode' : x[14],
                    'l_comment' : x[15]

                    }).toDF(schema)

region_df.write.parquet("hdfs://namenode:8020/bardia-parquet-data/lineitem.parquet")


#################################################################################################################################################
from pyspark import SparkContext
from pyspark.sql import SQLContext


sc = SparkContext(appName="CSV2Parquet")
sqlContext = SQLContext(sc)
from pyspark.sql.types import *

fields = [StructField("o_orderkey", LongType(), False),
          StructField("o_custkey", LongType(), True),
          StructField("o_orderstatus", StringType(), True),
          StructField("o_totalprice", DoubleType(), True),
          StructField("o_orderdate", StringType(), True),
          StructField("o_orderpriority", StringType(), True),
          StructField("o_clerk", StringType(), True),
          StructField("o_shippriority", LongType(), True),
          StructField("o_comment", StringType(), True) 
          ]
schema = StructType(fields)


rdd = sc.textFile("/data/OLAP_Benchmark_data/orders.tbl")

region_df = rdd.map(lambda x: x.split("|")) \
    .map(lambda x: {'o_orderkey': int(x[0]),
                    'o_custkey': int(x[1]),
                    'o_orderstatus': x[2],
                    'o_totalprice' : float(x[3]),
                    'o_orderdate' : x[4],
                    'o_orderpriority' : x[5],
                    'o_clerk' : x[6],
                    'o_shippriority' : int(x[7]),
                    'o_comment' : x[8]
                    }).toDF(schema)

region_df.write.parquet("hdfs://namenode:8020/bardia-parquet-data/orders.parquet")

#################################################################################################################################################
from pyspark import SparkContext
from pyspark.sql import SQLContext


sc = SparkContext(appName="CSV2Parquet")
sqlContext = SQLContext(sc)
from pyspark.sql.types import *

fields = [StructField("p_partkey", LongType(), False),
          StructField("p_name", StringType(), True),
          StructField("p_mfgr", StringType(), True),
          StructField("p_brand", StringType(), True),
          StructField("p_type", StringType(), True),
          StructField("p_size", LongType(), True),
          StructField("p_container", StringType(), True),
          StructField("p_retailprice", DoubleType(), True),
          StructField("p_comment", StringType(), True) 
          ]
schema = StructType(fields)


rdd = sc.textFile("/data/OLAP_Benchmark_data/part.tbl")

region_df = rdd.map(lambda x: x.split("|")) \
    .map(lambda x: {'p_partkey': int(x[0]),
                    'p_name': x[1],
                    'p_mfgr': x[2],
                    'p_brand' : x[3],
                    'p_type' : x[4],
                    'p_size' : int(x[5]),
                    'p_container' : x[6],
                    'p_retailprice' : float(x[7]),
                    'p_comment' : x[8]
                    }).toDF(schema)

region_df.write.parquet("hdfs://namenode:8020/bardia-parquet-data/part.parquet")


#################################################################################################################################################

from pyspark import SparkContext
from pyspark.sql import SQLContext


sc = SparkContext(appName="CSV2Parquet")
sqlContext = SQLContext(sc)
from pyspark.sql.types import *


fields = [StructField("ps_partkey", LongType(), False),
          StructField("ps_suppkey", LongType(), True),
          StructField("ps_availqty", LongType(), True),
          StructField("ps_supplycost", DoubleType(), True),
          StructField("ps_comment", StringType(), True)
          ]
schema = StructType(fields)


rdd = sc.textFile("/data/OLAP_Benchmark_data/partsupp.tbl")

region_df = rdd.map(lambda x: x.split("|")) \
    .map(lambda x: {'ps_partkey': int(x[0]),
                    'ps_suppkey': int(x[1]),
                    'ps_availqty': int(x[2]),
                    'ps_supplycost' : float(x[3]),
                    'ps_comment' : x[4]
                    }).toDF(schema)

region_df.write.parquet("hdfs://namenode:8020/bardia-parquet-data/partsupp.parquet")


#################################################################################################################################################

from pyspark import SparkContext
from pyspark.sql import SQLContext


sc = SparkContext(appName="CSV2Parquet")
sqlContext = SQLContext(sc)
from pyspark.sql.types import *

fields = [StructField("s_suppkey", LongType(), False),
          StructField("s_name", StringType(), True),
          StructField("s_address", StringType(), True),
          StructField("s_nationkey", LongType(), True),
          StructField("s_phone", StringType(), True),
          StructField("s_acctbal", DoubleType(), True),
          StructField("s_comment", StringType(), True)
          ]
schema = StructType(fields)


rdd = sc.textFile("/data/OLAP_Benchmark_data/supplier.tbl")

region_df = rdd.map(lambda x: x.split("|")) \
    .map(lambda x: {'s_suppkey': int(x[0]),
                    's_name': x[1],
                    's_address': x[2],
                    's_nationkey' : int(x[3]),
                    's_phone' : x[4],
                    's_acctbal' : float(x[5]),
                    's_comment' : x[6]
                    }).toDF(schema)

region_df.write.parquet("hdfs://namenode:8020/bardia-parquet-data/supplier.parquet")


#################################################################################################################################################