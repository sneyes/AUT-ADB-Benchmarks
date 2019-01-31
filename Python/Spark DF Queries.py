
# nation = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/nation.parquet")
# region = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/region.parquet")
# customer = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/customer.parquet")
# lineitem = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/lineitem.parquet")
# part = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/part.parquet")
# partsupp = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/partsupp.parquet")
# supplier = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/supplier.parquet")
# order = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/orders.parquet")

###############################   Q1  ############################### 
from decimal import *
from pyspark.sql.types import *
from pyspark.sql import functions as sqlFunction
from pyspark.sql import *
startt = time.time()
lineitem = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/lineitem.parquet")
results = lineitem.filter("l_shipdate <= '1998-12-01' ").groupby('l_returnflag','l_linestatus').agg(sqlFunction.sum(lineitem.l_quantity).alias('sum_qty'), sqlFunction.sum(lineitem.l_extendedprice).alias('sum_base_price'), sqlFunction.sum(lineitem.l_extendedprice*(1-lineitem.l_discount)).alias('sum_disc_price'), sqlFunction.sum(lineitem.l_extendedprice*(1-lineitem.l_discount)*(1+lineitem.l_tax)).alias('sum_charge') , sqlFunction.avg(lineitem.l_quantity).alias('avg_qty'), sqlFunction.avg(lineitem.l_extendedprice).alias('avg_price'), sqlFunction.avg(lineitem.l_discount).alias('avg_disc')).sort(lineitem.l_returnflag,lineitem.l_linestatus).show()
endd = time.time()
querytime = endd - startt
querytime
###############################   Q2  ############################### 
#interval not implemented
from decimal import *
from pyspark.sql.types import *
from pyspark.sql import functions as sqlFunction
from pyspark.sql import *
startt = time.time()
nation = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/nation.parquet")
region = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/region.parquet")
part = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/part.parquet")
partsupp = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/partsupp.parquet")
supplier = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/supplier.parquet")
region1 = region.filter("r_name = 'asia' ")
nation_region = nation.join(region1,nation.n_regionkey == region1.r_regionkey)
nation_region_supplier = nation_region.join(supplier,nation_region.n_nationkey == supplier.s_nationkey)
min_sc = partsupp.agg(sqlFunction.min('ps_supplycost')).collect()
part1 = part.filter('p_size = 30').filter("p_type like '%steel'")
mind = min_sc[0].asdict()["min(ps_supplycost)"]
partsupp1 = partsupp.filter(partsupp.ps_supplycost == mind)
t1 = nation_region_supplier.join(partsupp1,partsupp1.ps_suppkey == nation_region_supplier.s_suppkey)
t2 = t1.join(part1,part1.p_partkey == t1.ps_partkey)
results = t2.select('s_acctbal','s_name','n_name','p_partkey','p_mfgr','s_address','s_phone','s_comment').orderby(sqlFunction.desc("s_acctbal"),'n_name','s_name','p_partkey').limit(100).show()
endd = time.time()
querytime = endd - startt
querytime
###############################   Q3  ############################### 
from decimal import *
from pyspark.sql.types import *
from pyspark.sql import functions as sqlFunction
from pyspark.sql import *
startt = time.time()
lineitem = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/lineitem.parquet")
customer = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/customer.parquet")
order = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/orders.parquet")
lineitem1 = lineitem.filter("l_shipdate > '1995-03-13' ")
order1 = order.filter("o_orderdate < '1995-03-13' ")
customer1 = customer.filter("c_mktsegment = 'automobile'")
lineitem_order = lineitem1.join(order1,lineitem1.l_orderkey == order1.o_orderkey)
customer_order = customer1.join(lineitem_order,customer1.c_custkey == order1.o_custkey)
pre = customer_order.select(customer_order.o_orderkey,customer_order.o_orderdate,customer_order.o_shippriority).join(lineitem1,customer_order.o_orderkey == lineitem1.l_orderkey).select(customer_order.o_orderkey,customer_order.o_orderdate,customer_order.o_shippriority,lineitem1.l_orderkey,customer_order.o_orderkey,customer_order.o_orderdate,lineitem1.l_extendedprice,lineitem1.l_discount)
results = pre.groupby('l_orderkey', 'o_orderdate', 'o_shippriority').agg(sqlFunction.sum(pre.l_extendedprice *(1 -lineitem1.l_discount)).alias("revenue")).sort(pre.o_orderdate,sqlFunction.desc("revenue")).limit(100).show()
endd = time.time()
querytime = endd - startt
querytime
###############################   Q4  ############################### 
from decimal import *
from pyspark.sql.types import *
from pyspark.sql import functions as sqlFunction
from pyspark.sql import *
startt = time.time()
order = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/orders.parquet")
order1 = order.filter("o_orderdate >= '1995-01-01' ").filter("o_orderdate < '1995-04-01'")
lineitem = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/lineitem.parquet")
lineitem1 = lineitem.filter('l_commitdate < l_receiptdate')
results = lineitem1.join(order1, lineitem1.l_orderkey == order1.o_orderkey).groupby('o_orderpriority').agg(sqlFunction.count('*').alias("order_count")).select(order1.o_orderpriority,'order_count').orderby('o_orderpriority').show()
endd = time.time()
querytime = endd - startt
querytime
###############################   Q5  ############################### 
from decimal import *
from pyspark.sql.types import *
from pyspark.sql import functions as sqlFunction
from pyspark.sql import *
startt = time.time()
order = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/orders.parquet")
nation = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/nation.parquet")
region = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/region.parquet")
customer = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/customer.parquet")
supplier = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/supplier.parquet")
lineitem = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/lineitem.parquet")
region1 = region.filter("r_name = 'middle east'")
order1 = order.filter("o_orderdate >= '1994-01-01' ").filter("o_orderdate < '1995-01-01'")
customer_order = customer.join(order1,customer.c_custkey == order1.o_custkey).select('o_orderkey')
region_nation = region1.join(nation,region1.r_regionkey == nation.n_regionkey)
region_nation_supplier = region_nation.join(supplier,region_nation.n_nationkey == supplier.s_nationkey)
t1 = region_nation_supplier.join(lineitem,region_nation_supplier.s_suppkey == lineitem.l_suppkey).select('n_name','l_extendedprice','l_discount','l_orderkey')
t2 = t1.join(customer_order,t1.l_orderkey == customer_order.o_orderkey)
res = t2.groupby('n_name').agg(sqlFunction.sum(t2.l_extendedprice *(1 -t2.l_discount)).alias("revenue")).sort(sqlFunction.desc("revenue")).show()
endd = time.time()
querytime = endd - startt
querytime
###############################   Q6  ###############################
from decimal import *
from pyspark.sql.types import *
from pyspark.sql import functions as sqlFunction
from pyspark.sql import *
startt = time.time()
lineitem = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/lineitem.parquet")
lineitem1 = lineitem.filter("l_shipdate >= '1994-01-01' and l_shipdate < '1995-01-01' and l_discount >= 0.05 and l_discount >= 0.07 and l_quantity < 24")
res = lineitem1.agg(sqlFunction.sum(lineitem1.l_extendedprice * (lineitem1.l_discount)).alias("revenue")).show()
endd = time.time()
querytime = endd - startt
querytime
###############################   Q7  ############################### 
from decimal import *
from pyspark.sql.types import *
from pyspark.sql import functions as sqlFunction
from pyspark.sql import *
startt = time.time()
lineitem = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/lineitem.parquet")
nation = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/nation.parquet")
order = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/orders.parquet")
supplier = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/supplier.parquet")
nation1 = nation.filter("n_name  like 'japan%' or  n_name  like 'india%' ")
lineitem1 = lineitem.filter("l_shipdate >= '1995-01-01' and l_shipdate < '1996-12-31'")
supplier_nation = nation1.join(supplier,nation1.n_nationkey == supplier.s_nationkey).join(lineitem1,supplier.s_suppkey == lineitem1.l_suppkey).withcolumnrenamed("n_name", "supp_nation").select('l_orderkey','l_extendedprice','l_discount','l_shipdate','supp_nation')
t1 = nation1.join(customer,nation1.n_nationkey == customer.c_nationkey) .join(order,customer.c_custkey == order.o_custkey).withcolumnrenamed("n_name", "cust_nation") .select('cust_nation','o_orderkey') .join(supplier_nation, order.o_orderkey == supplier_nation.l_orderkey).filter(("supp_nation like 'india%' and cust_nation  like 'japan%'") or ("supp_nation  like 'japan%' and cust_nation  like 'india%'"))
results = t1.select('supp_nation','cust_nation',sqlFunction.year("l_shipdate").alias("l_year"),'l_discount','l_extendedprice').groupby('supp_nation','cust_nation','l_year').agg(sqlFunction.sum(t1.l_extendedprice * (1 - t1.l_discount)).alias("revenue")).orderby('supp_nation','cust_nation','l_year').show()
endd = time.time()
querytime = endd - startt
querytime
###############################   Q10  ############################### 
from decimal import *
from pyspark.sql.types import *
from pyspark.sql import functions as sqlFunction
from pyspark.sql import *
startt = time.time()
lineitem = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/lineitem.parquet")
customer = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/customer.parquet")
order = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/orders.parquet")
nation = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/nation.parquet")
lineitem1 = lineitem.filter("l_returnflag == 'r' ")
order1 = order.filter("o_orderdate >= '1994-01-01' ").filter("o_orderdate < '1994-11-01'")
order_customer = order1.join(customer,order1.o_custkey == customer.c_custkey)
order_customer_nation = order_customer.join(nation,order_customer.c_nationkey == nation.n_nationkey)
t1 = order_customer_nation.join(lineitem1,lineitem1.l_orderkey == order_customer_nation.o_orderkey)
t2 = t1.select('c_custkey','c_name',(t1.l_extendedprice*(1-t1.l_discount)).alias("vol"),'c_acctbal','n_name','c_address','c_phone','c_comment')
results = t2.groupby('c_custkey','c_name','c_acctbal','n_name','c_address','c_comment').agg(sqlFunction.sum("vol")).show()
endd = time.time()
querytime = endd - startt
querytime
###############################   Q11  ############################### 
from decimal import *
from pyspark.sql.types import *
from pyspark.sql import functions as sqlFunction
from pyspark.sql import *
startt = time.time()
nation = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/nation.parquet")
partsupp = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/partsupp.parquet")
supplier = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/supplier.parquet")
nation1 = nation.filter("n_name like '%argentina%' ")
nation_supplier = nation1.join(supplier,nation.n_nationkey == supplier.s_nationkey).select('s_suppkey')
nation_supplier_partsupp = nation_supplier.join(partsupp,nation_supplier.s_suppkey == partsupp.ps_suppkey).select('ps_partkey',(partsupp.ps_supplycost*partsupp.ps_availqty).alias("value"))
sum = nation_supplier_partsupp.agg(sqlFunction.sum("value").alias("total_value"))
t1 = nation_supplier_partsupp.groupby('ps_partkey').agg(sqlFunction.sum("value").alias("part_value"))
results = t1.crossjoin(sum).filter(t1.part_value > (0.0001*sum.total_value)).sort(sqlFunction.desc("part_value")).show()
endd = time.time()
querytime = endd - startt
querytime
###############################   Q12  ############################### 
from decimal import *
from pyspark.sql.types import *
from pyspark.sql import functions as sqlFunction
from pyspark.sql import *
startt = time.time()
def highpriority(x):
    if "2-high" in x or "1-urgent" in x: return 1
    else: return 0

def lowpriority(x):
    if "2-high" in x or "1-urgent" in x: return 0
    else: return 1
hpu = sqlFunction.udf(highpriority,integertype())
lpu = sqlFunction.udf(lowpriority,integertype())
lineitem = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/lineitem.parquet")
order = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/orders.parquet")
lineitem1 = lineitem.filter(("l_shipmode = 'mail' " or "l_shipmode = 'ship' ") and 'l_commitdate < l_receiptdate' and 'l_shipdate < l_commitdate'  and "l_receiptdate >= '1997-01-01' " and " l_receiptdate >= '1997-01-01' ")
lineitem_order = lineitem1.join(order,lineitem1.lineitem_orderrderkey == order.o_orderkey).select('l_shipmode' , 'o_orderpriority')
results = lineitem_order.groupby('l_shipmode').agg(sqlFunction.sum(hpu(lineitem_order.o_orderpriority).alias("sum_highorderpriority")),sqlFunction.sum(lpu(lineitem_order.o_orderpriority).alias("sum_loworderpriority"))).sort("l_shipmode").show()
endd = time.time()
querytime = endd - startt
querytime
###############################   Q13  ############################### 
from decimal import *
from pyspark.sql.types import *
from pyspark.sql import functions as sqlFunction
from pyspark.sql import *
startt = time.time()
customer = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/customer.parquet")
order = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/orders.parquet")
order1 = order.filter(" o_comment not like '%pending%deposits%' ")
customer_order = customer.join(order1,customer.c_custkey == order1.o_custkey,"left_outer")
t1 = customer_order.groupby('o_orderkey').agg(sqlFunction.count('o_orderkey').alias("c_count"))
customer_order = customer_order.select('o_custkey',customer_order.o_orderkey.alias("o_orderkey2"))
results = t1.join(customer_order,customer_order.o_orderkey2 == t1.o_orderkey).select('c_count','o_custkey','o_orderkey').groupby('c_count').agg(sqlFunction.count('o_custkey').alias("custdist")).sort(sqlFunction.desc('custdist'),sqlFunction.desc('c_count')).show()
endd = time.time()
querytime = endd - startt
querytime
###############################   Q14  ############################### 
from decimal import *
from pyspark.sql.types import *
from pyspark.sql import functions as sqlFunction
from pyspark.sql import *
startt = time.time()
def promo(data,out):
    if data.startswith("promo"): return float(out)
    else: return float(0)   
pud = sqlFunction.udf(promo,floattype())
lineitem = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/lineitem.parquet")
part = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/part.parquet")
lineitem1 = lineitem.filter("l_shipdate>='1996-12-01'" and "l_shipdate<'1997-01-01'")
lineitem_part = part.join(lineitem1,part.p_partkey == lineitem.l_partkey)
t1 = lineitem_part.select('p_type',(lineitem_part.l_extendedprice*(1-lineitem_part.l_discount)).alias("value"))
t2 = t1.agg(sqlFunction.sum("value").alias("total_value")).collect()
t3 = t2[0].asdict()["total_value"]
results = t1.agg(sqlFunction.sum(pud(t1.p_type,t1.value))*100/t3).show()
endd = time.time()
querytime = endd - startt
querytime
###############################   Q15  ###############################
from decimal import *
from pyspark.sql.types import *
from pyspark.sql import functions as sqlFunction
from pyspark.sql import *
startt = time.time()
lineitem = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/lineitem.parquet")
lineitem1 = lineitem.filter("l_shipdate >= '1997-07-01'" and "l_shipdate < '1997-10-01'").select('l_suppkey',(lineitem.l_extendedprice*(1-lineitem.l_discount)).alias("val"))
tot = lineitem1.groupby("l_suppkey").agg(sqlFunction.sum("val").alias("total"))
tmp = tot.agg(sqlFunction.max("total").alias("maxx")).collect()
tmpmax = tmp[0].asdict()["maxx"]
results = tot.filter(tot.total == tmpmax).join(supplier,supplier.s_suppkey == tot.l_suppkey).select('s_name','s_suppkey','s_address','s_phone','total').sort('s_suppkey')
results.show()
endd = time.time()
querytime = endd - startt
querytime
###############################   Q16  ############################### 
from decimal import *
from pyspark.sql.types import *
from pyspark.sql import functions as sqlFunction
from pyspark.sql import *
startt = time.time()
partsize = [4,7,12,19,21,39,48]
part = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/part.parquet")
supplier = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/supplier.parquet")
part1 = part.filter("p_brand != 'brand#31' " and "p_type not like 'large plated%'" and part.p_size.isin(partsize)).select('p_partkey','p_type','p_brand','p_size')
supplier_part = supplier.filter("s_comment like '%customer%complaints%' ").join(partsupp,supplier.s_suppkey == partsupp.ps_suppkey)
results = supplier_part.join(part1,supplier_part.ps_partkey == part1.p_partkey).groupby('p_brand','p_type','p_size').agg(sqlFunction.count('ps_suppkey').alias("supplier_count")).sort(sqlFunction.desc("supplier_count"),'p_brand','p_type','p_size')
results.show()
endd = time.time()
querytime = endd - startt
querytime
###############################   Q17  ############################### 
from decimal import *
from pyspark.sql.types import *
from pyspark.sql import functions as sqlFunction
from pyspark.sql import *
startt = time.time()
lineitem = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/lineitem.parquet")
part = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/part.parquet")
lineitem1 = lineitem.select('l_partkey','l_quantity','l_extendedprice')
part1 = part.filter("p_brand like '%brand#44%' " and "p_container like '%wrap pkg%' ")
part_lineitem = part1.join(lineitem1,part1.p_partkey == lineitem.l_partkey,"left_outer")
part2 = part_lineitem.groupby("p_partkey").agg(sqlFunction.avg(part_lineitem.l_quantity*0.2).alias("avg_q")).select(part_lineitem.p_partkey.alias("key"),'avg_q')
part2 = part2.join(part_lineitem,part2.key == part_lineitem.p_partkey).select('avg_q','key','l_quantity','l_extendedprice')
results = part1.join(part2,part2.key == part1.p_partkey).filter(part2.l_quantity < part2.avg_q).agg((sqlFunction.sum(part2.l_extendedprice)/7.00).alias("avg_yearly"))
results.show()
endd = time.time()
querytime = endd - startt
querytime
###############################   Q18  ############################### 
from decimal import *
from pyspark.sql.types import *
from pyspark.sql import functions as sqlFunction
from pyspark.sql import *
startt = time.time()
lineitem = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/lineitem.parquet")
order = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/orders.parquet")
customer = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/customer.parquet")
lineitem1 = lineitem.groupby('l_orderkey').agg(sqlFunction.sum('l_quantity').alias("sum_quantity")).filter('sum_quantity > 300').select(lineitem.l_orderkey.alias("key"),'sum_quantity')
lineitem_order = lineitem1.join(order,lineitem1.key == order.o_orderkey)
lineitem_order = lineitem_order.join(lineitem,lineitem.l_orderkey == lineitem_order.key)
results = lineitem_order.join(customer,customer.c_custkey == lineitem_order.o_custkey).select('l_quantity','c_name','c_custkey','o_orderkey','o_orderdate','o_totalprice').groupby('c_name','c_custkey','o_orderkey','o_orderdate','o_totalprice').agg(sqlFunction.sum('l_quantity')).sort(sqlFunction.desc("o_totalprice"),'o_orderdate').limit(100)
results.show()
endd = time.time()
querytime = endd - startt
querytime
###############################   Q19  ############################### 
from decimal import *
from pyspark.sql.types import *
from pyspark.sql import functions as sqlFunction
from pyspark.sql import *
startt = time.time()
part = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/part.parquet")
lineitem = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/lineitem.parquet")
case1 = ["sm case","sm box","sm pack","sm pkg"]
case2 = ["med bag","med box","med pkg","med pack"]
case3 = ["lg case","lg box","lg pack","lg pkg"]
part_lineitem = part.join(lineitem,part.p_partkey == lineitem.l_partkey).filter(("l_shipmode like '%air%' " or "l_shipmode like '%air reg%' ") and "l_shipinstruct like '%deliver in person%'")
results = part_lineitem.filter( ( (part_lineitem.p_brand.like("%brand#54%")) & (part_lineitem.p_container.isin(case1)) & (part_lineitem.l_quantity >= 8) & (part_lineitem.l_quantity <= 18) &  (part_lineitem.p_size >= 1) & (part_lineitem.p_size <= 10) ) | ((part_lineitem.p_brand.like("%brand#22%")) & (part_lineitem.p_container.isin(case2)) & (part_lineitem.l_quantity >= 3) & (part_lineitem.l_quantity <= 23) &  (part_lineitem.p_size >= 1) & (part_lineitem.p_size <= 5) ) | ((part_lineitem.p_brand.like("%brand#51%")) & (part_lineitem.p_container.isin(case3)) & (part_lineitem.l_quantity >= 22) & (part_lineitem.l_quantity <= 32) &  (part_lineitem.p_size >= 1) & (part_lineitem.p_size <= 15) ) ).select((part_lineitem.l_extendedprice*(1-part_lineitem.l_discount)).alias("volume")).agg(sqlFunction.sum("volume"))
results.show()
endd = time.time()
querytime = endd - startt
querytime
###############################   Q20  ############################### 
from decimal import *
from pyspark.sql.types import *
from pyspark.sql import functions as sqlFunction
from pyspark.sql import *
startt = time.time()

lineitem = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/lineitem.parquet")
partsupp = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/partsupp.parquet")
part = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/part.parquet")
nation = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/nation.parquet")
supplier = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/supplier.parquet")
lineitem1 = lineitem.filter("l_shipdate >= '1993-01-01' " and "l_shipdate < '1994-01-01' ").groupby('l_partkey','l_suppkey').agg(sqlFunction.sum(lineitem.l_quantity*0.5).alias("sum_q"))
part1 = part.filter(part.p_name.startswith("forest")).select('p_partkey')
nation1 = nation.filter("n_name like '%canada%' ")
nation_supplier = supplier.select('s_suppkey','s_name','s_nationkey','s_address').join(nation1,supplier.s_nationkey == nation1.n_nationkey)
part_partsupp = part1.join(partsupp,part1.p_partkey == partsupp.ps_partkey)
part_partsupp_lineitem = part_partsupp.join(lineitem1,lineitem1.l_suppkey == part_partsupp.ps_suppkey, lineitem1.l_partkey == part_partsupp.ps_partkey).filter(part_partsupp.ps_availqty > lineitem1.sum_q).select(part_partsupp.ps_suppkey)
results = part_partsupp_lineitem.join(nation_supplier,nation_supplier.s_suppkey == part_partsupp_lineitem.ps_suppkey).select(nation_supplier.s_name,nation_supplier.s_address).sort(nation_supplier.s_name)
results.show()
endd = time.time()
querytime = endd - startt
querytime
###############################   Q21  ############################### 
from decimal import *
from pyspark.sql.types import *
from pyspark.sql import functions as sqlFunction
from pyspark.sql import *
startt = time.time()
lineitem = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/lineitem.parquet")
supplier = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/supplier.parquet")
order = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/orders.parquet")
nation = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/nation.parquet")
supplier1 = supplier.select('s_suppkey','s_nationkey','s_name')
lineitem1 = lineitem.select('l_suppkey','l_orderkey','l_receiptdate','l_commitdate')
lineitem2 = lineitem1.filter('l_receiptdate > l_commitdate')
order1 = orders.select('o_orderkey','o_orderstatus').filter("o_orderstatus = 'f' ")
nation1 = nation.filter("n_name like '%egypt%' ")
tmp1 = lineitem1.groupby(lineitem1.l_orderkey).agg( sqlFunction.countdistinct(lineitem1.l_suppkey).alias("sup_count"),sqlFunction.max(lineitem1.l_suppkey).alias("sup_max") ).select(lineitem1.l_orderkey.alias("key"),"sup_count","sup_max")
tmp2 = lineitem2.groupby(lineitem1.l_orderkey).agg( sqlFunction.countdistinct(lineitem1.l_suppkey).alias("sup_count"),sqlFunction.max(lineitem1.l_suppkey).alias("sup_max") ).select(lineitem1.l_orderkey.alias("key"),"sup_count","sup_max")
nation_supplier = nation1.join(supplier1,supplier1.s_nationkey == nation1.n_nationkey)
nation_supplier_lineitem = nation_supplier.join(lineitem2,nation_supplier.s_suppkey == lineitem2.l_suppkey)
nation_supplier_lineitem_order = nation_supplier_lineitem.join(order1,order1.o_orderkey == nation_supplier_lineitem.l_orderkey)
nation_supplier_lineitem_order_tmp1 = nation_supplier_lineitem_order.join(tmp1,tmp1.key == nation_supplier_lineitem_order.l_orderkey).filter((tmp1.sup_count>1)|(tmp1.sup_count == 1)&(nation_supplier_lineitem_order.l_suppkey == tmp1.sup_max)).select(nation_supplier_lineitem_order.s_name,nation_supplier_lineitem_order.l_orderkey,nation_supplier_lineitem_order.l_suppkey)
results = nation_supplier_lineitem_order_tmp1.join(tmp2,nation_supplier_lineitem_order_tmp1.l_orderkey == tmp2.key,"left_outer").select(nation_supplier_lineitem_order_tmp1.s_name, nation_supplier_lineitem_order_tmp1.l_orderkey, nation_supplier_lineitem_order_tmp1.l_suppkey,tmp2.sup_count,tmp2. sup_max).filter((tmp2.sup_count == 1)&(nation_supplier_lineitem_order_tmp1.l_suppkey == tmp2.sup_max)).groupby(nation_supplier_lineitem_order_tmp1.s_name).agg(sqlFunction.count(nation_supplier_lineitem_order_tmp1.l_suppkey).alias("num_wait")).sort(sqlFunction.desc("num_wait"),nation_supplier_lineitem_order_tmp1.s_name)
results.show()
endd = time.time()
querytime = endd - startt
querytime
###############################   Q22  ############################### 
from decimal import *
from pyspark.sql.types import *
from pyspark.sql import functions as sqlFunction
from pyspark.sql import *
startt = time.time()
order = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/orders.parquet")
customer = sqlContext.read.parquet("hdfs://namenode:8020/bardia-parquet-data/customer.parquet")
clist = ["20", "40", "22", "30", "39", "42", "21"]
customer1 = customer.select('c_acctbal','c_custkey',customer.c_phone.substr(1,2).alias("country_code"))
customer1 = customer1.filter(customer1.country_code.isin(clist))
tmp = customer1.filter('c_acctbal > 0.0 ').agg(sqlFunction.avg(customer1.c_acctbal).alias("avg_ac"))
order_customer = orders.select('order_customerustkey').join(customer1,orders.order_customerustkey == customer1.c_custkey,"right_outer")
results = order_customer.join(tmp).filter(order_customer.c_acctbal > tmp.avg_ac).groupby(order_customer.country_code).agg(sqlFunction.count(order_customer.c_acctbal),sqlFunction.sum(order_customer.c_acctbal)).sort(order_customer.country_code)
results.show()
endd = time.time()
querytime = endd - startt
querytime
