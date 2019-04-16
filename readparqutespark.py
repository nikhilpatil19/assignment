from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext

sc =  SparkSession.builder \
    .master("local") \
    .enableHiveSupport() \
    .appName("parquets") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate() 
sqlContext = HiveContext(sc)

import sys
reload(sys)
sys.setdefaultencoding('utf8')
# to read parquet file
df1 = sqlContext.read.parquet('file:////home/mapr/Nikhil/userdata1.parquet')
df2 = sqlContext.read.parquet('file:////home/mapr/Nikhil/userdata2.parquet')
df3 = sqlContext.read.parquet('file:////home/mapr/Nikhil/userdata3.parquet')
df4 = sqlContext.read.parquet('file:////home/mapr/Nikhil/userdata4.parquet')
df5 = sqlContext.read.parquet('file:////home/mapr/Nikhil/userdata5.parquet')

join = df1.union(df2)
join = join.union(df3)
join = join.union(df4)
join = join.union(df5)
table1 = join.createOrReplaceTempView("sample_table")
table1 = sc.sql("create table hive_t_1 as select * from sample_table")
table1.write.option("header", "true").csv('file:////home/mapr/Nikhil/out.csv')

print('All things done right')



