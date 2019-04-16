from pyspark.context import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql import SparkSession


sc =  SparkSession.builder \
    .master("local") \
    .enableHiveSupport() \
    .appName("parquets") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate() 
sqlContext = HiveContext(sc)


male_count=sc.read.json("file:////home/mapr/Nikhil/json/male_count")
female_count=sc.read.json("file:////home/mapr/Nikhil/json/female_count")
avg_male_sal=sc.read.json("file:////home/mapr/Nikhil/json/avg_male_sal")
avg_female_sal=sc.read.json("file:////home/mapr/Nikhil/json/avg_female_sal")
avg_org_sal=sc.read.json("file:////home/mapr/Nikhil/json/avg_org_sal")
max_male_sal=sc.read.json("file:////home/mapr/Nikhil/json/max_male_sal")
max_female_sal=sc.read.json("file:////home/mapr/Nikhil/json/max_female_sal")
max_org_sal=sc.read.json("file:////home/mapr/Nikhil/json/max_org_sal")

print('read json file')

male_count.write.parquet('file:///home/mapr/Nikhil/parquet/male_count' )
female_count.write.parquet('file:///home/mapr/Nikhil/parquet/female_count' )
avg_male_sal.write.parquet('file:///home/mapr/Nikhil/parquet/avg_male_sal' )
avg_female_sal.write.parquet('file:///home/mapr/Nikhil/parquet/avg_female_sal' )
avg_org_sal.write.parquet('file:///home/mapr/Nikhil/parquet/avg_org_sal' )
max_male_sal.write.parquet('file:///home/mapr/Nikhil/parquet/max_male_sal' )
max_female_sal.write.parquet('file:///home/mapr/Nikhil/parquet/max_female_sal' )
max_org_sal.write.parquet('file:///home/mapr/Nikhil/parquet/max_org_sal' )

print('file write to parquet')



















#df.write.json('file:////home/mapr/Nikhil/output.JSON')

#JSONdf = sqlContext.read.json("file:////home/mapr/Nikhil/output.JSON")
#JSONdf.write.parquet("file:////home/mapr/Nikhil/out.parquet")


