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

male_count = sqlContext.sql(" SELECT count(*) as male_count FROM hive_t1 WHERE gender = 'Male' ")
female_count = sqlContext.sql(" SELECT count(*) as female_cout FROM hive_t1 WHERE gender = 'Female' ")
avg_male_sal = sqlContext.sql(" SELECT AVG(salary) as avg_male_sal FROM hive_t1 WHERE gender = 'Male' ")
avg_female_sal = sqlContext.sql(" SELECT AVG(salary) as avg_female_sal FROM hive_t1 WHERE gender = 'Female' ")
avg_org_sal = sqlContext.sql(" SELECT AVG(salary) as avg_org_sal FROM hive_t1")
max_male_sal = sqlContext.sql(" SELECT MAX(salary) as max_male_sal FROM hive_t1 WHERE gender = 'Male' ")
max_female_sal = sqlContext.sql(" SELECT MAX(salary) as max_female_sal FROM hive_t1 WHERE gender = 'Female' ")
max_org_sal = sqlContext.sql(" SELECT MAX(salary) as max_org_sal FROM hive_t1")
print("heee")


male_count.write.json('file:////home/mapr/Nikhil/json/male_count')
female_count.write.json('file:////home/mapr/Nikhil/json/female_count')
avg_male_sal.write.json('file:////home/mapr/Nikhil/json/avg_male_sal')
avg_female_sal.write.json('file:////home/mapr/Nikhil/json/avg_female_sal')
avg_org_sal.write.json('file:////home/mapr/Nikhil/json/avg_org_sal')
max_male_sal.write.json('file:////home/mapr/Nikhil/json/max_male_sal')
max_female_sal.write.json('file:////home/mapr/Nikhil/json/max_female_sal')
max_org_sal.write.json('file:////home/mapr/Nikhil/json/max_org_sal')


print('write into json file')