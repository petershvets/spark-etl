from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext

APP_NAME = "Spark-ETL"
conf = SparkConf().setAppName(APP_NAME).setMaster("spark://pdpshvets.modeln.com:7077")
#conf = conf.setMaster("local[*]")
sc = SparkContext(conf=conf)
#sc = SparkContext('local', 'pyspark.sql')
sqlContext = HiveContext(sc)

sqlContext.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
sqlContext.sql("LOAD DATA LOCAL INPATH '/home/pshvets/Spark_SQL/spark/spark-1.5.2-bin-hadoop2.6/examples/src/main/resources/kv1.txt' INTO TABLE src")
desc_table=sqlContext.sql("describe formatted src")
print("Table desc: ", type(desc_table))

# Queries can be expressed in HiveQL.
results = sqlContext.sql("FROM src SELECT key, value").collect()
print("Result type is: ", type(results))
#print(results)

# Read Hive pseudo-table

pseudo_tbl = sqlContext.sql("SHOW TABLES")

print("Read Hive pseudo-table: ", pseudo_tbl, type(pseudo_tbl))
pseudo_tbl.show()
