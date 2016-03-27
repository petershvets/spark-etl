#!/usr/bin/python

def udf_spark_etl:
	def generate_pk(df):
	sqlDFPK = df.withColumn('WID', monotonicallyIncreasingId()+1)
	return sqlDFPK
