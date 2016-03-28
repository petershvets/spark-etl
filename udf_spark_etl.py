#!/usr/bin/python

from pyspark.sql.functions import monotonicallyIncreasingId

def generate_pk(p_col_pk, p_df):
	"""
	Args: p_col_pk - Priamry Key column name
		  p_df - Data Frame for which to create PK column
	Returns: Spark DataFrame with new PK column
	"""
	dfPK = p_df.withColumn(p_col_pk, monotonicallyIncreasingId()+1)
	return dfPK


#df0 = sc.parallelize(range(2), 2).mapPartitions(lambda x: [(1,), (2,), (3,)]).toDF(['col1'])
#df1 = generate_pk('fWID', df0)
#df1.show()
