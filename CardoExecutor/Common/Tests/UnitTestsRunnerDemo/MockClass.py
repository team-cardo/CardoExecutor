import pyspark.sql.functions as F


class MockClass:
	def execute(self, df):
		df = df.withColumn('y', F.lit('6'))
		return df

	def udf_action(self, value):
		print(6)
