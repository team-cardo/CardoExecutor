import pyspark.sql.functions as F
from pyspark.sql.types import *

from CardoExecutor.Common.Tests.CardoTestCase import CardoTestCase
from MockClass import MockClass


class UnittestDemo(CardoTestCase):
	def test_simple(self):
		dataframe = self.context.spark.createDataFrame([['a']], 'col1: string')
		self.assertEqual(1, dataframe.count())

	def test_complex(self):
		dataframe = self.context.spark.createDataFrame([[1, 100.0],
														[1, 456.0]], 'id: int, data: double')
		dataframe = dataframe.groupBy('id').avg('data')
		self.assertEqual(1, dataframe.count())

	def test_class(self):
		dataframe = self.context.spark.createDataFrame([['a']], 'col1: string')
		expected = self.context.spark.createDataFrame([['a', '6']], 'col1: string, y: string')
		result = MockClass().execute(dataframe)
		self.assertDataFramesEqual(expected, result)

	def test_columns_order(self):
		dataframe = self.context.spark.createDataFrame([['a', 'b']], 'col1: string, col2: string')
		expected = self.context.spark.createDataFrame([['b', 'a']], 'col2: string, col1: string')
		self.assertDataFramesEqual(expected, dataframe, check_columns_order=False)

	def test_assert(self):
		struct_type = StructType([StructField('col1', StringType()),
								  StructField('col2', ArrayType(StructType([StructField('a', StringType())]))), ])
		dataframe = self.context.spark.createDataFrame([['a',
														 [
															 {'a': None},
															 {'a': 'x'},
															 {'a': 'y'},
														 ]]], struct_type)
		expected = self.context.spark.createDataFrame([['a',
														[
															{'a': 'x'},
															{'a': None},
															{'a': 'y'},
														]
														]], struct_type)
		self.assertDataFramesEqual(expected, dataframe)

	def test_udf(self):
		dataframe = self.context.spark.createDataFrame([['a']], 'col1: string')
		expected = self.context.spark.createDataFrame([['a', 'spark_test_case/consts.pyc']], 'col1: string, col2: string')
		dataframe = dataframe.withColumn('col2', F.udf(MockClass().udf_action)('col1'))
		self.assertDataFramesEqual(expected, dataframe, check_columns_order=False)
