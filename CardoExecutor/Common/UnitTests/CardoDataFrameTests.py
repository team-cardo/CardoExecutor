import pandas
from CardoExecutor.Common.Tests.CardoTestCase import CardoTestCase
from pyspark import RDD, Row
from pyspark.sql import DataFrame

from CardoExecutor.Common.CardoDataFrame import CardoDataFrame


class CardoDataFrameTests(CardoTestCase):
	def test_created_with_dataframe_returns_dataframe_correctly(self):
		dataset = self.context.spark.createDataFrame([['a']], 'column1: string')
		cardo_dataframe = CardoDataFrame(dataset, '6')
		self.assertIsInstance(cardo_dataframe.dataframe, DataFrame)
		self.assertEqual(dataset.collect(), cardo_dataframe.dataframe.collect())

	def test_created_with_dataframe_returns_rdd_correctly(self):
		dataset = self.context.spark.createDataFrame([['a']], 'column1: string')
		cardo_dataframe = CardoDataFrame(dataset, '6')
		self.assertIsInstance(cardo_dataframe.rdd, RDD)
		self.assertItemsEqual(dataset.collect(), cardo_dataframe.rdd.collect())

	def test_created_with_rdd_returns_rdd_correctly(self):
		rdd = self.context.spark.sparkContext.parallelize([Row(column1='a')])
		cardo_dataframe = CardoDataFrame(rdd, '6')
		self.assertIsInstance(cardo_dataframe.rdd, RDD)
		self.assertItemsEqual(rdd.collect(), cardo_dataframe.rdd.collect())

	def test_created_with_rdd_returns_dataframe_correctly(self):
		rdd = self.context.spark.sparkContext.parallelize([Row(column1='a')])
		cardo_dataframe = CardoDataFrame(rdd, '6')
		self.assertIsInstance(cardo_dataframe.dataframe, DataFrame)
		self.assertItemsEqual(rdd.collect(), cardo_dataframe.dataframe.collect())

	def test_set_dataframe(self):
		first_dataset = self.context.spark.createDataFrame([['a']], 'column1: string')
		second_dataset = self.context.spark.createDataFrame([['aa']], 'column1: string')
		cardo_dataframe = CardoDataFrame(first_dataset, '6')
		cardo_dataframe.dataframe = second_dataset
		self.assertItemsEqual(second_dataset.collect(), cardo_dataframe.dataframe.collect())
		self.assertItemsEqual(second_dataset.collect(), cardo_dataframe.rdd.collect())

	def test_set_rdd(self):
		first_dataset = self.context.spark.createDataFrame([['a']], 'column1: string')
		second_dataset = self.context.spark.sparkContext.parallelize([Row(column1='aa')])
		cardo_dataframe = CardoDataFrame(first_dataset, '6')
		cardo_dataframe.rdd = second_dataset
		self.assertItemsEqual(second_dataset.collect(), cardo_dataframe.dataframe.collect())
		self.assertItemsEqual(second_dataset.collect(), cardo_dataframe.rdd.collect())

	def test_created_with_pandas_returns_pandas_correctly(self):
		pandas_df = self.context.spark.createDataFrame([['a']], 'column1: string').toPandas()
		cardo_dataframe = CardoDataFrame(pandas_df)
		self.assertIsInstance(cardo_dataframe.pandas, pandas.DataFrame)
		self.assertTrue(pandas_df.equals(cardo_dataframe.pandas))

	def test_set_pandas(self):
		first_dataset = self.context.spark.createDataFrame([['a']], 'column1: string')
		second_dataset = self.context.spark.createDataFrame([['aa']], 'column1: string').toPandas()
		cardo_dataframe = CardoDataFrame(first_dataset, '6')
		cardo_dataframe.pandas = second_dataset
		self.assertTrue(second_dataset.equals(cardo_dataframe.pandas))
		self.assertItemsEqual(second_dataset.values[0][0], cardo_dataframe.dataframe.collect()[0][0])
		self.assertItemsEqual(second_dataset.values[0][0], cardo_dataframe.rdd.collect()[0][0])

	def test_created_with_dataframe_returns_pandas_correctly(self):
		dataset = self.context.spark.createDataFrame([['a']], 'column1: string')
		cardo_dataframe = CardoDataFrame(dataset, '6')
		self.assertIsInstance(cardo_dataframe.pandas, pandas.DataFrame)
		self.assertItemsEqual(dataset.collect()[0][0], cardo_dataframe.pandas.values[0][0])

	def test_created_with_rdd_returns_pandas_correctly(self):
		rdd = self.context.spark.sparkContext.parallelize([Row(column1='a')])
		cardo_dataframe = CardoDataFrame(rdd, '6')
		self.assertIsInstance(cardo_dataframe.pandas, pandas.DataFrame)
		self.assertItemsEqual(rdd.collect()[0][0], cardo_dataframe.pandas.values[0][0])

	def test_created_with_pandas_returns_dataframe_correctly(self):
		pandas = self.context.spark.createDataFrame([['aa']], 'column1: string').toPandas()
		cardo_dataframe = CardoDataFrame(pandas, '6')
		self.assertIsInstance(cardo_dataframe.dataframe, DataFrame)
		self.assertItemsEqual(pandas.values[0][0], cardo_dataframe.dataframe.collect()[0][0])

	def test_created_with_pandas_returns_rdd_correctly(self):
		pandas = self.context.spark.createDataFrame([['aa']], 'column1: string').toPandas()
		cardo_dataframe = CardoDataFrame(pandas, '6')
		self.assertIsInstance(cardo_dataframe.rdd, RDD)
		self.assertItemsEqual(pandas.values[0][0], cardo_dataframe.rdd.collect()[0][0])

	def test_unpersist_df(self):
		# Arrange
		df = self.context.spark.createDataFrame([['a']], 'column1: string')
		second_df = self.context.spark.createDataFrame([['b']], 'column1: string')
		cardo_dataframe = CardoDataFrame(df, '')
		cardo_dataframe.persist()
		cardo_dataframe.dataframe = second_df

		# Act
		cardo_dataframe.unpersist()

		# Assert
		self.assertFalse(df.is_cached)

	def test_unpersist_rdd(self):
		# Arrange
		rdd = self.context.spark.sparkContext.parallelize([Row(column1='aa')])
		second_rdd = self.context.spark.sparkContext.parallelize([Row(column1='bb')])
		cardo_dataframe = CardoDataFrame(rdd, '')
		cardo_dataframe.persist()
		cardo_dataframe.rdd = second_rdd

		# Act
		cardo_dataframe.unpersist()

		# Assert
		self.assertFalse(rdd.is_cached)

	def test_persist_df(self):
		# Arrange
		df = self.context.spark.createDataFrame([['a']], 'column1: string')
		cardo_dataframe = CardoDataFrame(df, '')

		# Act
		cardo_dataframe.persist()

		# Assert
		self.assertTrue(df.is_cached)

	def test_persist_rdd(self):
		# Arrange
		rdd = self.context.spark.sparkContext.parallelize([Row(column1='aa')])
		cardo_dataframe = CardoDataFrame(rdd, '')

		# Act
		cardo_dataframe.persist()

		# Assert
		self.assertTrue(rdd.is_cached)
