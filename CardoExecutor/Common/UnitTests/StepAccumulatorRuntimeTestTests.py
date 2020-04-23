import unittest

from pyspark import Row

from CardoExecutor.Common.CardoDataFrame import CardoDataFrame
from CardoExecutor.Common.Factory.CardoContextFactory import CardoContextFactory
from CardoExecutor.Common.Tests.StepAccumulatorRuntimeTest import StepAccumulatorRuntimeTest
from CardoExecutor.Common.UnitTests.MockLoggingHandler import MockLoggingHandler
from CardoExecutor.Common.CardoContext import CardoContext


class StepAccumulatorRuntimeTestTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.context = CardoContextFactory().create_context('unittest', environment='dev', partitions=1)

    @classmethod
    def tearDownClass(cls):
        cls.context.spark.stop()
        CardoContext.destroy()

    def setUp(self):
        self.log_handler = MockLoggingHandler()
        self.context.logger.root.handlers = []
        self.context.logger.root.addHandler(self.log_handler)

    def test_empty_table(self):
        # Arrange
        dataset_no_rows = CardoDataFrame(self.context.spark.createDataFrame([], schema="column: string"))
        dataset_no_cols = CardoDataFrame(self.context.spark.createDataFrame([[]]))
        no_rows_test = StepAccumulatorRuntimeTest(lambda x: x, 'unittest')
        no_cols_test = StepAccumulatorRuntimeTest(lambda x: x, 'unittest')

        # Act
        no_rows_test.test(self.context, dataset_no_rows).dataframe.collect()
        no_cols_test.test(self.context, dataset_no_cols).dataframe.collect()

        # Assert
        self.assertEqual(0, sum(map(lambda record: record.pm_value, self.log_handler.records)))

    def test_logs_on_single_row_table(self):
        dataset = CardoDataFrame(self.context.spark.createDataFrame([['a']], 'col1: string'))
        acc_test = StepAccumulatorRuntimeTest(lambda x: x, 'unittest')
        acc_test.test(self.context, dataset).dataframe.collect()
        self.assertEqual(1, sum(map(lambda record: record.pm_value, self.log_handler.records)))

    def test_table_with_multiple_partitions(self):
        dataset = CardoDataFrame(self.context.spark.createDataFrame([['a'], ['b']], 'col1: string'))
        dataset.dataframe = dataset.dataframe.repartition(10)
        acc_test = StepAccumulatorRuntimeTest(lambda x: x, 'unittest')
        acc_test.test(self.context, dataset).dataframe.collect()
        self.assertEqual(2, sum(map(lambda record: record.pm_value, self.log_handler.records)))

    def test_condition_runtime_test(self):
        dataset = CardoDataFrame(self.context.spark.createDataFrame([['a'], ['b']], 'col1: string'))
        acc_test = StepAccumulatorRuntimeTest(lambda x: x == 'a', 'unittest')
        acc_test.test(self.context, dataset).dataframe.collect()
        self.assertEqual(1, sum(map(lambda record: record.pm_value, self.log_handler.records)))

    def test_rdd_accumulation_with_rows_without_special_columns(self):
        dataset = CardoDataFrame(self.context.spark.sparkContext.parallelize([Row(a='asdasd')]))
        acc_test = StepAccumulatorRuntimeTest(lambda x: x, 'unittest')
        acc_test.test(self.context, dataset).rdd.collect()
        self.assertEqual(1, sum(map(lambda record: record.pm_value, self.log_handler.records)))

    def test_rdd_accumulation_with_rows_with_special_columns(self):
        dataset = CardoDataFrame(self.context.spark.sparkContext.parallelize([Row(a='asdasd')]))
        acc_test = StepAccumulatorRuntimeTest(lambda x: x == 'asdasd', 'unittest', columns=['a'])
        acc_test.test(self.context, dataset).rdd.collect()
        self.assertEqual(1, sum(map(lambda record: record.pm_value, self.log_handler.records)))

    def test_rdd_accumulation_without_rows_without_special_columns(self):
        dataset = CardoDataFrame(self.context.spark.sparkContext.parallelize(['1']))
        acc_test = StepAccumulatorRuntimeTest(lambda x: x, 'unittest')
        acc_test.test(self.context, dataset).rdd.collect()
        self.assertEqual(1, sum(map(lambda record: record.pm_value, self.log_handler.records)))
