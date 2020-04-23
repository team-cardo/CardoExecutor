import unittest
from logging import getLogger

from CardoExecutor.Common.CardoDataFrame import CardoDataFrame
from CardoExecutor.Common.Factory.CardoContextFactory import CardoContextFactory
from CardoExecutor.Common.Tests.RuntimeTestableStep import RuntimeTestableStep
from CardoExecutor.Common.UnitTests.MockLoggingHandler import MockLoggingHandler
from CardoExecutor.Contract.IStep import IStep
from CardoExecutor.Common.CardoContext import CardoContext


class RuntimeTestableStepTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.context = CardoContextFactory().create_context('unittest', environment='dev', partitions=1,
                                                           logger=getLogger("unittests"))

    @classmethod
    def tearDownClass(cls):
        cls.context.spark.stop()
        CardoContext.destroy()

    def setUp(self):
        self.log_handler = MockLoggingHandler()
        self.context.logger.root.handlers = []
        self.context.logger.root.addHandler(self.log_handler)

    def get_pm_summary(self, pm_id):
        records = filter(lambda record: pm_id in record.pm_id, self.log_handler.records)
        return sum(map(lambda record: record.pm_value, records))

    def test_step_without_unique_tests_should_only_count(self):
        class Test(IStep):
            def process(self, cardo_context, cardo_dataframe):
                return cardo_dataframe

        dataset = CardoDataFrame(self.context.spark.createDataFrame([['a']], 'col1: string'))
        result = RuntimeTestableStep(Test()).process(self.context, dataset)
        result.dataframe.collect()
        self.assertEqual(1, self.get_pm_summary('count'))
        self.assertEqual(2, len(self.log_handler.records))

    def test_step_with_multiple_outputs_without_unique_tests_should_count_on_each(self):
        class Test(IStep):
            def process(self, cardo_context, cardo_dataframe=None):
                return [CardoDataFrame(cardo_context.spark.createDataFrame([['a']], 'col1: string')),
                        CardoDataFrame(cardo_context.spark.createDataFrame([['a']], 'col1: string'))]

        result = RuntimeTestableStep(Test()).process(self.context)
        result[0].dataframe.collect()
        result[1].dataframe.collect()
        self.assertEqual(2, self.get_pm_summary('count'))
        self.assertEqual(4, len(self.log_handler.records))

    def test_step_with_unique_after_test_also_counts(self):
        class Test(IStep):
            def process(self, cardo_context, cardo_dataframe):
                return cardo_dataframe

            @IStep.pm_output()
            def is_null(self, value):
                return value is None

        dataset = CardoDataFrame(self.context.spark.createDataFrame([['a']], 'col1: string'))
        result = RuntimeTestableStep(Test()).process(self.context, dataset)
        result.dataframe.collect()
        self.assertEqual(1, self.get_pm_summary('count'))
        self.assertEqual(0, self.get_pm_summary('is_null'))
        self.assertEqual(3, len(self.log_handler.records))

    def test_step_with_unique_before_test_also_counts(self):
        class Test(IStep):
            def process(self, cardo_context, cardo_dataframe):
                cardo_dataframe.dataframe = cardo_dataframe.dataframe.where('col1 is not null')
                return cardo_dataframe

            @IStep.pm_input()
            def is_null(self, value):
                return value is None

        dataset = CardoDataFrame(self.context.spark.createDataFrame([['a'], [None]], 'col1: string'))
        result = RuntimeTestableStep(Test()).process(self.context, dataset)
        result.dataframe.collect()
        self.assertEqual(1, self.get_pm_summary('count'))
        self.assertEqual(1, self.get_pm_summary('is_null'))
        self.assertEqual(4, len(self.log_handler.records))

    def test_step_with_unique_test_on_specific_column(self):
        class Test(IStep):
            def process(self, cardo_context, cardo_dataframe):
                cardo_dataframe.dataframe = cardo_dataframe.dataframe.where('col1 is not null')
                return cardo_dataframe

            @IStep.pm_input(['col1'])
            def is_null_before(self, value):
                return value is None

            @IStep.pm_output(['col1'])
            def is_null_after(self, value):
                return value is None

        dataset = CardoDataFrame(self.context.spark.createDataFrame([[1, 'a'], [2, None]], 'num: int, col1: string'))
        result = RuntimeTestableStep(Test()).process(self.context, dataset)
        result.dataframe.collect()
        self.assertEqual(1, self.get_pm_summary('count'))
        self.assertEqual(1, self.get_pm_summary('is_null_before'))
        self.assertEqual(0, self.get_pm_summary('is_null_after'))
        self.assertEqual(5, len(self.log_handler.records))

    def test_multiple_inputs_and_specific_test_on_specific_table(self):
        class Test(IStep):
            def process(self, cardo_context, cardo_dataframe, another_dataframe=None):
                cardo_dataframe.dataframe = cardo_dataframe.dataframe.union(another_dataframe.dataframe)
                return cardo_dataframe

            @IStep.pm_input(cardo_dataframe_index=1)
            def is_null(self, value):
                return value is None

        dataset = CardoDataFrame(self.context.spark.createDataFrame([['a']], 'col1: string'))
        another_dataset = CardoDataFrame(self.context.spark.createDataFrame([[None]], 'col1: string'))
        result = RuntimeTestableStep(Test()).process(self.context, dataset, another_dataset)
        result.dataframe.collect()
        self.assertEqual(2, self.get_pm_summary('count'))
        self.assertEqual(1, self.get_pm_summary('is_null'))
        self.assertEqual(5, len(self.log_handler.records))
