import logging
import unittest
from functools import reduce

from CardoExecutor.Common.UnitTests.MockLoggingHandler import MockLoggingHandler
from CardoExecutor.Contract.CardoContextBase import CardoContextBase
from CardoExecutor.Contract.IStep import IStep
from CardoExecutor.Contract.IWorkflow import IWorkflow
from CardoExecutor.Executors.LinearWorkflowExecutor import LinearWorkflowExecutor
from CardoExecutor.Executors.UnitTests.MockRuntimeTestableStep import MockRuntimeTestableStep


class LinearWorkflowExecutorTests(unittest.TestCase):
	@classmethod
	def setUpClass(cls):
		cls.log_handler = MockLoggingHandler()

	def __get_mock_dataframe(self, value):
		class CardoDataFrameMock:
			def __init__(self, table, name):
				self.dataframe, self.table_name = table, name

			def deepcopy(self):
				return CardoDataFrameMock(self.dataframe, self.table_name)

		return CardoDataFrameMock(value, 'mock')

	def __get_mock_context(self):
		cardo_context = CardoContextBase()
		cardo_context.logger = logging.getLogger()
		cardo_context.logger.handlers = []
		cardo_context.logger.addHandler(self.log_handler)
		return cardo_context

	def __get_mock_step(self, return_value=None, process_function=None):
		generic_step = IStep()
		generic_step.process = process_function or (lambda *args: return_value)
		return generic_step

	def __find_index(self, step_to_find, steps):
		index = 0
		for step in steps:
			if step_to_find in step:
				return index
			index += 1
		return None

	def __get_mock_workflow(self, steps):
		workflow = IWorkflow()
		workflow.to_list = lambda: reduce(lambda a, b: a + b, steps)
		workflow.get_before = lambda step: (
			steps[self.__find_index(step, steps) - 1] if self.__find_index(step, steps) - 1 >= 0 else []) if step is not None else steps[-1]
		workflow.get_after = lambda step: (
			steps[self.__find_index(step, steps) + 1] if self.__find_index(step, steps) + 1 < len(steps) else []) if step is not None else steps[0]
		return workflow

	def test_linear_workflow(self):
		# Arrange
		steps_count = 2
		linear_workflow = self.__get_mock_workflow([[self.__get_mock_step(self.__get_mock_dataframe(i))] for i in range(steps_count)])
		cardo_context = self.__get_mock_context()
		workflow_executor = LinearWorkflowExecutor(MockRuntimeTestableStep)

		# Act
		result = workflow_executor.execute(linear_workflow, cardo_context)

		# Assert
		self.assertTrue(result[0].dataframe == steps_count - 1)

	def test_linear_workflow_with_many_dataframes(self):
		# Arrange
		num_of_dfs = 4
		first_step = [self.__get_mock_step([self.__get_mock_dataframe(i) for i in range(num_of_dfs)])]
		second_step = [self.__get_mock_step(process_function=lambda cardo_context, *cardo_dataframes: cardo_dataframes)]
		linear_workflow = self.__get_mock_workflow([first_step, second_step])
		cardo_context = self.__get_mock_context()
		workflow_executor = LinearWorkflowExecutor(MockRuntimeTestableStep)

		# Act
		results = workflow_executor.execute(linear_workflow, cardo_context)

        # Assert
		self.assertEqual(len(results[0]), num_of_dfs)

	def test_dag_workflow(self):
		# Arrange
		steps_count = 4
		dag_steps = [
			[self.__get_mock_step(self.__get_mock_dataframe(i)) for i in range(steps_count)],
			[self.__get_mock_step(process_function=(lambda context, *args: self.__get_mock_dataframe(sum(map(lambda arg: arg.dataframe, args)))))]
		]
		workflow = self.__get_mock_workflow(dag_steps)
		cardo_context = self.__get_mock_context()
		workflow_executor = LinearWorkflowExecutor(MockRuntimeTestableStep)

		# Act
		results = workflow_executor.execute(workflow, cardo_context)

		# Assert
		self.assertTrue(results[0].dataframe == sum(range(steps_count)))

	def test_dag_workflow_with_multiple_results_step(self):
		# Arrange
		steps_count = 4
		dag_steps = [
			[self.__get_mock_step([self.__get_mock_dataframe(i) for i in range(steps_count)])],
			map(lambda i: self.__get_mock_step(process_function=(lambda context, df: self.__get_mock_dataframe(df.dataframe * i))),
				range(steps_count)),
		]
		workflow = self.__get_mock_workflow(dag_steps)
		cardo_context = self.__get_mock_context()
		workflow_executor = LinearWorkflowExecutor(MockRuntimeTestableStep)

		# Act
		results = workflow_executor.execute(workflow, cardo_context)

		# Assert
		self.assertListEqual(map(lambda res: res.dataframe, results), map(lambda x: x ** 2, range(steps_count)))

	def test_workflow_with_exception(self):
		# Arrange
		steps_count = 1

		def raise_function(*args): raise ValueError()

		dag_steps = [
			[self.__get_mock_step([self.__get_mock_dataframe(6)])],
			[self.__get_mock_step(process_function=raise_function)]
		]
		workflow = self.__get_mock_workflow(dag_steps)
		cardo_context = self.__get_mock_context()
		workflow_executor = LinearWorkflowExecutor(MockRuntimeTestableStep)

		# Assert
		self.assertRaises(workflow_executor.execute, None, workflow, cardo_context)

	def test_process_without_enough_params_raises_indicative_error(self):
		process = lambda context, dataframe: None

		dag_steps = [
			[self.__get_mock_step([self.__get_mock_dataframe(1)]), self.__get_mock_step([self.__get_mock_dataframe(1)])],
			[self.__get_mock_step(process_function=process)]
		]

		workflow = self.__get_mock_workflow(dag_steps)
		cardo_context = self.__get_mock_context()
		workflow_executor = LinearWorkflowExecutor(MockRuntimeTestableStep)

		# Assert
		try:
			workflow_executor.execute(workflow, cardo_context)
			raise RuntimeError()
		except:
			self.assertEqual(self.log_handler.records[0].name, '.IStep')
