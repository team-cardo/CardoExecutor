import logging
import unittest
from time import sleep, time
from functools import reduce

from CardoExecutor.Contract.CardoContextBase import CardoContextBase
from CardoExecutor.Contract.IStep import IStep
from CardoExecutor.Contract.IWorkflow import IWorkflow
from CardoExecutor.Executors.AsyncWorkflowExecutor import AsyncWorkflowExecutor
from CardoExecutor.Executors.UnitTests.MockRuntimeTestableStep import MockRuntimeTestableStep


class AsyncWorkflowExecutorTests(unittest.TestCase):
	def __get_mock_dataframe(self, value):
		class CardoDataFrameMock:
			def __init__(self, table, name):
				self.dataframe, self.table_name = table, name

			def deepcopy(self):
				return CardoDataFrameMock(self.dataframe, self.table_name)

		return CardoDataFrameMock(value, 'mock')

	def __get_mock_context(self):
		cardo_context = CardoContextBase()
		cardo_context.logger = logging.getLogger(self.__class__.__name__)
		cardo_context.logger.addHandler(logging.NullHandler())
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
		workflow.get_before = lambda step: (steps[self.__find_index(step, steps) - 1] if self.__find_index(step, steps) - 1 >= 0 else []) if step is not None else steps[-1]
		workflow.get_after = lambda step: (steps[self.__find_index(step, steps) + 1] if self.__find_index(step, steps) + 1 < len(steps) else []) if step is not None else steps[0]
		return workflow

	def test_linear_workflow(self):
		# Arrange
		steps_count = 2
		linear_workflow = self.__get_mock_workflow([[self.__get_mock_step(self.__get_mock_dataframe(i))] for i in range(steps_count)])
		cardo_context = self.__get_mock_context()
		workflow_executor = AsyncWorkflowExecutor(MockRuntimeTestableStep)

		# Act
		result = workflow_executor.execute(linear_workflow, cardo_context)

		# Assert
		self.assertTrue(result[0].dataframe == steps_count - 1)

	def test_dag_workflow(self):
		# Arrange
		steps_count = 4
		dag_steps = [
			[self.__get_mock_step(self.__get_mock_dataframe(i)) for i in range(steps_count)],
			[self.__get_mock_step(process_function=(lambda context, *args: self.__get_mock_dataframe(sum(map(lambda arg: arg.dataframe, args)))))]
		]
		workflow = self.__get_mock_workflow(dag_steps)
		cardo_context = self.__get_mock_context()
		workflow_executor = AsyncWorkflowExecutor(MockRuntimeTestableStep)

		# Act
		results = workflow_executor.execute(workflow, cardo_context)

		# Assert
		self.assertTrue(results[0].dataframe == sum(range(steps_count)))

	def test_dag_workflow_with_multiple_results_step(self):
		# Arrange
		steps_count = 4
		dag_steps = [
			[self.__get_mock_step([self.__get_mock_dataframe(i) for i in range(steps_count)])],
			map(lambda i: self.__get_mock_step(process_function=(lambda context, df: self.__get_mock_dataframe(df.dataframe * i))), range(steps_count)),
		]
		workflow = self.__get_mock_workflow(dag_steps)
		cardo_context = self.__get_mock_context()
		workflow_executor = AsyncWorkflowExecutor(MockRuntimeTestableStep)

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
		workflow_executor = AsyncWorkflowExecutor(MockRuntimeTestableStep)

		# Assert
		with self.assertRaises(ValueError):
			workflow_executor.execute(workflow, cardo_context)

	def test_workflow_concurrency(self):
		steps_count = 4
		sleep_time = 3
		def process(context, dataframe):
			sleep(sleep_time)
			return self.__get_mock_dataframe(6)

		steps = [
			[self.__get_mock_step(self.__get_mock_dataframe(6))],
			[self.__get_mock_step(process_function=process) for i in range(steps_count)],
			[self.__get_mock_step(self.__get_mock_dataframe(6))]
		]
		workflow = self.__get_mock_workflow(steps)
		cardo_context = self.__get_mock_context()
		workflow_executor = AsyncWorkflowExecutor(MockRuntimeTestableStep)

		# Act
		start_time = time()
		results = workflow_executor.execute(workflow, cardo_context)
		end_time = time()

		# Assert
		self.assertLess(end_time - start_time, sleep_time * steps_count - 1)