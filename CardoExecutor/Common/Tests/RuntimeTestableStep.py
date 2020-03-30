import inspect

from CardoExecutor.Common.Tests.StepAccumulatorRuntimeTest import StepAccumulatorRuntimeTest
from CardoExecutor.Contract.IStepRuntimeWrapper import IStepRuntimeWrapper

DEFAULT_TESTS = []


class RuntimeTestableStep(IStepRuntimeWrapper):
	def __init__(self, step, default_tests=DEFAULT_TESTS):
		super(RuntimeTestableStep, self).__init__(step)
		self.step = step
		self.before_step_tests, self.after_step_tests = self.get_step_tests(step)
		self.default_tests = default_tests

	def get_step_tests(self, step):
		before_tests, after_tests = {}, {}
		for method in dict(inspect.getmembers(step)).values():
			if inspect.ismethod(method) and '__pm__' in method.__dict__:
				columns, name, before_or_after, cardo_dataframe_index, kwargs = method.__dict__['__pm__']
				step_test = StepAccumulatorRuntimeTest(method, name, columns, **kwargs)
				if before_or_after == 'before':
					before_tests[cardo_dataframe_index] = before_tests.get(cardo_dataframe_index, []) + [step_test]
				if before_or_after == 'after':
					after_tests[cardo_dataframe_index] = after_tests.get(cardo_dataframe_index, []) + [step_test]
		return before_tests, after_tests

	def __execute_tests(self, cardo_context, cardo_dataframe, tests_list):
		for test in tests_list:
			cardo_dataframe = test.test(cardo_context, cardo_dataframe)
		return cardo_dataframe

	def __execute_tests_for_dataframes(self, cardo_context, cardo_dataframes, tests_dict, default_tests=None):
		if default_tests is None:
			default_tests = []
		cardo_dataframes = list(cardo_dataframes)
		for i in range(len(cardo_dataframes)):
			tests = tests_dict.get(None, []) + tests_dict.get(i, []) + default_tests
			cardo_dataframes[i] = self.__execute_tests(cardo_context, cardo_dataframes[i], tests)
		return cardo_dataframes

	def process(self, cardo_context, *cardo_dataframes):
		cardo_dataframes = self.__execute_tests_for_dataframes(cardo_context, cardo_dataframes, self.before_step_tests)
		result = self.step.process(cardo_context, *cardo_dataframes)
		if isinstance(result, list) or isinstance(result, tuple):
			result = self.__execute_tests_for_dataframes(cardo_context, result, self.after_step_tests, self.default_tests)
		else:
			result = self.__execute_tests_for_dataframes(cardo_context, [result], self.after_step_tests, self.default_tests)[0]
		return result
