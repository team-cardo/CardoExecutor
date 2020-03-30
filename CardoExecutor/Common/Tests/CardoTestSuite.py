import inspect
import os
import sys

from CardoExecutor.Common.Tests import CardoTestCase


class CardoTestSuite(object):
	def __init__(self, pattern='*Tests.py', **kwargs):
		self.pattern = pattern
		self.kwargs = kwargs

	def _module_name_from_stack(self, stack):
		# type: ([object]) -> str
		calling_module_name = stack[1][1]
		return os.path.split(calling_module_name)[-1].split('.')[0]

	def create_test_suite(self):
		def load_tests(loader, tests, pattern):
			CardoTestCase.CONTEXT_CONFIG.update(self.kwargs)
			pattern = self.pattern or pattern
			if len(tests._tests) == 0:
				tests = loader.discover(os.getcwd(), pattern)
			return tests

		calling_module = self._module_name_from_stack(inspect.stack())
		vars(sys.modules[calling_module])['load_tests'] = load_tests
