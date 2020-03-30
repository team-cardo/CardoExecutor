import unittest

from CardoExecutor.Contract.IStep import IStep
from CardoExecutor.Workflows.SimpleWorkflow import SimpleWorkflow


class SimpleWorkflowTests(unittest.TestCase):
	def dummy_step(self):
		return IStep()

	def test_to_list(self):
		steps_list = [self.dummy_step(), self.dummy_step()]
		workflow = SimpleWorkflow('', steps_list)
		self.assertListEqual(steps_list, workflow.to_list())

	def test_get_before_in_middle(self):
		steps_list = [self.dummy_step(), self.dummy_step()]
		workflow = SimpleWorkflow('', steps_list)
		self.assertListEqual([steps_list[0]], workflow.get_before(steps_list[1]))

	def test_get_before_in_beginning(self):
		steps_list = [self.dummy_step(), self.dummy_step()]
		workflow = SimpleWorkflow('', steps_list)
		self.assertListEqual([], workflow.get_before(steps_list[0]))

	def test_get_after_in_beginning(self):
		steps_list = [self.dummy_step(), self.dummy_step()]
		workflow = SimpleWorkflow('', steps_list)
		self.assertListEqual([steps_list[1]], workflow.get_after(steps_list[0]))

	def test_get_after_in_middle(self):
		steps_list = [self.dummy_step(), self.dummy_step()]
		workflow = SimpleWorkflow('', steps_list)
		self.assertListEqual([], workflow.get_after(steps_list[1]))