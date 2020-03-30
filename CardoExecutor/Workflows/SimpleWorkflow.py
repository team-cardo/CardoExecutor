from copy import copy

from CardoExecutor.Contract.IWorkflow import IWorkflow


class SimpleWorkflow(IWorkflow):
	def __init__(self, name, steps=()):
		super(SimpleWorkflow, self).__init__(name)
		self.steps = steps

	def to_list(self):
		return copy(self.steps)

	def get_after(self, step):
		for i in range(0, len(self.steps)):
			if step == self.steps[i] and i < len(self.steps) - 1:
				return [self.steps[i + 1]]
		return []

	def get_before(self, step):
		for i in range(0, len(self.steps)):
			if step == self.steps[i] and i > 0:
				return [self.steps[i - 1]]
		return []
