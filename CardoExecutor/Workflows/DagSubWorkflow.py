from CardoExecutor.Contract.ISubWorkflow import ISubWorkflow
from CardoExecutor.Workflows.DagWorkflow import DagWorkflow


class DagSubWorkflow(DagWorkflow, ISubWorkflow):
	def __init__(self, *args, **kwargs):
		super(DagSubWorkflow, self).__init__(*args, **kwargs)
		self.__source_steps = None
		self.__end_steps = None

	@property
	def source_steps(self):
		return self.__source_steps or super(DagSubWorkflow, self).get_after()

	@source_steps.setter
	def source_steps(self, value):
		self.__source_steps = value

	@property
	def end_steps(self):
		return self.__end_steps or super(DagSubWorkflow, self).get_before()

	@end_steps.setter
	def end_steps(self, value):
		self.__end_steps = value