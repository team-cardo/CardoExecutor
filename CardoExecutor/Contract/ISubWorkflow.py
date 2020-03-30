from CardoExecutor.Contract.IWorkflow import IWorkflow


class ISubWorkflow(IWorkflow):
	@property
	def source_steps(self):
		raise NotImplementedError()

	@property
	def end_steps(self):
		raise NotImplementedError()