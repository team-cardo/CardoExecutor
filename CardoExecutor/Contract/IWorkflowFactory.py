from CardoExecutor.Contract.IWorkflow import IWorkflow


class IWorkflowFactory(object):
	def create_workflow(self, *args, **kwargs):
		# type: (object) -> IWorkflow
		raise NotImplementedError()