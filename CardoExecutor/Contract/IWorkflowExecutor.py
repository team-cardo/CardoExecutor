from CardoExecutor.Common.CardoDataFrame import CardoDataFrame
from CardoExecutor.Contract.IWorkflow import IWorkflow
from CardoExecutor.Contract.CardoContextBase import CardoContextBase


class IWorkflowExecutor(object):
	def execute(self, workflow, cardo_context):
		# type: (IWorkflow, CardoContextBase) -> [CardoDataFrame]
		raise NotImplementedError()