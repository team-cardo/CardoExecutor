from CardoExecutor.Contract.RecycleStepRuntimeWrapper import *
from CardoExecutor.Executors.LinearWorkflowExecutor import LinearWorkflowExecutor


class RecycleLinearWorkflowExecutor(LinearWorkflowExecutor):
	def __init__(self, test_wrapper=RecycleStepRuntimeWrapper):
		super(RecycleLinearWorkflowExecutor, self).__init__(test_wrapper)
