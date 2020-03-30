from CardoExecutor.Contract.IStep import IStep


class MockRuntimeTestableStep(IStep):
	def __init__(self, step):
		self.step = step

	def process(self, cardo_context, *cardo_dataframes):
		return self.step.process(cardo_context, *cardo_dataframes)
