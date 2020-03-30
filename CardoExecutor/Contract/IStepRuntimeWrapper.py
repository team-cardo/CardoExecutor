from CardoExecutor.Contract.IStep import IStep


class IStepRuntimeWrapper(IStep):
	def __init__(self, step):
		# type: (IStep) -> IStep
		self.step = step

	def process(self, cardo_context, cardo_dataframe):
		raise NotImplementedError()
