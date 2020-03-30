from CardoExecutor.Contract import IStep


class IWorkflow(object):
	def __init__(self, name=""):
		self.name = name
		self.dag = None

	def to_list(self):
		raise NotImplementedError()

	def get_before(self, step):
		# type: (IStep) -> [IStep]
		raise NotImplementedError()

	def get_after(self, step):
		# type: (IStep) -> [IStep]
		raise NotImplementedError()