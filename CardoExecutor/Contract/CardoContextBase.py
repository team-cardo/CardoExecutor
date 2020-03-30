class CardoContextBase(object):
	def __init__(self):
		self.logger = None
		self.spark = None
		self.run_id = None
		self.debug = None

	def deepcopy(self):
		copy = CardoContextBase()
		copy.logger = self.logger
		copy.spark = self.spark
		copy.run_id = self.run_id
		copy.debug = self.debug
		return copy
