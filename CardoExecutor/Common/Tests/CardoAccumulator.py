import uuid

from pyspark import Accumulator


class CardoAccumulator(Accumulator):
	def __init__(self, logger, name, *args):
		id = str(uuid.uuid1())
		super(CardoAccumulator, self).__init__(id, *args)
		self.name = '_'.join([name, id])
		self.logger = logger.getChild('pm')
		self.log_level = 35  # See documentation
		self.logger.log(self.log_level, None, extra={'pm_id': self.name, 'pm_value': 0})

	def add(self, term):
		if term > 0:
			self.logger.log(self.log_level, None, extra={'pm_id': self.name, 'pm_value': term})
		return Accumulator.add(self, term)
