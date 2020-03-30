import logging


class MockLoggingHandler(logging.Handler):
	def __init__(self, *args, **kwargs):
		self.reset()
		super(MockLoggingHandler, self).__init__(*args, **kwargs)

	def emit(self, record):
		self.records.append(record)

	def reset(self):
		self.records = []
