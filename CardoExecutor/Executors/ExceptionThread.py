import threading


class ExceptionThread(threading.Thread):
	def __init__(self, target, args):
		self.args = args
		self.target = target
		super(ExceptionThread, self).__init__(target=target, args=args)
		self.exception = None

	def run(self):
		try:
			self.target(*self.args)
		except Exception as e:
			self.exception = e
		finally:
			del self.target, self.args
