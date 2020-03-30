class CardoColumn(object):
	def __init__(self, input_column=None, output_column=None):
		if (input_column or output_column) is None:
			raise AttributeError("one of the parameters have to be non-None")
		self.input_column = input_column or output_column
		self.output_column = output_column or input_column
