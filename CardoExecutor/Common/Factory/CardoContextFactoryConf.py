import copy


class CardoContextFactoryConf(dict):
	def __init__(self, **kwargs):
		super(CardoContextFactoryConf, self).__init__(copy.deepcopy(kwargs))

	def __getattr__(self, item):
		if item in self.keys():
			return self[item]
