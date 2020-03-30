from pyspark import RDD, StorageLevel
from pyspark.sql import DataFrame, SparkSession
import copy

class CardoDataFrame(object):
	def __init__(self, dataset=None, table_name='', dataframe=None, rdd=None, pandas=None, payload=None):
		self.table_name = table_name
		self._dataframe = None
		self._rdd = None
		self._pandas = None
		self.payload = payload or {}
		if dataset is None:
			dataset = dataframe or rdd or pandas
		if isinstance(dataset, DataFrame):
			self._dataframe = dataset
		elif isinstance(dataset, RDD):
			self._rdd = dataset
		elif self.__is_pandas_dataframe(dataset):
			self._pandas = dataset
		self._persisted = []

	def __is_pandas_dataframe(self, dataset):
		# import is here because pandas installation isn't trivial and therefore we can't assume it is installed
		# By importing here, we avoid the "missing package" error.
		import pandas as pd
		return isinstance(dataset, pd.DataFrame)

	@property
	def payload_type(self):
		# type: () -> str
		if self._dataframe is not None:
			return 'dataframe'
		if self._rdd is not None:
			return 'rdd'
		if self._pandas is not None:
			return 'pandas'

	@property
	def dataframe(self):
		# type: () -> DataFrame
		if self._dataframe is None:
			if self._rdd is not None:
				self._dataframe = self._rdd.toDF()
			if self._pandas is not None:
				self._dataframe = SparkSession.builder.getOrCreate().createDataFrame(self._pandas)
			self._rdd = None
			self._pandas = None
		return self._dataframe

	@property
	def rdd(self):
		# type: () -> RDD
		if self._rdd is None:
			self._rdd = self.dataframe.rdd
			self._dataframe = None
			self._pandas = None
		return self._rdd

	@property
	def pandas(self):
		# type: () -> pd.DataFrame
		if self._pandas is None:
			self._pandas = self.dataframe.toPandas()
			self._dataframe = None
			self._rdd = None
		return self._pandas

	def persist(self, storage_level=StorageLevel.MEMORY_AND_DISK):
		persistable_object = getattr(self, self.payload_type)
		if persistable_object is not None:
			if hasattr(persistable_object, 'persist'):
				persistable_object.persist(storageLevel=storage_level)
				self._persisted.append(persistable_object)
		return self

	def unpersist(self, **kwargs):
		blocking = kwargs.get("blocking")
		for persistable in self._persisted:
			if blocking is not None:
				persistable.unpersist(blocking)
			else:
				persistable.unpersist()

	@dataframe.setter
	def dataframe(self, value):
		self._dataframe = value
		self._pandas = None
		self._rdd = None

	@rdd.setter
	def rdd(self, value):
		self._rdd = value
		self._dataframe = None
		self._pandas = None

	@pandas.setter
	def pandas(self, value):
		self._pandas = value
		self._dataframe = None
		self._rdd = None

	def deepcopy(self):
		return CardoDataFrame(None, self.table_name, self._dataframe, self._rdd, copy.deepcopy(self._pandas))
