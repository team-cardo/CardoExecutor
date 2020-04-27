import logging
import logging.config
import os
import pyspark
import uuid
import zipfile
from logging import Logger
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming.context import StreamingContext

from CardoExecutor.Contract.CardoContextBase import CardoContextBase
from CardoExecutor.Contract.MetaClasses import Singleton


class CardoContext(CardoContextBase, metaclass=Singleton):
	ZIP_CREATED = False
	def __init__(self, spark_config, log_config, run_id=None, debug=False, logger=None, stream=False,
				 stream_duration=None):
		# type: (dict, dict, str, bool, Logger, bool, int) -> None
		super(CardoContext, self).__init__()
		self._set_logger(log_config, logger)
		self.run_id = run_id or uuid.uuid1()
		self.logger.info('Initiating spark')
		self._create_spark_context(spark_config, stream, stream_duration)
		self.debug = debug
		if debug:
			self.logger.info('Broadcasting project directory')
			self._broadcast_project_directory()

	def _set_logger(self, log_config, logger):
		if logger is None:
			logging.config.dictConfig(log_config)
			self.logger = logging.getLogger('main')
		else:
			self.logger = logger

	def _broadcast_project_directory(self):
		zip_file_name = 'dep.zip'
		if not CardoContext.ZIP_CREATED:
			self._zip_project_directory(zip_file_name)
			CardoContext.ZIP_CREATED = True
		self.spark.sparkContext.addPyFile(zip_file_name)

	def _zip_project_directory(self, zip_file_name):
		handle = zipfile.ZipFile(zip_file_name, 'w', zipfile.ZIP_DEFLATED)
		for root, dirs, files in os.walk(os.path.curdir):
			[self._write_to_zip(file_, handle, root) for file_ in files if file_.endswith(".py")]

	def _write_to_zip(self, file_, handle, root):
		handle.write(os.path.join(root, file_))

	def _create_spark_context(self, spark_config, stream, stream_duration):
		if stream is True:
			self.streaming_context = StreamingContext(
				SparkContext(conf=pyspark.SparkConf().setAll(spark_config.items())).getOrCreate(),
				stream_duration)
			self.spark = SparkSession(self.streaming_context.sparkContext)
		else:
			self.spark = SparkSession.builder \
				.config(conf=pyspark.SparkConf().setAll(spark_config.items())) \
				.enableHiveSupport().getOrCreate()