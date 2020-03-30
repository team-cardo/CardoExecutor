import uuid
from logging import Logger

from CardoExecutor.Common.CardoContext import CardoContext
from CardoExecutor.Common.Factory.CardoContextFactoryConf import CardoContextFactoryConf
from CardoExecutor.Common.Factory.CardoContextFactoryLoggings import CardoContextFactoryLoggings
from CardoExecutor.Common.Factory.Templates.spark_multiple_hadoops_config_template import set_multiple_hadoops
from CardoExecutor.Common.Factory.Templates.spark_auto_queue_template import set_auto_queue
from CardoExecutor.Common.Factory.Templates.elastic_logging_config_template import get_config as get_logging_config
from CardoExecutor.Common.Factory.Templates.spark_config_template import get_config


class CardoContextFactory(object):
	def create_context(self, app_name, max_cores=1, executor_cores=1, executor_memory="5gb", append_spark_config=None,
					   environment="your_env1", append_log_config=None, partitions=200, logger=None, stream=False,
					   stream_interval=None, cluster="your_env2", multiple_hadoops=None, hive=None, master_number=1,
					   auto_queue=False):
		# type: (str, int, int, str, dict, str, dict, int, Logger, bool, int, str, bool, str, int, bool) -> CardoContext
		context_conf = CardoContextFactoryConf(**{key: value for key, value in locals().items() if key != 'self'})
		append_spark_config = append_spark_config or dict()
		append_log_config = append_log_config or dict()
		context_conf.run_id = str(uuid.uuid1()).lower()
		self.__logger = CardoContextFactoryLoggings()
		context = self.__create_context(context_conf)
		return context

	def __create_context(self, context_conf):
		spark_config = self.__get_spark_config(context_conf)
		log_config = self.__get_log_config(context_conf)
		context = CardoContext(spark_config=spark_config, log_config=log_config, debug=(context_conf.environment == 'dev'),
							   logger=context_conf.logger, run_id=context_conf.run_id, stream=context_conf.stream,
							   stream_duration=context_conf.stream_interval)
		self.__logger.log_about_creation(context)
		self.__logger.log_experimental_features(context, context_conf)
		return context

	def __get_spark_config(self, context_conf):
		# type: (CardoContextFactoryConf) -> dict
		if context_conf.environment.lower() not in ('prod', 'production'):
			context_conf.app_name = "{environment}_{app_name}".format(environment=context_conf.environment.upper(),
			                                                          app_name=context_conf.app_name)
			spark_config = get_config(context_conf.cluster, context_conf.app_name, context_conf.executor_cores,
			                          context_conf.executor_memory, context_conf.max_cores, context_conf.partitions)
			spark_config.update(context_conf.append_spark_config or {})
			self.__set_experimental_parameters(spark_config, context_conf)
			return spark_config
		return {}

	def __get_log_config(self, context_conf):
		# type: (CardoContextFactoryConf) -> dict
		logging_level = "NOTSET" if context_conf.environment == "dev" else "INFO"
		log_config = get_logging_config(environment=context_conf.environment, app_name=context_conf.app_name, level=logging_level,
										run_id=context_conf.run_id)
		log_config.update(context_conf.append_log_config or {})
		return log_config

	def __set_experimental_parameters(self, spark_config, context_conf):
		spark_config.update(set_auto_queue(context_conf.auto_queue))
		spark_config.update(set_multiple_hadoops(context_conf.multiple_hadoops, context_conf.hive, context_conf.master_number))
