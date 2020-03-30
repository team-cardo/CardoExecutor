import requests

from CardoExecutor.Common.Factory.Templates.elastic_logging_config_template import KIBANA_RUN_ID_URL, KIBANA_SHORTEN_SERVICE, \
	KIBANA_SHORTEN_SERVICE_HEADER, KIBANA_SHORTEN_RESULT_TEMPLATE


class CardoContextFactoryLoggings(object):
	def log_experimental_features(self, context, context_conf):
		if context_conf.multiple_hadoops:
			context.logger.warn('You are using experimental features of cardo - connection of multiple hadoops. DO NOT USE THIS IN PRODUCTION '
								'ENVIRONMENT.')
		if context_conf.hive:
			context.logger.warn(
					'You are using experimental features of cardo - overriding the default hive metastores. DO NOT USE THIS IN PRODUCTION ENVIRONMENT.')
		if context_conf.auto_queue:
			context.logger.info('AUTO_QUEUE: {queue}'.format(queue=context.spark.conf.get('spark.yarn.queue')))
			context.logger.warn(
					'You are using experimental features of cardo - overriding the default hive metastores. DO NOT USE THIS IN PRODUCTION ENVIRONMENT.')

	def log_about_creation(self, context):
		self.__log_web_ui_url(context)
		self.__log_kibana_location(context)

	def __log_kibana_location(self, context):
		try:
			kibana_full_url = KIBANA_RUN_ID_URL.format(run_id=context.run_id)
			kibana_shorten_url = requests.post(KIBANA_SHORTEN_SERVICE, headers=KIBANA_SHORTEN_SERVICE_HEADER, json={'url': kibana_full_url}).text
			context.logger.info('Kibana logs: {url}'.format(url=KIBANA_SHORTEN_RESULT_TEMPLATE.format(kibana_shorten_url)))
		except Exception as e:
			context.logger.warn('Cannot link kibana', exc_info=e)

	def __log_web_ui_url(self, context):
		try:
			ui_url = context.spark.sparkContext.uiWebUrl
			if context.spark.sparkContext.master == 'yarn':
				ui_url = context.spark.sparkContext._conf.get(
					'spark.org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter.param.PROXY_URI_BASES')
				ui_url = ui_url.split(',')[0]
			context.logger.info('Spark UI: {url}'.format(url=ui_url))
		except Exception as e:
			context.logger.warn('Cannot link webUI', exc_info=e)
