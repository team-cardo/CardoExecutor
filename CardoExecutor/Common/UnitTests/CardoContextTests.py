import os
import unittest
from pyspark.streaming import StreamingContext

from CardoExecutor.Common.CardoContext import CardoContext


class CardoContextTests(unittest.TestCase):
	def test_create_spark(self):
		# Arrange
		spark_conf = {
			"spark.app.name": "TEST",
			"spark.master": "local",
			"spark.cores.max": "1",
			"spark.eventLog.enabled": "true"
		}

		logging_conf = {
			"version": 1,
			"formatters": {
				"standard": {
					"format": "%(asctime)s | %(name)s | %(levelname)s | %(thread)d | %(message)s"
				}
			},
			"handlers": {
				"default": {
					"level": "DEBUG",
					"formatter": "standard",
					"class": "logging.NullHandler",
				}
			},
			"loggers": {
				'': {
					"handlers": ["default"],
				}
			}
		}

		# Act
		context = CardoContext(spark_config=spark_conf, log_config=logging_conf)

		# Assert
		self.assertIsNotNone(context.spark)
		self.assertIsNotNone(context.logger)
		context.spark.stop()
		CardoContext.destroy()

	def test_debug_mode(self):
		# Arrange
		spark_conf = {
			"spark.app.name": "TEST",
			"spark.master": "local",
			"spark.cores.max": "1",
			"spark.eventLog.enabled": "true"
		}
		os.system("rm -f ./dep.zip")

		# Act
		context = CardoContext(spark_config=spark_conf, log_config={"version": 1}, debug=True)

		# Assert
		self.assertTrue(os.path.isfile("dep.zip"))
		context.spark.stop()
		CardoContext.destroy()

	def test_stream_mode(self):
		spark_conf = {
			"spark.app.name": "TEST",
			"spark.master": "local",
			"spark.cores.max": "1",
			"spark.eventLog.enabled": "true"
		}
		context = CardoContext(spark_config=spark_conf, log_config={"version": 1}, stream=True, stream_duration=60)

		self.assertIsInstance(context.streaming_context, StreamingContext)
		context.spark.stop()
		CardoContext.destroy()

	def test_singleton(self):
		spark_conf = {
			"spark.app.name": "TEST",
			"spark.master": "local",
			"spark.cores.max": "1",
			"spark.eventLog.enabled": "true"
		}
		context1 = CardoContext(spark_config=spark_conf, log_config={"version": 1})
		context2 = CardoContext(spark_config=spark_conf, log_config={"version": 1})

		self.assertEqual(context1.run_id, context2.run_id)
		context1.spark.stop()
		CardoContext.destroy()
