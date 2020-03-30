import pyspark.sql.functions as F
import time

from CardoExecutor.Common.CardoDataFrame import CardoDataFrame
from CardoExecutor.Contract.IStepRuntimeWrapper import IStepRuntimeWrapper


class NonLazyStepEvaluation(IStepRuntimeWrapper):
	def __init__(self, step):
		super(NonLazyStepEvaluation, self).__init__(step)

	def __time_function(self, function, *args):
		start_time = time.time()
		result = function(*args)
		end_time = time.time()
		return result, end_time - start_time

	def process(self, cardo_context, *cardo_dataframes):
		results_list = self.step.process(cardo_context, *cardo_dataframes)
		results_list = results_list if isinstance(results_list, list) or isinstance(results_list, tuple) else [results_list]
		cardo_context.logger.debug('evaluating results of step `{step_name}`'.format(step_name=self.step.__class__.__name__))
		self.__persist_and_count(cardo_context, results_list)
		cardo_context.logger.debug('showing partitions after step `{step_name}`'.format(step_name=self.step.__class__.__name__))
		self.__show_partitions_count(cardo_context, list(results_list))
		cardo_context.logger.debug('debug of step `{step_name}` was completed'.format(step_name=self.step.__class__.__name__))
		return results_list if len(results_list) > 1 else results_list[0]

	def __persist_and_count(self, cardo_context, result):
		"""
		:type result: list of CardoDataFrame
		"""
		for dataframe_index, cardo_dataframe in enumerate(result):
			if cardo_dataframe.payload_type == 'dataframe':
				dataframe = cardo_dataframe.dataframe
				dataframe.persist()
				count, run_time = self.__time_function(dataframe.count)
			if cardo_dataframe.payload_type == 'rdd':
				cardo_dataframe.rdd.persist()
				count, run_time = self.__time_function(cardo_dataframe.rdd.count)
			else:
				cardo_context.logger.debug('Cannot time {}'.format(cardo_dataframe.payload_type))
			cardo_context.logger.debug('evaluation of dataframe #{index} took {run_time}sec'.format(index=dataframe_index, run_time=run_time))

	def __show_partitions_count(self, cardo_context, result):
		"""
		:type result: list of CardoDataFrame
		"""
		for dataframe_index, cardo_dataframe in enumerate(result):
			cardo_context.logger.debug('showing partitions of dataframe #{}'.format(dataframe_index))
			if cardo_dataframe.payload_type == 'dataframe':
				partitions = cardo_dataframe.dataframe.groupBy(F.spark_partition_id()).count().collect()
				for partition in partitions:
					cardo_context.logger.debug('partition id #{}: {}'.format(partition[0], partition[1]))
			else:
				cardo_context.logger.debug('Cannot show partition status for {}'.format(cardo_dataframe.payload_type))
