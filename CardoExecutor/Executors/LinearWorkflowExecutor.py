from pyspark.serializers import cloudpickle as pickle
import time

from CardoExecutor.Common.CardoDataFrame import CardoDataFrame
from CardoExecutor.Common.Tests.RuntimeTestableStep import RuntimeTestableStep
from CardoExecutor.Contract.CardoContextBase import CardoContextBase
from CardoExecutor.Contract.IStep import step_to_hash_dict
from CardoExecutor.Contract.IWorkflow import IWorkflow
from CardoExecutor.Contract.IWorkflowExecutor import IWorkflowExecutor
import hashlib


class LinearWorkflowExecutor(IWorkflowExecutor):
	def __init__(self, test_wrapper=RuntimeTestableStep):
		self.test_wrapper = test_wrapper

	def __time_and_execute_step(self, step, cardo_context, dependencies):
		step_name = step.__class__.__name__
		cardo_context = cardo_context.deepcopy()
		cardo_context.logger = cardo_context.logger.getChild(step_name)
		cardo_context.logger.info("Executing step `{step}`".format(step=step_name))
		try:
			result, runtime = self.__time_function(self.test_wrapper(step).process, cardo_context, *dependencies)
			cardo_context.logger.info(
				"Step `{step}` finished, took {runtime} sec".format(step=step_name, runtime=runtime),
				extra={"step_runtime": runtime})
			return result
		except Exception as error:
			cardo_context.logger.error(
				u"Step `{step}` failed due to exception {error}".format(step=step_name, error=error), exc_info=error)
			raise

	def __get_dependency_results(self, cardo_context, dependencies, step, steps_results, workflow):
		dependencies_results = []
		for dependency in dependencies:
			dependency_result = self.__execute_step_by_dependencies(cardo_context, workflow, steps_results, dependency)
			if isinstance(dependency_result, list) or isinstance(dependency_result, tuple):
				if len(workflow.get_after(dependency)) > 1:
					dependency_result = dependency_result[workflow.get_after(dependency).index(step)]
					dependencies_results.append(dependency_result)
				else:
					dependencies_results.extend(dependency_result)
			else:
				dependencies_results.append(dependency_result)
		return dependencies_results

	def __execute_step_by_dependencies(self, cardo_context, workflow, steps_results, step):
		if step in steps_results:
			return steps_results[step]
		else:
			dependencies = workflow.get_before(step)
			self.__calc_step_hash(dependencies, step)
			dependencies_results = self.__get_dependency_results(cardo_context, dependencies, step, steps_results,
			                                                     workflow)
			dependencies_results = map(lambda cardo_dataframe: cardo_dataframe.deepcopy(), dependencies_results)
			steps_results[step] = self.__time_and_execute_step(step, cardo_context, dependencies_results)

	def __calc_step_hash(self, dependencies, step):
		step_dict = step.__dict__.copy()
		step_dict.pop('name', None)
		step_dict.pop('recompute', None)
		step_dict.pop('load_delta_limit', None)
		md5_hash = hashlib.md5(pickle.dumps((step.__class__, step_dict)))
		for dependency in dependencies:
			md5_hash.update(step_to_hash_dict.get(dependency))
		step_to_hash_dict[step] = md5_hash.digest()

	def __execute_all_steps(self, cardo_context, workflow):
		steps_results = {}
		try:
			for step in workflow.to_list():
				self.__execute_step_by_dependencies(cardo_context, workflow, steps_results, step)
		except Exception as e:
			cardo_context.logger.fatal(
				u"Failed to run workflow `{workflow_name}` due to exception: {e}".format(workflow_name=workflow.name,
				                                                                         e=e), exc_info=e)
			raise
		return steps_results

	def __time_function(self, function, *args):
		start_time = time.time()
		result = function(*args)
		end_time = time.time()
		return result, end_time - start_time

	def __log_workflow_start(self, cardo_context, workflow):
		cardo_context.logger.info(
				"Starting to execute workflow `{workflow_name}` with {num_steps} steps".format(num_steps=len(workflow.to_list()),
																							   workflow_name=workflow.name))
		cardo_context.logger.debug("Workflow plan: {}".format(str(workflow.dag)))

	def execute(self, workflow, cardo_context):
		# type: (IWorkflow, CardoContextBase) -> [CardoDataFrame]
		cardo_context = cardo_context.deepcopy()
		cardo_context.logger = cardo_context.logger.getChild(workflow.name)
		self.__log_workflow_start(cardo_context, workflow)
		results, runtime = self.__time_function(self.__execute_all_steps, cardo_context, workflow)
		cardo_context.logger.info(
			"Finished workflow `{workflow_name}`, took {runtime} sec".format(workflow_name=workflow.name,
			                                                                 runtime=runtime),
			extra={"workflow_runtime": runtime})
		return map(lambda step: results[step], workflow.get_before(None))
