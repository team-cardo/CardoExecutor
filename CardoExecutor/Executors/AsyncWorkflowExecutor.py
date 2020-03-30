import threading
import time

from CardoExecutor.Common.CardoDataFrame import CardoDataFrame
from CardoExecutor.Common.Tests.RuntimeTestableStep import RuntimeTestableStep
from CardoExecutor.Contract.CardoContextBase import CardoContextBase
from CardoExecutor.Contract.IWorkflow import IWorkflow
from CardoExecutor.Contract.IWorkflowExecutor import IWorkflowExecutor
from CardoExecutor.Executors.ExceptionThread import ExceptionThread


class AsyncWorkflowExecutor(IWorkflowExecutor):
	def __init__(self, test_wrapper=RuntimeTestableStep):
		self.test_wrapper = test_wrapper

	def __execute_single_step(self, step, cardo_context, dependencies, lock, steps_results):
		step_name = step.__class__.__name__
		cardo_context = cardo_context.deepcopy()
		cardo_context.logger = cardo_context.logger.getChild(step_name)
		cardo_context.logger.info("Executing step `{step}`".format(step=step_name))
		result, runtime = self.__time_function(self.test_wrapper(step).process, cardo_context, *dependencies)
		cardo_context.logger.info("Step `{step}` finished, took {runtime} sec".format(step=step_name, runtime=runtime),
								  extra={"step_runtime": runtime})
		self.__set_result(steps_results, step, result, lock)
		return result

	def __set_result(self, steps_results, step, result, lock):
		with lock:
			steps_results[step] = result

	def __execute_first_in_queue(self, cardo_context, steps_queue, steps_results, workflow, lock):
		step = steps_queue.pop(0)
		dependencies_results = self.__get_dependencies_result(workflow, steps_results, step, lock)
		if dependencies_results is not None:
			dependencies_results = map(lambda dependency: dependency.deepcopy(), dependencies_results)
			thread = ExceptionThread(target=self.__execute_single_step,
									 args=(step, cardo_context, dependencies_results, lock, steps_results))
			thread.start()
			return thread
		else:
			steps_queue.append(step)

	def __get_dependencies_result(self, workflow, steps_results, step, lock):
		before_steps = workflow.get_before(step)
		dependencies_results = []
		for before_step in before_steps:
			with lock:
				if before_step not in steps_results:
					return None
				dependency = steps_results[before_step]
			if isinstance(dependency, list) or isinstance(dependency, tuple):
				dependency = dependency[workflow.get_after(before_step).index(step)]
			dependencies_results.append(dependency)
		return dependencies_results

	def __execute_all_steps(self, cardo_context, workflow):
		steps_queue = workflow.to_list()
		lock = threading.Lock()
		steps_results = {}
		threads = []
		try:
			while len(steps_queue) > 0:
				thread = self.__execute_first_in_queue(cardo_context, steps_queue, steps_results, workflow, lock)
				if thread is not None:
					threads.append(thread)
			for thread in threads:
				thread.join()
				if thread.exception is not None:
					raise thread.exception
		except Exception as e:
			cardo_context.logger.fatal(
					"Failed to run workflow `{workflow_name}` due to exception: {e}".format(workflow_name=workflow.name, e=e), exc_info=e)
			raise
		return steps_results

	def __time_function(self, function, *args):
		start_time = time.time()
		result = function(*args)
		end_time = time.time()
		return result, end_time - start_time

	def execute(self, workflow, cardo_context):
		# type: (IWorkflow, CardoContextBase) -> [CardoDataFrame]
		cardo_context = cardo_context.deepcopy()
		cardo_context.logger = cardo_context.logger.getChild(workflow.name)
		cardo_context.logger.info(
			"Starting to execute workflow `{workflow_name}` with {num_steps} steps".format(num_steps=len(workflow.to_list()), workflow_name=workflow.name))
		results, runtime = self.__time_function(self.__execute_all_steps, cardo_context, workflow)
		cardo_context.logger.info(
				"Finished workflow `{workflow_name}`, took {runtime} sec".format(workflow_name=workflow.name, runtime=runtime),
				extra={"workflow_runtime": runtime})
		return map(lambda step: results[step], workflow.get_before(None))
