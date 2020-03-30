from CardoExecutor.Workflows.DagSubWorkflow import DagSubWorkflow


class Feature(DagSubWorkflow):
	def __init__(self, steps, name, recompute=None, recompute_from_step=None, feature_type=None, *args, **kwargs):
		"""
		:type steps: list of steps to run
		"""
		super(Feature, self).__init__(name, *args, **kwargs)
		self.steps = steps
		self.feature_type = feature_type
		self._add_all_steps()
		self._set_rename_last_step()
		if recompute_from_step or recompute:
			self._configure_recompute_from(recompute, recompute_from_step)

	def _add_all_steps(self):
		for step in self.steps:
			self.add_last(step)

	def _set_rename_last_step(self):
		self.steps[-1].rename = self.name

	def _configure_recompute_from(self, recompute, recompute_from_step):
		if recompute is None:
			for index, step in enumerate(self.steps):
				step.recompute = (index >= recompute_from_step)
		else:
			for step in self.steps:
				step.recompute = recompute
