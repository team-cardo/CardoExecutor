import unittest

from CardoExecutor.Workflows.DagSubWorkflow import DagSubWorkflow
from CardoExecutor.Workflows.DagWorkflow import DagWorkflow


class DagWorkflowTests(unittest.TestCase):
	def test_get_before(self):
		# Arrange
		workflow = DagWorkflow()
		workflow.dag.add_nodes_from(range(4))
		workflow.dag.add_weighted_edges_from([(0, 2, 1), (1, 2, 0), (2, 3, 0)])

		# Act
		before_step = workflow.get_before(2)

		# Assert
		self.assertListEqual(before_step, [1, 0])

	def test_get_after(self):
		# Arrange
		workflow = DagWorkflow()
		workflow.dag.add_nodes_from(range(4))
		workflow.dag.add_weighted_edges_from([(0, 1, 2), (0, 2, 0), (0, 3, 1)])

		# Act
		after_step = workflow.get_after(0)

		# Assert
		self.assertListEqual(after_step, [2, 3, 1])

	def test_get_list(self):
		# Arrange
		workflow = DagWorkflow()
		workflow.dag.add_nodes_from(range(5))
		workflow.dag.add_weighted_edges_from([(0, 2, 1), (1, 2, 0), (2, 3, 0)])

		# Act
		as_list = workflow.to_list()

		# Assert
		self.assertItemsEqual(as_list, workflow.dag.nodes())
		pass

	def test_after_none(self):
		# Arrange
		workflow = DagWorkflow()
		workflow.dag.add_nodes_from(range(4))
		workflow.dag.add_weighted_edges_from([(0, 1, 2), (0, 2, 0), (2, 3, 1)])

		# Act
		after_none = workflow.get_after(None)

		# Assert
		self.assertListEqual(after_none, [0])

	def test_before_none(self):
		# Arrange
		workflow = DagWorkflow()
		workflow.dag.add_nodes_from(range(4))
		workflow.dag.add_weighted_edges_from([(0, 1, 2), (0, 2, 0), (2, 3, 1)])

		# Act
		before_none = workflow.get_before()

		# Assert
		self.assertItemsEqual(before_none, [1, 3])

	def test_append_workflows(self):
		# Arrange
		workflow = DagWorkflow()
		workflow.dag.add_nodes_from(range(4))
		workflow.dag.add_edges_from([(0, 1), (0, 2)])
		append_workflow = DagSubWorkflow()
		append_workflow.dag.add_nodes_from(range(5, 8))
		append_workflow.dag.add_edges_from([(5, 7), (6, 7)])

		# Act
		workflow.append_workflow(append_workflow, before_steps=[2], after_steps=[3])

		# Assert
		self.assertItemsEqual(workflow.dag.edges(), [(0, 1), (0, 2), (2, 6), (2, 5), (6, 7), (5, 7), (7, 3)])

	def test_add_functions(self):
		# Arrange
		workflow = DagWorkflow()

		# Act
		workflow.add_after([0,])
		workflow.add_last(1, 2)
		workflow.add_after((3,), after=(1, 2))

		# Assert
		self.assertItemsEqual(workflow.dag.edges(), [(0, 1), (0, 2), (1, 3), (2, 3)])

	def test_sets_right_weights_on_multiple_steps_after_single_step(self):
		# Arrange
		workflow = DagWorkflow()

		# Act
		workflow.add_after((0, 1), (2,))

		# Assert
		self.assertItemsEqual(workflow.dag.edges(), [(2, 0), (2, 1)])
		self.assertEqual(workflow.dag.get_edge_data(2, 0)['weight'], 0)
		self.assertEqual(workflow.dag.get_edge_data(2, 1)['weight'], 1)

	def test_sets_right_weights_on_multiple_steps_before_single_step(self):
		# Arrange
		workflow = DagWorkflow()

		# Act
		workflow.add_after((2,), (0, 1))

		# Assert
		self.assertItemsEqual(workflow.dag.edges(), [(0, 2), (1, 2)])
		self.assertEqual(workflow.dag.get_edge_data(0, 2)['weight'], 0)
		self.assertEqual(workflow.dag.get_edge_data(1, 2)['weight'], 1)
