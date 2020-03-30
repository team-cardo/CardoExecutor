import itertools
from collections import Sequence

import networkx as nx

from CardoExecutor.Common.CardoGraph import CardoGraph
from CardoExecutor.Contract.ISubWorkflow import ISubWorkflow
from CardoExecutor.Contract.IWorkflow import IWorkflow


class DagWorkflow(IWorkflow):
	def __init__(self, name=""):
		super(DagWorkflow, self).__init__(name)
		self.dag = CardoGraph()
		self.edge_order_index = 0

	def __sort_by_weight(self, node, connected):
		# type: (object, list) -> list
		weights = {other_node: self.dag.get_edge_data(node, other_node) or self.dag.get_edge_data(other_node, node) for
				   other_node in connected}
		weights = {key: (weights[key].get('weight', 0), weights[key].get('edge_index', 0)) for key in weights}
		return sorted(list(connected), key=lambda other_node: weights[other_node])

	def __get_order_index(self):
		self.edge_order_index += 1
		return self.edge_order_index

	def get_before(self, step=None):
		# type: (object) -> list
		if step is None:
			return [node for node in self.dag.nodes() if self.dag.out_degree(node) == 0]
		else:
			predecessors = list(self.dag.predecessors(step))
			return self.__sort_by_weight(step, predecessors)

	def to_list(self):
		# type: () -> list
		return list(nx.algorithms.topological_sort(self.dag))

	def get_after(self, step=None):
		# type: (object) -> list
		if step is None:
			return [node for node in self.dag.nodes() if self.dag.in_degree(node) == 0]
		else:
			successors = list(self.dag.successors(step))
			return self.__sort_by_weight(step, successors)

	def append_workflow(self, subworkflow, before_steps=None, after_steps=None):
		# type: (ISubWorkflow, Sequence, Sequence) -> None
		edges = []

		self.dag = nx.compose(self.dag, subworkflow.dag)
		if before_steps is not None:
			production = list(itertools.product(before_steps, subworkflow.source_steps))
			edges += [tuple + (before_steps.index(tuple[0]),) for tuple in production]
		if after_steps is not None:
			production = list(itertools.product(subworkflow.end_steps, after_steps))
			edges += [tuple + (after_steps.index(tuple[1]),) for tuple in production]
		self.dag.add_weighted_edges_from(edges, edge_index=self.__get_order_index())

	def add_after(self, steps, after=()):
		# type: (Sequence, Sequence) -> None
		self.dag.add_nodes_from(steps)
		edges = list(itertools.product(after, steps))
		edge_indexes = {edge: (after.index(edge[0]), steps.index(edge[1])) for edge in edges}
		edges_sorted = sorted(edges, key=lambda edge: edge_indexes[edge])
		edges_with_weight = [edge + (index,) for index, edge in enumerate(edges_sorted)]
		self.dag.add_weighted_edges_from(edges_with_weight, edge_index=self.__get_order_index())

	def add_last(self, *steps):
		# type: (*object) -> None
		self.add_after(steps, after=self.get_before())
