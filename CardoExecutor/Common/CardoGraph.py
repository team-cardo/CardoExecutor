import base64
import zlib

import networkx as nx
import six

INSTANCE_NODE_ATTR = "node_attributes"
LABEL_ATTR = "label"
DAG_WORKFLOW_VISUALIZATION_URL = 'http://cardo-dag-viewer-cardo.your-domain'  # TODO: move to localhost


class CardoGraph(nx.DiGraph):
	def __apply_attributes(self):
		for key in self.nodes.keys():
			self.nodes[key].update(self.__get_node_attributes(key))

	def __get_node_attributes(self, obj):
		default = {LABEL_ATTR: self.__get_node_name(obj)}
		if hasattr(obj, INSTANCE_NODE_ATTR):
			return dict(default, **getattr(obj, INSTANCE_NODE_ATTR))
		else:
			return default

	@staticmethod
	def __get_node_name(obj):
		if hasattr(obj, LABEL_ATTR):
			return getattr(obj, LABEL_ATTR)
		else:
			return obj.__class__.__name__

	def __str__(self):
		self.__apply_attributes()
		dag_as_pydot = nx.nx_pydot.to_pydot(self)
		dag_as_string = dag_as_pydot.to_string()
		dag_as_string = dag_as_string.replace('\n', '')
		if six.PY2:
			dag_as_base64 = base64.b64encode(zlib.compress(dag_as_string)).replace('\n', '')
		else:
			dag_as_base64 = base64.b64encode(zlib.compress(bytes(dag_as_string, 'utf-8'))).decode('utf-8')
		return DAG_WORKFLOW_VISUALIZATION_URL.format(payload=dag_as_base64)