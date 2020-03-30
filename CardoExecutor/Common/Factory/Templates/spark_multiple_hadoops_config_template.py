NAMENODES_TEMPLATE = {
	"spark.hadoop.dfs.namenode.rpc-address.{nameservice}.{namenode}": "{namenode_host}:8020",
	"spark.hadoop.dfs.namenode.servicerpc-address.{nameservice}.{namenode}": "{namenode_host}:8022",
	"spark.hadoop.dfs.namenode.http-address.{nameservice}.{namenode}": "{namenode_host}:50070",
	"spark.hadoop.dfs.namenode.https-address.{nameservice}.{namenode}": "{namenode_host}:50470",
}
# TEST
YOUR_TEST_NAMESERVICE = 'your_test_nameservice_host'
YOUR_TEST_NAMENODES = ['your_test_namenode_host1, your_test_namnode_host2']
YOUR_TEST_METASTORES = ['thrift://your_test_metastore_host1:9083', 'thrift://your_test_metastore_host2:9083']
# Prod
YOUR_PROD_NAMESERVICE = 'your_prod_nameservice_host'
YOUR_PROD_NAMENODES = ['your_prod_namenode_host1, your_prod_namnode_host2']
YOUR_PROD_METASTORES = ['thrift://your_prod_metastore_host1:9083', 'thrift://your_prod_metastore_host2:9083']


def get_namenodes_config(nameservice, namenodes, namenode_alias_template='nn{}'):
	config = {
		'spark.hadoop.dfs.ha.automatic-failover.enabled.{nameservice}'.format(nameservice=nameservice): 'true',
		'spark.hadoop.dfs.client.failover.proxy.provider.{nameservice}'.format(nameservice=nameservice):
			'org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider',
		'spark.hadoop.dfs.ha.namenodes.{nameservice}'.format(nameservice=nameservice):
			','.join(map(namenode_alias_template.format, range(len(namenodes))))
	}
	for namenode_index, namenode in enumerate(namenodes):
		namenode_alias = namenode_alias_template.format(namenode_index)
		for k, v in NAMENODES_TEMPLATE.items():
			config[k.format(nameservice=nameservice, namenode=namenode_alias)] = v.format(namenode_host=namenode)
	return config


def set_multiple_hadoops(multiple_hadoops, hive, master_number=1):
	if multiple_hadoops:
		master_namenode = YOUR_PROD_NAMENODES[master_number - 1] if hive == 'prod' else YOUR_TEST_NAMENODES[master_number - 1]
		config = {
			'spark.hadoop.dfs.nameservices': ','.join([YOUR_PROD_NAMESERVICE, YOUR_TEST_NAMESERVICE]),
			'spark.hadoop.hive.metastore.uris': ','.join(YOUR_PROD_METASTORES if hive == 'prod' else YOUR_TEST_METASTORES),
			'spark.yarn.access.hadoopFileSystems': 'hdfs://{host}'.format(host=master_namenode)
		}
		config.update(get_namenodes_config(YOUR_PROD_NAMESERVICE, YOUR_PROD_NAMENODES))
		config.update(get_namenodes_config(YOUR_TEST_NAMESERVICE, YOUR_TEST_NAMENODES))
		return config
	return {}
