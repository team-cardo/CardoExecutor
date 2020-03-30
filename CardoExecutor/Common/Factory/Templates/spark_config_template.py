def get_your_env1_config(app_name, executor_cores, executor_memory, max_cores, partitions):
	return {
		"spark.app.name": app_name,
		"spark.master": "spark://your-host-spark-master:7077",
		"spark.executor.cores": str(executor_cores),
		"spark.eventLog.enabled": "true",
		"spark.executor.memory": str(executor_memory),
		"spark.cores.max": str(max_cores),
		"spark.logConf": "true",
		"spark.sql.shuffle.partitions": str(partitions),
		"spark.jars": "file:///your_path_to_jar1.jar,"
					  "file:///your_path_to_jar2.jar",
		'spark.sql.catalogImplementation': 'hive',
	}


def get_your_env2_config(app_name, executor_cores, executor_memory, max_cores, partitions):
	return dict(get_your_env1_config(app_name, executor_cores, executor_memory, max_cores, partitions),
				**{"spark.master": "yarn", "spark.jars": "file:///your_path_to_jar1.jar,"
					  									"file:///your_path_to_jar2.jar"})


def get_local_config(app_name, executor_cores, executor_memory, max_cores, partitions):
	return dict(get_your_env1_config(app_name, executor_cores, executor_memory, max_cores, partitions),
				**{"spark.master": "local"})


CLUSTER_DICT = {"your_env1": get_your_env1_config, "env2": get_your_env2_config, "local": get_local_config}


def get_config(cluster, *args, **kwargs):
	return CLUSTER_DICT.get(cluster, get_your_env1_config)(*args, **kwargs)
