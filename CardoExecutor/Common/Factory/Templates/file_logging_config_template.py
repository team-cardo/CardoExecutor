def get_config(level, environment, app_name):
	return {
		"version": 1,
		"formatters": {
			"standard": {
				"format": "%(asctime)s | %(name)s | %(levelname)s | %(thread)d | %(message)s"
			}
		},
		"handlers": {
			"default": {
				"level": level,
				"formatter": "standard",
				"class": "logging.FileHandler",
				"filename": "/home/your_path/log/{environment}/{app_name}.log".format(environment=environment, app_name=app_name)
			}
		},
		"loggers": {
			'': {
				"handlers": ["default"],
				"level": level,
			}
		}
	}