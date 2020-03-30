import sys

from cmreslogging.handlers import CMRESHandler
from cardoutils3.common import parse_argv

from CardoExecutor.Common.Factory.Templates.Filters import WorkflowAndStepFilter, Level35Filter, NotLevel35Filter


def get_config(level, environment, app_name, run_id):
    es_additional_fields = {"run_id": run_id, 'exec_command': ' '.join(sys.argv)}
    es_additional_fields.update(parse_argv(sys.argv[1:]))
    return {
        "version": 1,
        "formatters": {
            "standard": {
                "format": "%(asctime)s | {run_id} | %(workflow)s | %(step)s | %(levelname)s | %(message)s".format(
                    run_id=run_id)
            }
        },
        "filters": {
            "default": {
                '()': WorkflowAndStepFilter
            },
            "Level35": {
                '()': Level35Filter
            },
            "NotLevel35": {
                '()': NotLevel35Filter
            }
        },
        "handlers": {
            "default": {
                "level": level,
                "class": "cmreslogging.handlers.CMRESHandler",
                "hosts": [
                    {'host': 'your_elastic_host1', 'port': 9200},
                    {'host': 'your_elastic_host2', 'port': 9200},
                    {'host': 'your_elastic_host3', 'port': 9200},
                    {'host': 'your_elastic_host4', 'port': 9200}
                ],
                "es_index_name": "cardo_logs_{environment}".format(environment=environment),
                "index_name_frequency": CMRESHandler.IndexNameFrequency.YEARLY,
                "es_doc_type": app_name.lower(),
                "es_additional_fields": es_additional_fields,
                "auth_type": CMRESHandler.AuthType.NO_AUTH,
                "formatter": "standard",
                "raise_on_indexing_exceptions": True,
                "use_ssl": False,
                "filters": ["default", "Level35"]
            },
            "stdout": {
                "class": "logging.StreamHandler",
                "formatter": "standard",
                "level": level,
                "filters": ["default", "Level35"],
            },
            "stderr": {
                "class": "logging.StreamHandler",
                "formatter": "standard",
                "level": level,
                "filters": ["default", "Level35"],
                "stream": "ext://sys.stderr"
            },
            "accumulator": {
                "level": level,
                "class": "cmreslogging.handlers.CMRESHandler",
                "hosts": [
                    {'host': 'your_elastic_host1', 'port': 9200},
                    {'host': 'your_elastic_host2', 'port': 9200},
                    {'host': 'your_elastic_host3', 'port': 9200},
                    {'host': 'your_elastic_host4', 'port': 9200}
                ],
                "es_index_name": "accumulator_cardo_logs_{environment}".format(environment=environment),
                "index_name_frequency": CMRESHandler.IndexNameFrequency.YEARLY,
                "es_doc_type": app_name.lower(),
                "es_additional_fields": es_additional_fields,
                "auth_type": CMRESHandler.AuthType.NO_AUTH,
                "formatter": "standard",
                "raise_on_indexing_exceptions": True,
                "use_ssl": False,
                "filters": ["default", "NotLevel35"]
            }
        },
        'root': {
            "handlers": ["default", "stderr", "accumulator"] if environment == 'prod' else ['default', 'stdout', "accumulator"],
            "level": level
        }
    }


KIBANA_SHORTEN_SERVICE = 'your_kibana_shorten_service_url'
KIBANA_SHORTEN_RESULT_TEMPLATE = 'your_kibana_shorten_result_template_url'
KIBANA_SHORTEN_SERVICE_HEADER = {'kbn-version': '5.4.2'}
KIBANA_RUN_ID_URL = "your_link_to_kibana_dash with {run_ud}"
