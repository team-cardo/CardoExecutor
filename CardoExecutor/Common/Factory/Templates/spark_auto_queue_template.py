from subprocess import Popen, PIPE


def set_auto_queue(auto_queue):
	if auto_queue:
		try:
			url = 'your url to all-queues'
			best_idle_queue = get_best_queue(auto_queue, url)
			return {'spark.yarn.queue': best_idle_queue}
		except:
			return {}
	return {}


def get_best_queue(auto_queue, url):
	import requests
	queues = requests.get(url).json()
	kinit_proc = Popen(["hadoop queue -showacls | grep SUBMIT_APPLICATIONS"], stdout=PIPE, shell=True)
	res = kinit_proc.communicate()[0]
	res_list = res.decode('ascii').split('  SUBMIT_APPLICATIONS\n')
	res_list.remove('')
	idle_queues = [queue for queue in queues if
	               queue['used_cores'] == 0 and
	               queue['used_memory'] == 0 and
	               queue['name'] != 'root.default' and
	               queue['name'] in res_list]
	if isinstance(auto_queue, dict):
		idle_queues = [queue for queue in idle_queues if
		               queue['max_cores'] >= auto_queue['max_cores'] and queue['max_memory'] >= auto_queue[
			               'max_memory']]
	best_idle_queues = sorted(idle_queues, key=lambda queue: (queue['max_memory'], queue['max_cores']), reverse=True)
	best_idle_queue = best_idle_queues[0]['name']
	return best_idle_queue
