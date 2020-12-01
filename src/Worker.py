import socket
import json
import threading
import sys
import time
import random
import logging

logging.basicConfig(filename="logs/worker.log", format='%(asctime)s %(levelname)s - %(message)s', filemode='a', level="DEBUG")
logger = logging.getLogger()

execution_pool = []
pool_lock = threading.Lock()

def receive_from_master(port):
	with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
		s.bind(("localhost", port))
		s.listen()

		while True:
			conn, addr = s.accept()
			data = conn.recv(1024).decode()
			if(not data):
				break
			data = json.loads(data)
			task_id = data["task_id"]
			worker_id = data["worker_id"]
			logger.info(f"Task [{task_id}] arrived at Worker [{worker_id}]")
			with pool_lock:
				execution_pool.append(dict(data, **{"time_left": data["duration"], "status": 0}))

def task_execution(port):
	global execution_pool
	while(True):
		with pool_lock:
			if(len(execution_pool) == 0):
				continue
			else:
				for task_index in range(len(execution_pool)):
					execution_pool[task_index]["time_left"] -= 1
					if(execution_pool[task_index]["time_left"] == 0):
						task_id = execution_pool[task_index]["task_id"]
						worker_id = execution_pool[task_index]["worker_id"]
						logger.info(f"Task [{task_id}] finished at Worker [{worker_id}]")
						with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
							s.connect(("localhost", port))
							data_to_send = {key: value for key, value in execution_pool[task_index].items() if key not in ["time_left", "status"]}
							s.send(json.dumps(data_to_send).encode())
							execution_pool[task_index]["status"] = 1
				execution_pool = [task for task in execution_pool if task["status"] == 0]
				time.sleep(1)



if(__name__ == "__main__"):
	PORT_TO_SEND_UPDATE = 5001
	PORT_TO_RECEIVE_JOB = int(sys.argv[1])
	WORKER_ID = sys.argv[2]

	thread_1 = threading.Thread(target = receive_from_master, args = (PORT_TO_RECEIVE_JOB,))
	thread_2 = threading.Thread(target = task_execution, args = (PORT_TO_SEND_UPDATE,))

	thread_1.start()
	thread_2.start()

	thread_1.join()
	thread_2.join()