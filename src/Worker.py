import socket
import json
import threading
import sys
import time
import random
import logging

#Creating the log file for the workers for analysis
logging.basicConfig(filename="logs/worker.log", format='%(asctime)s %(levelname)s - %(message)s', filemode='a', level="DEBUG")
logger = logging.getLogger()

#Setting the global variables which are also the shared variables
execution_pool = []
pool_lock = threading.Lock()

def receive_from_master(port):
	"""
	Function to recieve tasks from the master on thread one
	parameters: Port number to listen to 
	"""
	with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
		s.bind(("localhost", port))
		s.listen()

		while True:
			conn, addr = s.accept()
			data = conn.recv(1024).decode()
			if(not data):
				break
			data = json.loads(data)

			#Getting the data from the just received task
			task_id = data["task_id"]
			worker_id = data["worker_id"]

			#Logging the data into the log file
			logger.info(f"Task [{task_id}] arrived at Worker [{worker_id}]")

			with pool_lock:
				#Inserting the new task into the execution pool within a lock to prevent race condition
				#Keeping status of unfinished tasks as 0
				execution_pool.append(dict(data, **{"time_left": data["duration"], "status": 0}))

def task_execution(port):
	"""
	Function to simuate the execution of each task
	Parameters: Port number to send the finished task
	"""
	global execution_pool
	while(True):
		with pool_lock:

			#Checking for jobs in the execution pool
			if(len(execution_pool) == 0):
				continue
			else:
				#If jobs are found in the execution pool, iteratively reducing their duration by 1
				for task_index in range(len(execution_pool)):
					execution_pool[task_index]["time_left"] -= 1

					#If duration of a particular task becomes 0 then sending the completed task back to the master
					if(execution_pool[task_index]["time_left"] == 0):
						task_id = execution_pool[task_index]["task_id"]
						worker_id = execution_pool[task_index]["worker_id"]
						logger.info(f"Task [{task_id}] finished at Worker [{worker_id}]")
						with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
							s.connect(("localhost", port))
							data_to_send = {key: value for key, value in execution_pool[task_index].items() if key not in ["time_left", "status"]}
							s.send(json.dumps(data_to_send).encode())
							#Changing status of task from 0 to 1 in the execution pool
							execution_pool[task_index]["status"] = 1

				#Keeping only those tasks whose status is still 0, maning they have a longer duration to run
				execution_pool = [task for task in execution_pool if task["status"] == 0]

				#Sleeping the thread for 1 second to simulate the feeling of execution
				time.sleep(1)



if(__name__ == "__main__"):
	"""
	Main function which takes two command line arguments:
	Port number to listen to for incoming tasks
	ID of worker to be sent to
	"""
	PORT_TO_SEND_UPDATE = 5001
	PORT_TO_RECEIVE_JOB = int(sys.argv[1])
	WORKER_ID = sys.argv[2]

	#Starting the two threads
	thread_1 = threading.Thread(target = receive_from_master, args = (PORT_TO_RECEIVE_JOB,))
	thread_2 = threading.Thread(target = task_execution, args = (PORT_TO_SEND_UPDATE,))

	thread_1.start()
	thread_2.start()

	thread_1.join()
	thread_2.join()