import socket
import json
import threading
import sys
import time
import random
import logging


"""
Creating the log file to perform further analysis using the logs
"""
logging.basicConfig(filename="logs/master.log", format='%(asctime)s %(levelname)s - %(message)s', filemode='a', level="DEBUG")
logger = logging.getLogger()

"""
Setting up the required global variables 
with shared variables as :
	Workers
	pool
locking[] servers as the semaphore
"""
host = "localhost"
port_jobs = 5000
port_workers = 5001
workers = []
locking=[1]
pool=[]
jobs_done = 0
jobs_done_lock = threading.Lock()
RR_check = 0
global_job_no=0


def wait(locking):
	"""
	Wait function for the semaphore to either enter the critical section or to be prevented from entering the critical section
	"""
	while(locking[0]<1):
		pass
	locking[0]-=1
	return

def signal(locking):
	"""
	Function to release the semaphore
	"""
	locking[0]+=1


def send_to_worker(SCHEDULING_ALGO):
	"""
	Function which runs on thread three.
	This function constantly checks for jobs in the pool and then distributes the tasks across different workers
	---parameters---
	Scheduling algorithm 
	example:- "RANDOM" or "RR" "LL"
	"""
	global workers
	global RR_check
	while True:
		if len(pool)<=0 :
			continue
		wait(locking)
		#Scheduling algorithm for RANDOM
		if(SCHEDULING_ALGO == "RANDOM"):
			#iterating through each job in the pool
			for job in pool:
				#loop to deal with the map_tasks for the current job
				for task in job["map_tasks"]:
					if "assigned" in task:
						continue
					"""
					preparing the data to be sent to the worker
					"""
					data={}
					data["job_id"]=job["job_id"]
					data["task_id"]=task["task_id"]
					data["duration"]=task["duration"]

					#Choosing a random worker out of all the workers
					worker_number = random.randint(0, len(workers) - 1)
					count = 0

					#Loop to choose a worker that has free slots
					while(workers[worker_number]["slots_free"]==0):
						if(count>5*len(workers)):
							#condition to deal with case when no free worker is available
							signal(locking)
							wait(locking)
							count=0
						worker_number = random.randint(0, len(workers) - 1)
						count+=1
					data["worker_id"]=worker_number+1
					workers[worker_number]["slots_free"]-=1

					#Sending the data of the current map_task to the randomly chosen worker
					with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
						s.connect(("localhost", workers[worker_number]["port"]))
						obj=json.dumps(data)
						s.send(obj.encode())

					#Entering the data into the log file
					task_id = data["task_id"]
					worker_id = data["worker_id"]
					logger.info(f"Sent Task [{task_id}] to Worker [{worker_id}]")

					index=0
					# marking the particular map_task as assigned 
					for index in range(0,len(pool)):
						if pool[index]["job_id"]==job["job_id"]:
							break
					task_index=0
					for task_index in range(0,len(pool[index]["map_tasks"])):
						if task["task_id"]==pool[index]["map_tasks"][task_index]["task_id"]:
							break
					pool[index]["map_tasks"][task_index]["assigned"]=1


				if(len(job["map_tasks"])==0):
					#loop to deal with the reduce tasks after all map_tasks have finished for the current job
					for reducers in job["reduce_tasks"]:
						if "assigned" in reducers:
							continue
						#preparing the data to be sent to the worker
						data={}
						data["job_id"]=job["job_id"]
						data["task_id"]=reducers["task_id"]
						data["duration"]=reducers["duration"]

						#Choosing a random worker out of all the workers
						worker_number = random.randint(0, len(workers) - 1)
						count=0

						#Loop to choose a worker that has free slots
						while(workers[worker_number]["slots_free"]==0):
							if(count>15):
								signal(locking)
								#condition to deal with case when no free worker is available
								wait(locking)
								count=0
							worker_number = random.randint(0, len(workers) - 1)
							count+=1
						data["worker_id"]=worker_number+1
						workers[worker_number]["slots_free"]-=1

						#Sending the data of the current reduce_task to the randomly chosen worker
						with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
							s.connect(("localhost", workers[worker_number]["port"]))
							obj=json.dumps(data)
							s.send(obj.encode())

						#Entering the data into the log file
						task_id = data["task_id"]
						worker_id = data["worker_id"]
						logger.info(f"Sent Task [{task_id}] to Worker [{worker_id}]")

						# marking the particular reduce_task as assigned 
						index=0
						for index in range(0,len(pool)):
							if pool[index]["job_id"]==job["job_id"]:
								break
						task_index=0
						for task_index in range(0,len(pool[index]["reduce_tasks"])):
							if reducers["task_id"]==pool[index]["reduce_tasks"][task_index]["task_id"]:
								break
						pool[index]["reduce_tasks"][task_index]["assigned"]=1
		#Scheduling algorithm for Least Loaded
		elif(SCHEDULING_ALGO=="LL"):
			#iterating through each job in the pool
			for job in pool:
				for task in job["map_tasks"]:
					if "assigned" in task:
						continue
					#preparing the data to be sent to the worker
					data={}
					data["job_id"]=job["job_id"]
					data["task_id"]=task["task_id"]
					data["duration"]=task["duration"]
					workers2=workers

					#sorting the workers in descending order based off number of free slots
					workers=sorted(workers2, key = lambda i: i['slots_free'],reverse=True)
					data["worker_id"]=-1
					x=0

					#loop to choose a worker which has free slots
					while(data["worker_id"]==-1):
						x=0
						for worker in workers:
							if(worker["slots_free"]>0):
								data["worker_id"]=worker["worker_id"]
								break
							else:
								#Condition to deal with the case of no free workers being available
								signal(locking)
								wait(locking)
							x+=1
					workers[x]["slots_free"]-=1

					#Sending the data of the current map_task to the randomly chosen worker
					with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
						s.connect(("localhost", workers[x]["port"]))
						obj=json.dumps(data)
						s.send(obj.encode())

					#Entering the data into the log file
					task_id = data["task_id"]
					worker_id = data["worker_id"]
					logger.info(f"Sent Task [{task_id}] to Worker [{worker_id}]")

					# marking the particular map_task as assigned 
					index=0
					for index in range(0,len(pool)):
						if pool[index]["job_id"]==job["job_id"]:
							break
					task_index=0
					for task_index in range(0,len(pool[index]["map_tasks"])):
						if task["task_id"]==pool[index]["map_tasks"][task_index]["task_id"]:
							break
					pool[index]["map_tasks"][task_index]["assigned"]=1


				if(len(job["map_tasks"])==0):
					#Checking if all map tasks are done and only then doing reduce tasks
					for reducers in job["reduce_tasks"]:
						if "assigned" in reducers:
							continue
						#preparing the data to be sent to the worker
						data={}
						data["job_id"]=job["job_id"]
						data["task_id"]=reducers["task_id"]
						data["duration"]=reducers["duration"]

						#sorting the workers in descending order based off number of free slots
						workers2=workers
						workers=sorted(workers2, key = lambda i: i['slots_free'],reverse=True)
						data["worker_id"]=-1

						#loop to choose a worker which has free slots
						x=0
						while(data["worker_id"]==-1):
							x=0
							for worker in workers:
								if(worker["slots_free"]>0):
									data["worker_id"]=worker["worker_id"]
									break
								else:
									#Condition to deal with the case of no free workers being available
									signal(locking)
									wait(locking)
								x+=1
						workers[x]["slots_free"]-=1

						#Sending the data of the current reduce_task to the randomly chosen worker
						with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
							s.connect(("localhost", workers[x]["port"]))
							obj=json.dumps(data)
							s.send(obj.encode())

						#Entering the data into the log file
						task_id = data["task_id"]
						worker_id = data["worker_id"]
						logger.info(f"Sent Task [{task_id}] to Worker [{worker_id}]")

						# marking the particular reduce_task as assigned 
						index=0
						for index in range(0,len(pool)):
							if pool[index]["job_id"]==job["job_id"]:
								break
						task_index=0
						for task_index in range(0,len(pool[index]["reduce_tasks"])):
							if reducers["task_id"]==pool[index]["reduce_tasks"][task_index]["task_id"]:
								break
						pool[index]["reduce_tasks"][task_index]["assigned"]=1
		#Scheduling algorithm for Round Robin
		elif(SCHEDULING_ALGO == "RR"):
			# x=0
			for job in pool:
				for task in job["map_tasks"]:
					if "assigned" in task:
						continue
					#preparing the data to be sent to the worker
					data={}
					data["job_id"]=job["job_id"]
					data["task_id"]=task["task_id"]
					data["duration"]=task["duration"]
					data["worker_id"]=-1
					count=0

					#using globale variable to decide which worker to send the data to
					RR_check=RR_check%len(workers)

					#looping till worker with free slots is reached
					while(workers[RR_check]["slots_free"]<=0):
						RR_check=(RR_check+1)%len(workers)
						if (count==15):
							#Condition to deal with the case of no free workers
							signal(locking)
							wait(locking)
							count=0
						count+=1
					data["worker_id"]=workers[RR_check]["worker_id"]
					workers[RR_check]["slots_free"]-=1

					#Sending the data of the current map_task to the randomly chosen worker
					with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
						s.connect(("localhost", workers[RR_check]["port"]))
						obj=json.dumps(data)
						s.send(obj.encode())

					#Entering the data into the log file
					task_id = data["task_id"]
					worker_id = data["worker_id"]
					logger.info(f"Sent Task [{task_id}] to Worker [{worker_id}]")

					# marking the particular map_task as assigned 
					index=0
					for index in range(0,len(pool)):
						if pool[index]["job_id"]==job["job_id"]:
							break
					task_index=0
					for task_index in range(0,len(pool[index]["map_tasks"])):
						if task["task_id"]==pool[index]["map_tasks"][task_index]["task_id"]:
							break
					pool[index]["map_tasks"][task_index]["assigned"]=1

					#incrementing the value of the globale variable RR_check to appoint the next task with the next worker
					RR_check += 1


				if(len(job["map_tasks"])==0):
					#Checking if all map tasks are done and only then doing reduce tasks
					for reducers in job["reduce_tasks"]:
						if "assigned" in reducers:
							continue
						#preparing the data to be sent to the worker
						data={}
						data["job_id"]=job["job_id"]
						data["task_id"]=reducers["task_id"]
						data["duration"]=reducers["duration"]

						#using globale variable to decide which worker to send the data to
						RR_check=RR_check%len(workers)
						count=0
						data["worker_id"]=-1

						#looping till worker with free slots is reached
						while(workers[RR_check]["slots_free"]<=0):
							RR_check=(RR_check+1)%len(workers)
							if (count==15):
								signal(locking)
								wait(locking)
								count=0
							count+=1
						data["worker_id"]=workers[RR_check]["worker_id"]
						workers[RR_check]["slots_free"]-=1

						#Sending the data of the current reduce_task to the randomly chosen worker
						with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
							s.connect(("localhost", workers[RR_check]["port"]))
							obj=json.dumps(data)
							s.send(obj.encode())

						#Entering the data into the log file
						task_id = data["task_id"]
						worker_id = data["worker_id"]
						logger.info(f"Sent Task [{task_id}] to Worker [{worker_id}]")

						# marking the particular map_task as assigned 
						index=0
						for index in range(0,len(pool)):
							if pool[index]["job_id"]==job["job_id"]:
								break
						task_index=0
						for task_index in range(0,len(pool[index]["reduce_tasks"])):
							if reducers["task_id"]==pool[index]["reduce_tasks"][task_index]["task_id"]:
								break
						pool[index]["reduce_tasks"][task_index]["assigned"]=1
						#incrementing the value of the globale variable RR_check to appoint the next task with the next worker
						RR_check += 1
		signal(locking)


def listen_for_jobs():
	"""
	Function to listen for incoming requests on Thread one
	No parameters
	"""
	global global_job_no
	with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
		s.bind((host, port_jobs))
		s.listen()
		while True:
			conn, addr = s.accept()
			with conn:
				while True:
					data = conn.recv(1024).decode()
					if not data:
						break
					job = json.loads(data)
					wait(locking)
					#Adding new jobs/requests to the pool
					if(type(job["job_id"])!=int or job["job_id"]!=global_job_no):
						job["job_id"]=global_job_no
					
					pool.append(job)
					global_job_no+=1
					#Logging information in the log file
					job_id = str(job["job_id"])
					logger.info(f"Master received Job [{job_id}]")
					signal(locking)

def listen_for_workers(a):
	"""
	Function to listen for completed tasks from workers on thread two
	"""
	global jobs_done
	with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
		s.bind((host, port_workers))
		s.listen()

		while True:
			conn, addr = s.accept()
			with conn:
				while True:
					data = conn.recv(1024).decode()
					if(not data):
						break
					data = json.loads(data)
					wait(locking)

					#Obtaning information about teh finished task, like worker_id, task_id, and job_no
					task_id = str(data["task_id"])
					worker_id = str(data["worker_id"])

					#logging the information in the log file
					logger.info(f"Received Task [{task_id}] from Worker [{worker_id}]")

					#Finding the worker that just completed the task in the workers list
					worker_index=0
					for worker_index in range(0,len(workers)):
						if workers[worker_index]["worker_id"]==data["worker_id"]:
							break

					#Increasing the number of slots for that worker by 1
					workers[worker_index]["slots_free"]+=1

					#Finding the index of the task and the job that was just completed.
					#Following which the task is removed from that job in the pool
					job_index=0
					for job_index in range(0,len(pool)):
						if(data["job_id"]==pool[job_index]["job_id"]):
							break
					if(len(pool[job_index]["map_tasks"])!=0):
						task_index=0
						for task_index in range(0,len(pool[job_index]["map_tasks"])):
							if pool[job_index]["map_tasks"][task_index]["task_id"]==data["task_id"]:
								break
						pool[job_index]["map_tasks"].pop(task_index)
					else:
						task_index=0
						for task_index in range(0,len(pool[job_index]["reduce_tasks"])):
							if pool[job_index]["reduce_tasks"][task_index]["task_id"]==data["task_id"]:
								break
						pool[job_index]["reduce_tasks"].pop(task_index)

					#Removing the job from the pool if both map and reduce tasks are completed.
					if(len(pool[job_index]["map_tasks"])==0 and len(pool[job_index]["reduce_tasks"])==0):
						pool.pop(job_index)
						job_id = data["job_id"]
						with jobs_done_lock:
							jobs_done += 1
						logger.info(f"Job [{job_id}] successfully finished. Jobs done : {jobs_done}")
						print(f"Job [{job_id}] successfully finished. Jobs done : {jobs_done}")
					signal(locking)

if(__name__ == "__main__"):
	"""
	Main function which takes two command line arguments:
	Path to config file: Which contains the configurations of the worker nodes
	Scheduling Algorithm: Which contains algorithm to be used for scheduling the tasks. No default scheduler .
	Must enter "RANDOM" , "LL" or "RR" only
	"""
	PATH_TO_CONFIG = sys.argv[1]
	SCHEDULING_ALGO = sys.argv[2]

	#Reading the configuration file to insert their information into the workers list
	workers = json.load(open(PATH_TO_CONFIG))["workers"]
	for each in workers:
		each["slots_free"] = each["slots"]


	#Creating the three threads
	thread_1 = threading.Thread(target = listen_for_jobs)
	thread_2 = threading.Thread(target = listen_for_workers, args = (30,))
	thread_3 = threading.Thread(target = send_to_worker, args = (SCHEDULING_ALGO,))

	thread_1.start()
	thread_3.start()
	thread_2.start()

	thread_1.join()
	thread_2.join()
	thread_3.join()
