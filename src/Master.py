import socket
import json
import threading
import sys
import time
import random
import logging

logging.basicConfig(filename="logs/master.log", format='%(asctime)s %(levelname)s - %(message)s', filemode='a', level="DEBUG")
logger = logging.getLogger()

host = "localhost"
port_jobs = 5000
port_workers = 5001
workers = []
locking=[1]
pool=[]
jobs_done = 0
jobs_done_lock = threading.Lock()
RR_check = 0

def wait(locking):
	while(locking[0]<1):
		pass
	locking[0]-=1
	return

def signal(locking):
	locking[0]+=1

def send_to_worker(SCHEDULING_ALGO):
	global workers
	global RR_check
	while True:
		if len(pool)<=0 :
			continue
		wait(locking)
		if(SCHEDULING_ALGO == "RANDOM"):
			for job in pool:
				for task in job["map_tasks"]:
					if "assigned" in task:
						continue
					data={}
					data["job_id"]=job["job_id"]
					data["task_id"]=task["task_id"]
					data["duration"]=task["duration"]
					worker_number = random.randint(0, len(workers) - 1)
					count = 0
					while(workers[worker_number]["slots_free"]==0):
						if(count>15):
							signal(locking)
							wait(locking)
							count=0
						worker_number = random.randint(0, len(workers) - 1)
						count+=1
					data["worker_id"]=worker_number+1
					workers[worker_number]["slots_free"]-=1
					with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
						s.connect(("localhost", workers[worker_number]["port"]))
						obj=json.dumps(data)
						s.send(obj.encode())
					task_id = data["task_id"]
					worker_id = data["worker_id"]
					logger.info(f"Sent Task [{task_id}] to Worker [{worker_id}]")

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
					for reducers in job["reduce_tasks"]:
						if "assigned" in reducers:
							continue
						data={}
						data["job_id"]=job["job_id"]
						data["task_id"]=reducers["task_id"]
						data["duration"]=reducers["duration"]
						worker_number = random.randint(0, len(workers) - 1)
						count=0
						while(workers[worker_number]["slots_free"]==0):
							if(count>15):
								signal(locking)
								wait(locking)
								count=0
							worker_number = random.randint(0, len(workers) - 1)
							count+=1
						data["worker_id"]=worker_number+1
						workers[worker_number]["slots_free"]-=1
						with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
							s.connect(("localhost", workers[worker_number]["port"]))
							obj=json.dumps(data)
							s.send(obj.encode())
						task_id = data["task_id"]
						worker_id = data["worker_id"]
						logger.info(f"Sent Task [{task_id}] to Worker [{worker_id}]")

						index=0
						for index in range(0,len(pool)):
							if pool[index]["job_id"]==job["job_id"]:
								break
						task_index=0
						for task_index in range(0,len(pool[index]["reduce_tasks"])):
							if reducers["task_id"]==pool[index]["reduce_tasks"][task_index]["task_id"]:
								break
						pool[index]["reduce_tasks"][task_index]["assigned"]=1
		elif(SCHEDULING_ALGO=="LL"):
			for job in pool:
				for task in job["map_tasks"]:
					if "assigned" in task:
						continue
					data={}
					data["job_id"]=job["job_id"]
					data["task_id"]=task["task_id"]
					data["duration"]=task["duration"]
					workers2=workers
					workers=sorted(workers2, key = lambda i: i['slots_free'],reverse=True)
					data["worker_id"]=-1
					x=0
					while(data["worker_id"]==-1):
						x=0
						for worker in workers:
							if(worker["slots_free"]>0):
								data["worker_id"]=worker["worker_id"]
								break
							else:
								signal(locking)
								wait(locking)
							x+=1
					workers[x]["slots_free"]-=1
					with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
						s.connect(("localhost", workers[x]["port"]))
						obj=json.dumps(data)
						s.send(obj.encode())
					task_id = data["task_id"]
					worker_id = data["worker_id"]
					logger.info(f"Sent Task [{task_id}] to Worker [{worker_id}]")

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
					for reducers in job["reduce_tasks"]:
						if "assigned" in reducers:
							continue
						data={}
						data["job_id"]=job["job_id"]
						data["task_id"]=reducers["task_id"]
						data["duration"]=reducers["duration"]
						workers2=workers
						workers=sorted(workers2, key = lambda i: i['slots_free'],reverse=True)
						data["worker_id"]=-1
						x=0
						while(data["worker_id"]==-1):
							x=0
							for worker in workers:
								if(worker["slots_free"]>0):
									data["worker_id"]=worker["worker_id"]
									break
								else:
									signal(locking)
									wait(locking)
								x+=1
						workers[x]["slots_free"]-=1
						with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
							s.connect(("localhost", workers[x]["port"]))
							obj=json.dumps(data)
							s.send(obj.encode())
						task_id = data["task_id"]
						worker_id = data["worker_id"]
						logger.info(f"Sent Task [{task_id}] to Worker [{worker_id}]")

						index=0
						for index in range(0,len(pool)):
							if pool[index]["job_id"]==job["job_id"]:
								break
						task_index=0
						for task_index in range(0,len(pool[index]["reduce_tasks"])):
							if reducers["task_id"]==pool[index]["reduce_tasks"][task_index]["task_id"]:
								break
						pool[index]["reduce_tasks"][task_index]["assigned"]=1
		elif(SCHEDULING_ALGO == "RR"):
			# x=0
			for job in pool:
				for task in job["map_tasks"]:
					if "assigned" in task:
						continue
					data={}
					data["job_id"]=job["job_id"]
					data["task_id"]=task["task_id"]
					data["duration"]=task["duration"]
					data["worker_id"]=-1
					count=0
					RR_check=RR_check%len(workers)
					while(workers[RR_check]["slots_free"]<=0):
						RR_check=(RR_check+1)%len(workers)
						if (count==15):
							signal(locking)
							wait(locking)
							count=0
						count+=1
					data["worker_id"]=workers[RR_check]["worker_id"]
					workers[RR_check]["slots_free"]-=1
					with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
						s.connect(("localhost", workers[RR_check]["port"]))
						obj=json.dumps(data)
						s.send(obj.encode())
					task_id = data["task_id"]
					worker_id = data["worker_id"]
					logger.info(f"Sent Task [{task_id}] to Worker [{worker_id}]")

					index=0
					for index in range(0,len(pool)):
						if pool[index]["job_id"]==job["job_id"]:
							break
					task_index=0
					for task_index in range(0,len(pool[index]["map_tasks"])):
						if task["task_id"]==pool[index]["map_tasks"][task_index]["task_id"]:
							break
					pool[index]["map_tasks"][task_index]["assigned"]=1
					RR_check += 1


				if(len(job["map_tasks"])==0):
					for reducers in job["reduce_tasks"]:
						if "assigned" in reducers:
							continue
						data={}
						data["job_id"]=job["job_id"]
						data["task_id"]=reducers["task_id"]
						data["duration"]=reducers["duration"]
						RR_check=RR_check%len(workers)
						count=0
						data["worker_id"]=-1
						while(workers[RR_check]["slots_free"]<=0):
							RR_check=(RR_check+1)%len(workers)
							if (count==15):
								signal(locking)
								wait(locking)
								count=0
							count+=1
						data["worker_id"]=workers[RR_check]["worker_id"]
						workers[RR_check]["slots_free"]-=1
						with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
							s.connect(("localhost", workers[RR_check]["port"]))
							obj=json.dumps(data)
							s.send(obj.encode())
						task_id = data["task_id"]
						worker_id = data["worker_id"]
						logger.info(f"Sent Task [{task_id}] to Worker [{worker_id}]")

						index=0
						for index in range(0,len(pool)):
							if pool[index]["job_id"]==job["job_id"]:
								break
						task_index=0
						for task_index in range(0,len(pool[index]["reduce_tasks"])):
							if reducers["task_id"]==pool[index]["reduce_tasks"][task_index]["task_id"]:
								break
						pool[index]["reduce_tasks"][task_index]["assigned"]=1
						RR_check += 1
		signal(locking)


def listen_for_jobs():
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
					pool.append(job)
					job_id = str(job["job_id"])
					logger.info(f"Master received Job [{job_id}]")
					signal(locking)

def listen_for_workers(a):
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
					task_id = str(data["task_id"])
					worker_id = str(data["worker_id"])
					logger.info(f"Received Task [{task_id}] from Worker [{worker_id}]")
					worker_index=0
					for worker_index in range(0,len(workers)):
						if workers[worker_index]["worker_id"]==data["worker_id"]:
							break
					workers[worker_index]["slots_free"]+=1
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

					if(len(pool[job_index]["map_tasks"])==0 and len(pool[job_index]["reduce_tasks"])==0):
						pool.pop(job_index)
						job_id = data["job_id"]
						with jobs_done_lock:
							jobs_done += 1
						logger.info(f"Job [{job_id}] successfully finished. Jobs done : {jobs_done}")
						print(f"Job [{job_id}] successfully finished. Jobs done : {jobs_done}")
					signal(locking)

if(__name__ == "__main__"):
	PATH_TO_CONFIG = sys.argv[1]
	SCHEDULING_ALGO = sys.argv[2]

	workers = json.load(open(PATH_TO_CONFIG))["workers"]
	for each in workers:
		each["slots_free"] = each["slots"]

	thread_1 = threading.Thread(target = listen_for_jobs)
	thread_2 = threading.Thread(target = listen_for_workers, args = (30,))
	thread_3 = threading.Thread(target = send_to_worker, args = (SCHEDULING_ALGO,))

	thread_1.start()
	thread_3.start()
	thread_2.start()

	thread_1.join()
	thread_2.join()
	thread_3.join()