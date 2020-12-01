import re
import copy
import sys
from datetime import datetime
import matplotlib.pyplot as plt

def YACSWorker(FILE_PATH):
    #Opening log of worker
    file = open(FILE_PATH)

    #Getting data from file and formatting it as per the requirement
    list_tasks = file.readlines()
    list_tasks = list(map(lambda x : x.strip().replace(" INFO", "").split(" - "), list_tasks))
    list_tasks = list(map(lambda x : x[0] + x[1].replace("Task", ""), list_tasks))
    list_tasks = list(map(lambda x : x.split(" "), list_tasks))
    list_tasks = list(map(lambda x : x[1] + " " + x[2], list_tasks))
    list_tasks = list(map(lambda x : x.split(" "), list_tasks))
    list_tasks = list(map(lambda x : x[0][:8] + " " + x[1][1:-1], list_tasks))
    list_tasks = list(map(lambda x : x.split(" "), list_tasks))

    #Making a dictionary with key as the worker and a list of values with the start time and end time of tasks
    workers = {}
    for i in range(len(list_tasks)):
        if(list_tasks[i][1] in workers):
            workers[list_tasks[i][1]].append(list_tasks[i][0])
        else:
            workers[list_tasks[i][1]] = [list_tasks[i][0]]

    #Initializing format for time
    FMT = '%H:%M:%S'
    times = []

    #Using datetime library to find difference between start time and end time
    for key in workers:
        tdelta = datetime.strptime(workers[key][1], FMT) - datetime.strptime(workers[key][0], FMT)
        times.append(tdelta.seconds)

    #Calculating the mean
    n = len(times)
    mean = sum(times)/n

    #Calculating the mode
    times.sort() 
    if n % 2 == 0: 
        median1 = times[n//2] 
        median2 = times[n//2 - 1] 
        median = (median1 + median2)/2
    else: 
        median = times[n//2] 

    return (mean, median) 



def Filter(string, substr):
    #Function to filter text based on specified substring
    return [s for s in string if any(sub in s for sub in substr)] 

def YACSMaster(FILE_PATH):
    #Opening log file of master
    file = open(FILE_PATH)
    
    #Getting data from file and formatting it as per the requirement
    list_tasks = file.readlines()
    list_tasks = list(map(lambda x : x.strip('\n'), list_tasks))
    filtering_received = ["received"]
    filtering_finished = ["finished"]
    list_received = Filter(list_tasks, filtering_received)
    list_finished = Filter(list_tasks, filtering_finished)
    list_received = list(map(lambda x : x.split(" "), list_received))
    list_received = list(map(lambda x : x[1] + " " + x[7], list_received))
    list_received = list(map(lambda x : x.split(" "), list_received))
    list_received = list(map(lambda x : x[0][:8] + " " + x[1][1:-1], list_received))
    list_received = list(map(lambda x : x.split(" "), list_received))
    list_finished = list(map(lambda x : x.split(" "), list_finished))
    list_finished = list(map(lambda x : x[1] + " " + x[5], list_finished))
    list_finished = list(map(lambda x : x.split(" "), list_finished))
    list_finished = list(map(lambda x : x[0][:8] + " " + x[1][1:-1], list_finished))
    list_finished = list(map(lambda x : x.split(" "), list_finished))

    #Making a dictionary with key as the worker and a list of values with the start time and end time of jobs
    workers = {}
    for i in range(len(list_received)):
        workers[list_received[i][1]] = [list_received[i][0]]
    for i in range(len(list_finished)):
        workers[list_finished[i][1]].append(list_finished[i][0])

    #Initializing format for time
    FMT = '%H:%M:%S'
    times = []
    for key in workers:
        tdelta = datetime.strptime(workers[key][1], FMT) - datetime.strptime(workers[key][0], FMT)
        times.append(tdelta.seconds)

    #Calculating the mean
    n = len(times)
    mean = sum(times)/n

    #Calculating the median
    times.sort() 
    if n % 2 == 0: 
        median1 = times[n//2] 
        median2 = times[n//2 - 1] 
        median = (median1 + median2)/2
    else: 
        median = times[n//2] 

    return (mean, median)


def plotYACS(FILE_PATH, algo):
    #Function to plot a line graph to show how many tasks are present for each worker at any given time
    file = open(FILE_PATH)
    list_tasks = file.readlines()
    list_tasks = list(map(lambda x : x.strip().replace("INFO - Task [", "").split(" "), list_tasks))
    list_tasks = list(map(lambda x : x[1] + " " + x[3] + " " + x[6][1:-1], list_tasks))
    list_tasks = list(map(lambda x : x.split(" "), list_tasks))

    count_dictionary = {'1' : 0, '2' : 0, '3' : 0}
    plotting_list = []
    for i in range(len(list_tasks)):
        if(list_tasks[i][1] == "arrived"):
            count_dictionary[list_tasks[i][2]] += 1
            plotting_list.append((list_tasks[i][0], copy.deepcopy(count_dictionary)))
        elif(list_tasks[i][1] == "finished"):
            count_dictionary[list_tasks[i][2]] -= 1
            plotting_list.append((list_tasks[i][0], copy.deepcopy(count_dictionary)))

    x = [x[0] for x in plotting_list]
    y1 = [y[1]['1'] for y in plotting_list]
    y2 = [y[1]['2'] for y in plotting_list]
    y3 = [y[1]['3'] for y in plotting_list]

    fig, (ax1, ax2, ax3) = plt.subplots(3, sharex = True, sharey = True, figsize = (22, 12))
    fig.suptitle(algo + " : Time vs Number of Tasks for each Worker")
    ax1.plot(x, y1)
    ax2.plot(x, y2)
    ax3.plot(x, y3)

    plt.xticks(rotation = -90)
    ax1.set(ylabel='Number of tasks : Worker 1')
    ax2.set(ylabel='Number of tasks : Worker 2')
    ax3.set(xlabel = 'Time', ylabel='Number of tasks : Worker 3')
    plt.savefig("stats/plot_task_" + sys.argv[3] + ".png")


def plotAlgo(WORKER_FILE_PATH, MASTER_FILE_PATH, algo):
    #Function to plot bar graph for mean and median for both worker and master for the given algorithm
    worker = YACSWorker(WORKER_FILE_PATH)
    master = YACSMaster(MASTER_FILE_PATH)
    x_axis = ["Mean for Task", "Median for Task", "Mean for Job", "Median for Job"] 
    y_axis = worker + master 

    fig = plt.figure(figsize = (10, 5)) 
    plt.bar(x_axis, y_axis, color ='maroon', width = 0.4)  
    plt.ylabel("Average Time Taken") 
    plt.title("Mean and Median for Tasks and Jobs for " + str(algo)) 
    plt.savefig("stats/plot_mean_median_" + sys.argv[3] + ".png") 


#Finding mean and median time taken for a task to be completed by a worker
answer = YACSWorker(sys.argv[1])
print(" ----- Task Completion Time for " + sys.argv[3] + " ----- ")
print("Mean = " + str(answer[0]))
print("Median = " + str(answer[1]))


#Finding mean and median time taken for a job to be completed by master
answer = YACSMaster(sys.argv[2])
print(" ----- Job Completion Time for " + sys.argv[3] + " ----- ")
print("Mean = " + str(answer[0]))
print("Median = " + str(answer[1]))

#Plotting graphs
plotYACS(sys.argv[1], sys.argv[3])
plotAlgo(sys.argv[1], sys.argv[2], sys.argv[3])
