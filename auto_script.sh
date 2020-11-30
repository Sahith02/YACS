
if test -f "temp/pid_to_kill.txt"; then
	rm temp/pid_to_kill.txt
fi
touch temp/pid_to_kill.txt
if test -f "logs/master.log"; then
	rm logs/master.log
fi
if test -f "logs/worker.log"; then
	rm logs/worker.log
fi

python3 src/Worker.py 4000 1 &
WORKER_1_PID=$!
echo $WORKER_1_PID >> temp/pid_to_kill.txt

python3 src/Worker.py 4001 2 &
WORKER_2_PID=$!
echo $WORKER_2_PID >> temp/pid_to_kill.txt

python3 src/Worker.py 4002 3 &
WORKER_3_PID=$!
echo $WORKER_3_PID >> temp/pid_to_kill.txt

python3 src/Master.py src/Config.json $2 &
MASTER_PID=$!
echo $MASTER_PID >> temp/pid_to_kill.txt

python3 src/Requests.py $1 &
