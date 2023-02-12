import json
import os
from shutil import rmtree


workers_path = "/home/PES2UG20CS026/YetAnotherMapReduce/WORKER/"
master_path= "/home/PES2UG20CS026/YetAnotherMapReduce/MASTER/"
no_of_workers = 6
worker_logpath = "/home/PES2UG20CS026/YetAnotherMapReduce/WORKER/WORKER_LOGS.txt"
master_logpath = "/home/PES2UG20CS026/YetAnotherMapReduce/MASTER/MASTER_LOGS.txt"
sync_period = 60







try:
    rmtree(workers_path)
except:
    pass
try:
    rmtree(master_path)
except:
    pass

os.mkdir(workers_path)
os.mkdir(master_path)

worker_string = '''import socket
import time
def workernode{}HB():
	msgFromClient = "{}"
	bytesToSend = str.encode(msgFromClient)
	serverAddressPort = ("127.0.0.1", 2000)
	bufferSize = 1024
	UDPClientSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
	while True:
		UDPClientSocket.sendto(bytesToSend, serverAddressPort)
		time.sleep({})'''



master_string = '''import socket
import time
import datetime
import json
import os

<<<<<<< HEAD
f = open('{}')
workers_path = "/home/PES2UG20CS026/YetAnotherMapReduce/WORKER/"
master_path= "/home/PES2UG20CS026/YetAnotherMapReduce/MASTER/"
no_of_workers = 6
worker_logpath = "/home/PES2UG20CS026/YetAnotherMapReduce/WORKER/WORKER_LOGS.txt"
master_logpath = "/home/PES2UG20CS026/YetAnotherMapReduce/MASTER/MASTER_LOGS.txt"
sync_period = 60

=======

workers_path = "/home/PES2UG20CS026/YetAnotherMapReduce/WORKER/"
master_path= "/home/PES2UG20CS026/YetAnotherMapReduce/MASTER/"
sync_period = 60
worker_logpath = "/home/PES2UG20CS026/YetAnotherMapReduce/WORKER/WORKER_LOGS.txt"
master_logpath = "/home/PES2UG20CS026/YetAnotherMapReduce/MASTER/MASTER_LOGS.txt"
>>>>>>> 78c1c9b4bb8500e3f70a591237c1ab602b715205

masterlogs = open(master_logpath, 'a')
workerlogs = open(worker_logpath, 'a')

masterset = set(range(1, no_of_workers + 1))

def masterreceiveheartbeat1():
	masterlogs = open(master_logpath, 'a')
    workerlogs = open(worker_logpath, 'a')
	localIP = "127.0.0.1"
	localPort = 2000
	bufferSize = 1024

	secserverAddressPort = ("127.0.0.1", 3000)
	UDPClientSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
	msgFromClient = "1"
	bytesToSend = str.encode(msgFromClient)

	# msgFromServer = "Hello UDP Client"
	# bytesToSend = str.encode(msgFromServer)
	UDPServerSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
	UDPServerSocket.bind((localIP, localPort))
	# print("Master server up and listening")

	set1 = set()

	while(True):
		start = time.time()
		while(time.time() < start + sync_period):
			bytesAddressPair = UDPServerSocket.recvfrom(bufferSize)
			message = int(bytesAddressPair[0].decode())
			set1.add(int(message))
		if len(set1) == no_of_workers:
			# print("200, All workers functioning", datetime.datetime.fromtimestamp(time.time()))
			UDPClientSocket.sendto(bytesToSend, secserverAddressPort)
			workerlogs.write("200, All workers functioning at - ")
			workerlogs.write(str(datetime.datetime.fromtimestamp(time.time())) + '\\n')
			workerlogs.flush()
			# os.fsync(workerlogs.fileno())
			set1 = set()
	'''



try:
    os.mkdir(master_path)
except:
    pass
filename = master_path + 'master.py'
handle = open(filename, 'w')
handle.write(master_string)


for i in range(1, no_of_workers + 1):
    dirname = workers_path + 'worker{}/'.format(i)
    os.mkdir(dirname)
    filename = workers_path + 'worker{}'.format(i) + '/worker{}.py'.format(i)
    open(filename, 'w').close()
    filehandle = open(filename,"w")
    filehandle.write(worker_string.format(i, i, sync_period/3))
    filehandle.close()


mataData_of_workers = {}  #keeps of track of the workers and their availability to write to the worker nodes
metaDataOfInputFiles = {}  #keeps track of files uploaded needed for reading from worker nodes for cat command


workerpath_metaData = master_path + 'workers_metaData.json'
inputFilePath_metaData = master_path + 'inputFiles_metaData.json'
metaDataOfReplicaspath = master_path + 'metaDataofReplicas.json'
h1 = open(workerpath_metaData, 'w')
h1.write(str(json.dumps(mataData_of_workers, indent=4)))
h2 = open(inputFilePath_metaData, 'w')
h2.write(str(json.dumps(metaDataOfInputFiles, indent=4)))

h1.close()
h2.close()


