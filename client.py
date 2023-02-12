from os import error
import threading
from threading import Thread
from multiprocessing import Process
import json
import sys
from write import split
from read import read
from mapreduce import mapreduce
import startmasterworker


workers_path = "/home/PES2UG20CS026/YetAnotherMapReduce/WORKER/"
master_path= "/home/PES2UG20CS026/YetAnotherMapReduce/MASTER/"
num_of_workers = 6
worker_logpath = "/home/PES2UG20CS026/YetAnotherMapReduce/WORKER/WORKER_LOGS.txt"
master_logpath = "/home/PES2UG20CS026/YetAnotherMapReduce/MASTER/MASTER_LOGS.txt"



sys.path.append(workers_path)
sys.path.append(master_path)


for i in range(1, num_of_workers + 1):
    sys.path.append(workers_path + 'worker{}/'.format(i))
   
list_of_commands = '''
To write a file to the worker nodes: write <absolute path of the file>
To read a file from the worker nodes: cat <filename>
To run the map-reduce job: runmapreducejob -i <absolute path of input file> -o <absolute path of output>  -m <mapper absolute path> -r <reducer absolute path>'''

while True:
    print()
    print("Enter your command...")
    print(list_of_commands)
    print()
    command  = input().split()
    if command[0] == "write":
        if len(command) == 2:
            try:
                message = split(command[1])
                print(message)
                print()
            except error as e:
                print(e)
        else:
            print("Please correct your syntax for the write command")
    if command[0] == "read":
        if len(command) == 2:
            try:
                read(command[1])
            except error as e:
                print(e)
        else:
            print("Please correct your syntax for the read command")
    
    if command[0] == "runmapreducejob":
        
        inputfilepath = command[2]
        outputfilepath = command[4]
        mapperpath = command[6]
        reducerpath = command[8]
        mapreduce(inputfilepath, outputfilepath, mapperpath, reducerpath)
        
    

