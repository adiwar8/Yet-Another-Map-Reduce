import os
import json
import shutil
import time
from write import split
from read import read


block_size = 64
workers_path = "/home/pes2ug20cs025/YetAnotherMapReduce/WORKER/"
master_path = "/home/pes2ug20cs025/YetAnotherMapReduce/MASTER/"
dfspath = "/home/pes2ug20cs025/YetAnotherMapReduce/output"
size_of_worker = 10
sync_period = 60
worker_logpath = "/home/pes2ug20cs025/YetAnotherMapReduce/WORKER/WORKER_LOGS.txt"
master_logpath = "/home/pes2ug20cs025/YetAnotherMapReduce/MASTER/MASTER_LOGS.txt"

def mapreduce(inputfilepath, outputfilepath, mapperpath,reducerpath):
    
    
    try:
        split(inputfilepath)
        print("Starting MR job...")
    except:
        print("Starting MR job...")
    workerspath = workers_path
    masterpath = master_path
    f = open(masterpath + 'inputFiles_metaData.json')
    text = json.load(f)
    inputfilename = inputfilepath.split('/')[-1]
    a = text[inputfilename]
    S = ""	
    global content
    for each in range(1, len(a)):

        destfile = workerspath + "{}".format(a[each][2]) + "/" + "{}".format(a[each][1])+'.'+inputfilename.split('.')[1]
        destpath = workerspath + "{}".format(a[each][2])

        
        os.system('cat ' +destfile+ '|' ' + python3 '+mapperpath+'>' +destpath+ '/output.'+inputfilename.split('.')[1])

        os.system('cat ' +destpath+ '/output.'+inputfilename.split('.')[1] + '| sort -k 1,1 | python3 ' +reducerpath+' >'+destpath +'/reduceroutput')
 
