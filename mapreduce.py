import os
import json
import shutil
import time
from write import split
from read import read


workers_path = "/home/PES2UG20CS026/YetAnotherMapReduce/WORKER/"
master_path= "/home/PES2UG20CS026/YetAnotherMapReduce/MASTER/"
worker_logpath = "/home/PES2UG20CS026/YetAnotherMapReduce/WORKER/WORKER_LOGS.txt"
master_logpath = "/home/PES2UG20CS026/YetAnotherMapReduce/MASTER/MASTER_LOGS.txt"

def mapreduce(inputfilepath, outputfilepath, mapperpath,reducerpath):
    
    
    try:
        split(inputfilepath)
        print("Writing file into worker")
        print("Starting MapReduce job...")
    except:
        print("Starting MapReduce job...")
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
        os.system('cat ' +destfile+ '| python3 '+mapperpath+'>' +destpath+ '/output.'+inputfilename.split('.')[1])
        with open(destpath+ '/output.'+inputfilename.split('.')[1],"r") as primary:
            content = primary.read()
            primary.close()
        with open("result"+'.'+inputfilename.split('.')[1],"a") as answer:
            answer.writelines(content)
            answer.close()

    os.system('cat ' "result"+'.'+inputfilename.split('.')[1] + '| sort -k 1,1 | python3 ' +reducerpath+' > reduceroutput.txt')
    print("MapReduce Job Done")


    
 