import shutil 
import json
import os
import glob


workers_path = "/home/PES2UG20CS026/YetAnotherMapReduce/WORKER/"
master_path= "/home/PES2UG20CS026/YetAnotherMapReduce/MASTER/"
num_of_workers = 6
worker_logpath = "/home/PES2UG20CS026/YetAnotherMapReduce/WORKER/WORKER_LOGS.txt"
master_logpath = "/home/PES2UG20CS026/YetAnotherMapReduce/MASTER/MASTER_LOGS.txt"


def fileUpload(input):
	workerpath_metaData = master_path + 'workers_metaData.json'
	inputFilePath_metaData = master_path + 'inputFiles_metaData.json'
	file_1 = open(workerpath_metaData)
	file_2 = open(inputFilePath_metaData)
	mataData_of_workers = json.load(file_1)
	metaDataOfInputFiles = json.load(file_2)
	file_1.close()
	file_2.close()
	

	metaDataOfInputFiles[input[0]]=list()
	metaDataOfInputFiles[input[0]].append(input[1])
	splitNumberCount=1
	
	for i in range(1, num_of_workers + 1):
		
			
		
			
		source = input[splitNumberCount+1]
		destination = workers_path + "worker{}".format(i)
			
		
		shutil.move(source, destination)
		

			

			

		splitName=input[splitNumberCount+1].split('/')[-1].split('.')[0]
		splitInfo=list()
		splitInfo.append(splitNumberCount)
		splitInfo.append(splitName)   
		splitInfo.append('worker{}'.format(i))      		
		metaDataOfInputFiles[input[0]].append(splitInfo)

			
		if (splitNumberCount == input[1]): 
			message = "File succesfully written to worker nodes!"
			break
		splitNumberCount += 1

	file_1 = open(workerpath_metaData, 'w')
	file_2 = open(inputFilePath_metaData, 'w')
	file_1.write(str(json.dumps(mataData_of_workers, indent=4)))
	file_2.write(str(json.dumps(metaDataOfInputFiles, indent=4)))
	file_1.close()
	file_2.close()
	
	return message


def split(filePath):
    input=[]  
    try:
        shutil.rmtree('temp')
    except:
        pass
    os.mkdir('temp')

    fileName=filePath.split('/')[-1]

   
    os.system('split -n l/{} '.format(num_of_workers) + filePath + ' ./temp/')

    files = glob.glob('./temp/*')
    files.sort() 

    input.append(fileName)
    input.append(len(files))
    i = 1
    for file in files:
        newFilePath='./temp/'+fileName.split('.')[0]+'*{}'.format(i)+'.'+fileName.split('.')[1]
        os.rename(file, newFilePath)  
        splitPath=newFilePath
        i += 1
        input.append(splitPath)
    message = fileUpload(input)
    return message

