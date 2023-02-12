import json

workers_path = "/home/PES2UG20CS026/YetAnotherMapReduce/WORKER/"
master_path= "/home/PES2UG20CS026/YetAnotherMapReduce/MASTER/"
worker_logpath = "/home/PES2UG20CS026/YetAnotherMapReduce/WORKER/WORKER_LOGS.txt"
master_logpath = "/home/PES2UG20CS026/YetAnotherMapReduce/MASTER/MASTER_LOGS.txt"


def read(fileName):
	workerpath_metaData = master_path + 'workers_metaData.json'
	inputFilePath_metaData = master_path + 'inputFiles_metaData.json'
	f = open(inputFilePath_metaData)
	data=json.load(f)
	splits_of_inputFile=[]
	try:
		for i in range(1, data[fileName][0] + 1):
			splits_of_inputFile.append(workers_path + data[fileName][i][2] + '/' + data[fileName][i][1] + '.' + fileName.split('.')[1]) 
	except:
		print("File cannot be found!")

	for splits in splits_of_inputFile:
		file1 = open(splits,'r')

		lines = file1.readlines()
		for line in lines:
			print(line.strip())

		file1.close()
	




		
		
