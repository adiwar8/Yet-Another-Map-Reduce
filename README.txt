This is the Big data project submitted by: Aditya R Shenoy - PES2UG20CS025
                                           Aditya R Warrier - PES2UG20CS026
                                           Monica Sreevalli - PES2UG20CS084
                                           
Topic: Yet Another Map Reduce (YaMR)


We start the execution by running python3 startmasterworker.py.
This starts the master and worker nodes on the local machines

Then, we run client.py to assign work to the worker nodes or write/read from them.
The master and worker nodes are active on the machine until we exit the client.

read.py describes the reading input data  from the worker nodes.

write.py describes writing (inputting data) to the worker nodes.

mapreduce.py describes the map-reduce job we perform on the input data.

we run a mapreduce job on the client with the syntax:
runmapreducejob -i <absolute path of input file> -o <absolute path of output> -m <mapper absolute path> -r <reducer absolute path>

The inputted json/txt file is split into equal partitions ammong all the worker nodes.

Each of the json/txt file partitions on the worker nodes are passed through the mapper.

The outputs of the mappers are then passed through the reducer file.

The out of the reducer is saved to the local directory as reduceroutput.txt.


