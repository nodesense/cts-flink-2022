flink-conf.yaml 

changed rest.port = 8282












$FLINK_HOME/bin/start-cluster.sh
open browser, http://localhost:8282

open terminal

  cd workshop

  jupyter lab 


How to run job 
 

-m means master

```
$FLINK_HOME/bin/flink  run -m localhost:8282 -py /home/rps/workshop/notebooks/HelloWorld.py

```

# checkpoint - managed by flink internallly, on the event of error,
the checkpoint is used to continue the operations where flink left before crash..



```
$FLINK_HOME/bin/flink  run -m localhost:8282 -py /home/rps/workshop/notebooks/HelloWorldStream.py

```

Parallism for cluster linked with task slot

Task manager 1, task slots - 1 task manager config 
    4 CPUs ==> 8 vCPU [Hyperthreading],split the actual physical core into 2 logical core by hardware level

    8 vCPU as input 

    How to configure task slot

        Two Different type of Tasks 
            1. IO Bound [Data Intensive, load data from DB, S3, Hadoop Disk/network] - delay expected to retrive data, CPU is much faster
                8 vCPU = x2  [16 Task slot]
                8 cVPUs x3 or x4 [24 task slot or 32 slot]

            2. CPU Bound [ML, Graph Computing, Video Analytics, GPU]

                8 vCPU = 8 slot 

    Where to configure task slot, in the flink-conf.yaml file

Task Manager 2 - running differnt VM

    16 vCPU - 2x - 32 Task Slots 


Task Manager 3.....4...N

Total Slots = Slots from Task manager 1 + Slots from Task Manager 2
                  16 slot     + 32 slots = 48 slots avaialble in cluster

------------------------------
# set parallelism

This is for your flink job to run.. may vary from app to app, depends on resource requirements..one flink app needs 2 paralllism, where other needs 4

env.set_parallelism(1)

1 parallism - 1 task slot is used, to run the tasks
2 parallisms = 2 task slot to be used [same task manager, or differnt task manager], depends on avaiablity 

-------------

# Flink Clinet 

Flink client runs the flink application, 
    1. parse sql/execute python code
    2. When it sees execute statement or statemenet completion in .sql , 
            it creates a job , each execute will have a job created

    3. If your flink code, has 1 execute() then 1 job created
        if your flink code has 10 excute statements, 10 job created

        Each job is submitted to flink cluster, ie Flink JobManager (8282/default 8081) as  Data Flow [converted from DAG [Directed Asyclic Graph])



# Job

Flink execute statements create jobs

.execute
.execute_sql etc 

# Task Pipeline


Parallism drives the partitions in flink 

=============================================
Kafka - Partitions - scaling [Input partititions] = 1

Flink application - parallism = 1

Kafka - output - [write result of flink into kafka] = 1

Pipeline 

  read from kafka [ slot 0]
  process data using flink [slot 0]
  write to kafka [slot 0]

======================



=============================================
lagging of 50 msg per second

Kafka - Partitions - scaling [Input 2 partititions] = 2 parttitions [100 msg per sec]

Flink application - parallism = 1 - 1 task slot used [50 msg per sec],
                        every sec 50 msg shall be lagging

Kafka - output - [write result of flink into kafka] = 1 [50 msg per sec]

Pipeline 

  read from kafka [ slot 0] from 2 parttitions 
  process data using flink [slot 0]
  write to kafka [slot 0]

======================


=============================================
set parallism = 2
Kafka - Partitions - scaling [Input 2 partititions] = 2 parttitions [100 msg per sec]

Flink application - parallism = 2  - 2 task slots used 
                                [each task slot process 50 msg per sec],
                        every sec 100 msg shall be processed

Kafka - output - [write result of flink into kafka] = 1 [100 msg per sec]

Pipeline 1

  read from kafka [ slot 0] from 1 parttitions [partition 0]
  process data using flink [slot 0]
  write to kafka [slot 0]

Pipeline 2

  read from kafka [ slot 1] from 1 parttitions [partition 1]
  process data using flink [slot 1]
  write to kafka [slot 1]

  problem: writing to kafka goes to single parttition, if that is slow,
            it will affect performance
======================



to balance data across tasks, we have api,
    1 task slot too busy, 1 task slot, not much tasks to perform

Input Partition 0: 100 K messages
Input Partition 1: 10 K messages 

Shuffle, about 55k msgs pulled from partition 0 task queue, added to other slot in flink partitions

Data order will not be guaranteed, this randomly picks values

1. Shuffle the data across task slots - random elements are picked, distribute uniformly 

ds.shuffle() - this ds shall shuffle data

---------------

Rebalance

partititions elements round robin basic, creating equal load per partitiion.
the first message send to first channel

ds.rebalance()

-----

rescale 

automatic mapping, betwene input to tasks, tasks to output


----

{"tag": "sensor1", "value": 32.3}
{"tag": "sensor2", "value": 2}
{"tag": "sensor3", "value": 3}
{"tag": "sensor4", "value": 4}
{"tag": "sensor5", "value": 5}
