

open terminal 


###  Hadooo cluster

start cluster

```
$HADOOP_HOME/sbin/start-all.sh
```

```
jps
```

check namenode, datanode running

stop cluster

```
$HADOOP_HOME/sbin/stop-all.sh
```

## Kafka , ZooKeeper, Schema Registry

start only, Kafka , ZooKeeper, Schema Registry

```
confluent local start zookeeper
confluent local start kafka
confluent local start schema-registry
    
confluent local status

```

### Flink cluster

port config 

flink-conf.yaml 


```
rest.port = 8282
```

Start Cluster

```

$FLINK_HOME/bin/start-cluster.sh
```



```

$FLINK_HOME/bin/stop-cluster.sh
```

open browser, http://localhost:8282


Submit job

```
$FLINK_HOME/bin/flink  run -m localhost:8282 -py /home/rps/workshop/notebooks/HelloWorld.py
```



# checkpoint - managed by flink internallly, on the event of error,
the checkpoint is used to continue the operations where flink left before crash..



```
$FLINK_HOME/bin/flink  run -m localhost:8282 -py /home/rps/workshop/notebooks/HelloWorldStream.py

```

open http://localhost:8282
    
 Check in hadoop,


http://localhost:50070
    
    under browse directories
    
    
 ## KAkfa producer

```

{"tag": "sensor1", "value": 32.3}
{"tag": "sensor2", "value": 2}
{"tag": "sensor3", "value": 3}
{"tag": "sensor4", "value": 4}
{"tag": "sensor5", "value": 5}

do save point

{"tag": "sensor6", "value": 6}
{"tag": "sensor7", "value": 7}
{"tag": "sensor8", "value": 8}
```
    
## Save Point


```
$FLINK_HOME/bin/flink  run -m localhost:8282 -py /home/rps/workshop/notebooks/Kafka-SavePoint.py

```

result 

```
Job has been submitted with JobID 63fa92f627685a4c0831d9137e2e0e9e

```

Look for job id printedo n console, and copy the job id for further commands

Failure may happen unplanned or unexpected way..

Good to save save points periodically using job id [scheduler]

### Trigger a save point using commmand

```
$FLINK_HOME/bin/flink savepoint 63fa92f627685a4c0831d9137e2e0e9e hdfs://localhost:9000/savepoints/63fa92f627685a4c0831d9137e2e0e9e/june-13-2022-6-32-pm
        
```

```
Savepoint completed. Path: hdfs://localhost:9000/savepoints/63fa92f627685a4c0831d9137e2e0e9e/june-13-2022-6-32-pm/savepoint-63fa92-072f4f387c4b
```
            

open http://localhost:8282
        
        
        

### Trigger a save point using commmand

```
$FLINK_HOME/bin/flink savepoint 63fa92f627685a4c0831d9137e2e0e9e hdfs://localhost:9000/savepoints/63fa92f627685a4c0831d9137e2e0e9e/june-13-2022-7-09-pm
        
```

```
Savepoint completed. Path: hdfs://localhost:9000/savepoints/63fa92f627685a4c0831d9137e2e0e9e/june-13-2022-7-09-pm/savepoint-63fa92-c183070ae2f8
You can resume your program from this savepoint with the run command.
```
