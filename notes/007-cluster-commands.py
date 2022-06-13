

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
