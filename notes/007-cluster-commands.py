open terminal 

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
