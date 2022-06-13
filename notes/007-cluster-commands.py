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
