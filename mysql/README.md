Download jar files

```
wget -P $FLINK_HOME/lib https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/1.15.0/flink-connector-jdbc-1.15.0.jar
wget -P $FLINK_HOME/lib https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.29/mysql-connector-java-8.0.29.jar
```




$FLINK_HOME/bin/start-cluster.sh

listen on port 8081 default change to other by 
    nano $FLINK_HOME/conf/flink-conf.yaml
        rest.port: 8282

```
$FLINK_HOME/bin/sql-client.sh -f /home/krish/flink/mysql/mysql.sql
```

to enter into flink sql mode

```
$FLINK_HOME/bin/sql-client.sh 
```


```
 ./bin/flink run -m localhost:8282 -py /home/krish/flink/json-read.py

```

sudo apt install openssh-server

# HIVE

https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/table/hive/overview/#dependencies

https://mvnrepository.com/artifact/org.apache.flink/flink-connector-hive_2.12/1.15.0
https://mvnrepository.com/artifact/org.apache.hive/hive-exec/2.3.8
https://mvnrepository.com/artifact/org.antlr/antlr-runtime/3.5.2


