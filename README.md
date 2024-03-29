Flink Python

open linux terminal 

```
sudo apt install git 

```

```
cd ~
git clone https://github.com/nodesense/cts-flink-2022 workshop

cd workshop

jupyter lab
```

# Setting up flink 

open terminal 

```
cd ~
```

```
wget  https://dlcdn.apache.org/flink/flink-1.15.3/flink-1.15.3-bin-scala_2.12.tgz

tar xf flink-1.15.3-bin-scala_2.12.tgz

sudo rm -rf /opt/flink-1.15.3

sudo mv flink-1.15.3 /opt

sudo chmod 777 -R /opt/flink-1.15.3
```

# update bash rc files

in terminal
```
code ~/.bashrc
```

paste below end of the file


```
export FLINK_HOME=/opt/flink-1.15.3
export CLASSPATH=$CLASSPATH:$FLINK_HOME/lib/*:.
export HADOOP_CLASSPATH=`hadoop classpath`

```


# Start Flink Cluster

```
$FLINK_HOME/bin/start-cluster.sh 
```

# Stop Flink Cluster

```
$FLINK_HOME/bin/stop-cluster.sh 
```



it should open jupyter in firefox or look the url displayed in jupyter lab, copy paste with token

# JAR Locations

1. Parquet   https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/connectors/table/formats/parquet/

https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-parquet/1.15.0/flink-sql-parquet-1.15.0.jar

# PyFlink for Development

```
 python -m venv flink-dev
 cd flink-dev
 source bin/activate
 pip install apache-flink==1.15.3
 pip install jupyterlab
 jupyter-lab
 ```

now check the jupyter link shown on the console, copy paste the same in your browser. 

### Hint to know site-packages location
system wide
```
python -m site

python -c 'import site; print(site.getsitepackages())'

```

python -m site --user-site


The flink lib path is here   

```
/home/krish/flink-dev/lib/python3.8/site-packages/pyflink/lib
```
