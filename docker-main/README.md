# docker installation

```
sudo curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose

sudo chmod +x /usr/local/bin/docker-compose

docker-compose --version
```



sudo apt-get update

sudo apt-get install \
    ca-certificates \
    curl \
    gnupg \
    lsb-release

sudo mkdir -p /etc/apt/keyrings

curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg

echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt-get update

sudo apt-get install docker-ce docker-ce-cli containerd.io docker-compose-plugin

apt-cache madison docker-ce

sudo docker run hello-world


# docker

cd docker-main

# start zookeeper

cd zookeeper

docker-compose up -d


# Copy lib directorys

open terminal 


# start flink

cd flink 



```
cp -r $FLINK_HOME/lib .
```

docker build -t flink:test -f Dockerfile .

mkdir ha

chmod 777 ha/

docker-compose up -d

# submit job into the cluster

docker ps -a --> list all running container

docker exec -it [:containerId] /bin/bash

./bin/flink run -m localhost:8081 -py code/HelloWorld.py






sudo apt-get install docker-ce=<VERSION_STRING> docker-ce-cli=<VERSION_STRING> containerd.io docker-compose-plugin
