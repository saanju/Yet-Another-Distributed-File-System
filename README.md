# Yet-Another-Distributed-File-System

## Procedure To Run

### Using Docker Containers

1.Start [Docker Desktop](https://www.docker.com/products/docker-desktop/) \
2.cd into directory containing repository \
3.Run 
```sh
docker pull hemabhushan/yadfs-node
```
4.(Use only to build docker image from Dockerfile)  Ignore this step if image is pulled through docker hub
```sh
docker build -t yadfs-node -f .\Dockerfile3 .
```
5.Start Docker Containers   (Here ubuntu=Number of Datanodes + Namenode)
```sh
docker-compose up -d --scale ubuntu=6
```
6.Retrieve IP Addresses of docker containers by running DockerNetwork.py which stores IPAddresses to config.json  (Locally in Host OS)
```sh
python DockerNetwork.py
```
7.Use separate terminals to run Client, Namenode and Datanodes, follow this sequence \
To Start Datanode n (n=1 is reserved for Namenode and n=2 is reserved for Client) use
```sh
docker exec -it yet-another-distributed-file-system-ubuntu-n python3 /usr/files/DataNode.py
```
To Start Namenode
```sh
docker exec -it yet-another-distributed-file-system-ubuntu-1 python3 /usr/files/NameNode.py
```
To Start Client (Make sure the files you want to Upload to the DFS is in the files folder)
```sh
docker exec -it yet-another-distributed-file-system-ubuntu-2 python3 /usr/files/Client.py
```
