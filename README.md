# Yet-Another-Distributed-File-System

## Procedure To Run

### Using Docker Containers

1.Start [Docker Desktop](https://www.docker.com/products/docker-desktop/) \
2.cd into directory containing repository \
3.Run 
```sh
docker pull
```
4.(Use only to build docker image from Dockerfile)Ignore this step if image is pulled through docker hub
```sh
docker build -t yadfs-node -f .\Dockerfile3 .
```
5.Start Docker Containers(Here ubuntu=Number of Datanodes + Namenode)
```sh
docker-compose up -d --scale ubuntu=3
```
6.Retrieve IP Addresses of docker containers by running DockerNetwork.py which stores IPAddresses to config.json  (Locally in Host OS)
```sh
python DockerNetwork.py
```
