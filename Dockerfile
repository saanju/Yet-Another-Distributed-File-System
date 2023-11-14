FROM redis:latest

RUN apt-get update 

RUN apt-get install -y python3-pip

RUN cd /usr/local/bin 

RUN ln -s /usr/bin/python3 python 

COPY ./requirements.txt ./requirements.txt

RUN pip3 install -r requirements.txt

ENTRYPOINT ["sleep", "infinity"]