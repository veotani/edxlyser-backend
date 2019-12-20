## About this project
In this project we try to build a system for online-course data analysis. We are working with Saint-Petersburg State University online-course creators and maintainers to take data from. We aim to solve several problems:
1. Continous data collection. The data is produced by online-course platform. It is hosted on SPSU servers.
2. Data visualisation and analysis.
The analysis within this project is not going to be a final analysis. Our role is to provide representations for different specialists: sociologists, psychologists, etc. 
This system is also going to be used in adaptive learning system. In such learning systems each student gets it's own path to complete a course: for example some need more video lectures meanwhile others learn better by reading.

### Tools
To build this system we are going to use following technologies:
1. Docker
2. Golang
3. ELK (Elasticsearch, Kibana, Logstash)
4. Kafka
5. Kubernetes (in the future)

## Instructions
*Instructions are Linux only*

Assuming you have docker and docker-compose installed.
1. First, give filebeat appropriate rights:
``chmod go-w filebeat/config/filebeat.yml ``
2. Second, run the docker-compose
``sudo docker-compose up -d``
3. Finally, put some edx logs files into "logs" folder

System will start to monitor logs in "logs" folder, then send them to kafka and take this logs in ``processor.go``.

## Plans
There should be many different go services, that take logs from kafka and process them. Unsolved tasks are:
1. Filebeat must send different event types to different topics (can be solved by changing filebeat settings)
2. Add services written in go to take logs from different topics, process them and store in DB
3. Add elasticsearch container to the system to store events
