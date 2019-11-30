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