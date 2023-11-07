## Install kafka and zookeeper and run them in a docker container
## Run the producer.py file which will send the scheduling information to scheduler.py using the topic name scheduled_apps1.Keep the schedule time accordingly

## Run the scheduler.py file , It will receive the scheduling information sent by producer.py

## It will perform the scheduling task using python library scheduler

## When the schedule time arrives it sends a message to deployer using the kafka topic scheduler_to_deployer which contains application_id,status{start or stop}.

## Based on the message received by deployer it will perform it's task