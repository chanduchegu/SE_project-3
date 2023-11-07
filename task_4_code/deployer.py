import logging
from kafka import KafkaConsumer
import json
import time
# Set up logger
logging.basicConfig(filename='consumer1.log', level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s',filemode='w')
from kafka import KafkaConsumer
import json
import logging 


if __name__=='__main__':
    consumer=KafkaConsumer("scheduler_to_deployer",bootstrap_servers='  localhost:9092',auto_offset_reset='latest',group_id='consumer-group-a')
    print("starting the deployer")
    
    for msg in consumer:
        # pass
        logging.info("message received")
        print("message received")
        print(msg.value)
        # unzipFile(msg.value)