import schedule
import time
import threading 
#Kafka to be configured on local machine and then run
from kafka import KafkaConsumer
from kafka import KafkaProducer
import json
import logging
# logging.config(bas)
#cron job format (day to be added later)
logging.basicConfig(filename='scheduler1.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',filemode='w')
def json_serializer(data):
    return json.dumps(data).encode("utf-8")
consumer=KafkaConsumer("scheduled_apps1",bootstrap_servers=' localhost:9092',auto_offset_reset='latest',group_id='consumer-group-a')

producer=KafkaProducer(bootstrap_servers=[' localhost:9092'],api_version=(0, 10, 1),
                       value_serializer=json_serializer)
def cron(app_id,instance_id,start_time,end_time,day):
    # logging.info(day+ start_time+ end_time)
    if day == "monday":
        schedule.every().monday.at(start_time).do(deployer_message, app_id,instance_id,0)
        schedule.every().monday.at(end_time).do(deployer_message, app_id,instance_id,1)
    if day == "tuesday":
        schedule.every().tuesday.at(start_time).do(deployer_message, app_id,instance_id,0)
        schedule.every().tuesday.at(end_time).do(deployer_message, app_id,instance_id,1)
    if day == "wednesday":
        schedule.every().wednesday.at(start_time).do(deployer_message, app_id,instance_id,0)
        schedule.every().wednesday.at(end_time).do(deployer_message, app_id,instance_id,1)
    if day == "thursday":
        schedule.every().thursday.at(start_time).do(deployer_message, app_id,instance_id,0)
        schedule.every().thursday.at(end_time).do(deployer_message, app_id,instance_id,1)
    if day == "friday":
        schedule.every().friday.at(start_time).do(deployer_message, app_id,instance_id,0)
        schedule.every().friday.at(end_time).do(deployer_message, app_id,instance_id,1)
    if day == "saturday":
        schedule.every().saturday.at(start_time).do(deployer_message, app_id,instance_id,0)
        schedule.every().saturday.at(end_time).do(deployer_message, app_id,instance_id,1)
    if day == "sunday":
        schedule.every().sunday.at(start_time).do(deployer_message, app_id,instance_id,0)
        schedule.every().sunday.at(end_time).do(deployer_message, app_id,instance_id,1)

#app to be run by deployer. Communication medium to be made later
def deployer_message(app_id,instance_id,flag):
    # print("hello")
    logging.info("in deployer function")
    if flag == 0:
        logging.info(str(app_id) + " Start sent to deployer")
        #line to be activated when kafka is configured
        data={
            "app_id":app_id,
            "instance_id":instance_id,
            "status":"start"
        }
        print("application id sent to deployer")
        
        producer.send('scheduler_to_deployer', data)
        # data[] = scheduling info & instance ID
    else:
        data={
            "app_id":app_id,
            "instance_id":instance_id,
            "status":"stop"
        }
        print("application id sent to deployer")
        logging.info(str(app_id) + " End sent to deployer")
        producer.send('scheduler_to_deployer', data)

def decode_json(app_crons):
    logging.info("decoding_json")
    print(app_crons)
    app_id = app_crons["app_id"]
    sched_flag = app_crons["sched_flag"]
    sched_info = app_crons["schedule_info"]["timings"]
    instance_id=app_crons["instance_id"]
    if sched_flag == 0:
        deployer_message(app_id,0)
        return
    else:
        logging.info("hellooo")
        for day in sched_info:
            start_time = sched_info[day]["start_hour"]
            end_time = sched_info[day]["end_hour"]
            if start_time!="" and end_time!="":
                cron(app_id,instance_id,start_time,end_time,day)
def get_schedule_info_thread():
    logging.info("starting the consumer")
    # print("adsf")
    for msg in consumer:
        print("scheduling info received")
        app_crons = json.loads(msg.value)
        logging.info(app_crons)
        decode_json(app_crons)

#data input format : app_id, schedule_datetimestamp_start, schedule_datetimestamp_start
def run_pending_jobs_thread():
    logging.info("inside run pending jobs")
    i=0
    while True:
        schedule.run_pending()
        # time.sleep(1)

if __name__ == "__main__":
    logging.info("[*] SCHEDULER ON")
    # Create threads
    get_schedule_thread = threading.Thread(target=get_schedule_info_thread)
    run_pending_thread = threading.Thread(target=run_pending_jobs_thread)
    # Start threads
    get_schedule_thread.start()
    run_pending_thread.start()

    # Join threads to wait for them to complete
    # get_schedule_thread.join()
    # run_pending_thread.join()

    print("All threads finished")
