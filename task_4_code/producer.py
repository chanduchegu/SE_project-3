
from kafka import KafkaProducer
import json
import time
def json_serializer(data):
    return json.dumps(data).encode("utf-8")


producer=KafkaProducer(bootstrap_servers=['localhost:9092'],api_version=(0, 10, 1),
                       value_serializer=json_serializer)

if __name__ == "__main__":
    i=0
    while i<1:
        app_crons = {
		"app_id": 123,
		"sched_flag": 1,
        "instance_id":1,
        "schedule_info":{
                "start_date": "2023-04-19T18:30:00.000Z",
                "end_date": "2023-04-27T18:30:00.000Z",
                "timings": {
                    "monday": {
                    "start_hour": "10:57:30",
                    "end_hour": "10:58:00"
                    },
                    "tuesday": {
                    "start_hour": "11:20:00",
                    "end_hour": "11:20:40"
                    },
                    "wednesday": {
                    "start_hour": "11:32:00",
                    "end_hour": "11:33:00"
                    },
                    "thursday": {
                    "start_hour": "22:35:00",
                    "end_hour": "22:36:00"
                    },
                    "friday": {
                    "start_hour": "",
                    "end_hour": ""
                    },
                    "saturday": {
                    "start_hour": "17:25:00",
                    "end_hour": ""
                    },
                    "sunday": {
                    "start_hour": "",
                    "end_hour": ""
                    }
                }
            }
        }
        print("message sent to scheduler using the kafka topic scheduled_apps1")
        producer.send("scheduled_apps1",app_crons)
        time.sleep(5)
        i+=1

