import random
import time
import csv
import threading
# Define the data format
def generate_sensor_data(sensor_file,sensor_name,i):

    fieldnames = ['sensor_type', 'timestamp', 'value']

    # Create a CSV file to store the data
    with open(sensor_file, mode='w') as sensor_data_file:
        sensor_data_writer = csv.DictWriter(sensor_data_file, fieldnames=fieldnames)
        sensor_data_writer.writeheader()
        sname=sensor_name+"_"+str(i)
        # Continuously generate data points
        k=20
        while k>0:
            # Simulate the temperature sensor
            if sensor_name=="temperature":
                temperature = round(random.uniform(20.0, 30.0), 2)
                temperature_data = {'sensor_type': sname, 'timestamp': time.time(), 'value': temperature}
                sensor_data_writer.writerow(temperature_data)
                print(temperature_data)

            
            # Simulate the humidity sensor
            if sensor_name=="humidity":
                humidity = round(random.uniform(40.0, 60.0), 2)
                humidity_data = {'sensor_type': sname, 'timestamp': time.time(), 'value': humidity}
                sensor_data_writer.writerow(humidity_data)
                print(humidity_data)
            if sensor_name=="aqi":
                aqi = round(random.uniform(0, 500), 2)
                aqi_data =  {'sensor_type': sname, 'timestamp': time.time(), 'value': aqi}
                sensor_data_writer.writerow(aqi_data)
                print(aqi_data)
            
            # Wait for 5 seconds before generating the next data point
            time.sleep(0.5)
            k-=1

def main():
    temp=int(input("enter number of temperature sensors"))
    humidity=int(input("enter number of humidity sensors"))
    aqi=int(input("enter number of aqi sensors"))
    for i in range(1,temp+1):
        fname='temperature_'+str(i)+'.csv'
        # t1 = threading.Thread(target=generate_sensor_data,args=(fname,"temperature",))
        # t1.start()
        generate_sensor_data(fname,"temperature",i)
    for i in range(1,humidity+1):
        fname='humidity_'+str(i)+'.csv'
        # t1 = threading.Thread(target=generate_sensor_data,args=(fname,"humidity",))
        # t1.start()
        generate_sensor_data(fname,"humidity",i)
    for i in range(1,aqi+1):
        fname='aqi_'+str(i)+'.csv'
        # t1 = threading.Thread(target=generate_sensor_data,args=(fname,"aqi",))
        # t1.start()
        generate_sensor_data(fname,"aqi",i)



if __name__=="__main__":
    main()