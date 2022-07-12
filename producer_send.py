"""
Date : 2022-07-12
Author : 주현석
Description : event hub에 Data 보내는 샘플 입니다.
"""
import psutil, datetime, socket, json, os
from azure.eventhub import EventHubProducerClient, EventData

# config 정보 json 파일 읽기
f = open("./config.json", "r") 
json_data = json.load(f)

# Extract Event Hub Connection String and Event Hub Instance name
connection_str = json_data['conn_str']
event_hub_path = json_data['event_hub_path']

# create producer 
producer = EventHubProducerClient.from_connection_string(conn_str=connection_str, eventhub_name=event_hub_path)
# get os hostname
hostname = socket.gethostname()

try:
    # loop send data to producer
    while True:
        event_data_batch = producer.create_batch()
        cpu_percent = psutil.cpu_percent(2)
        mem_usage = psutil.virtual_memory()[2]
        reading = {'hostname': hostname
            , 'timestamp': str((datetime.datetime.now()).strftime("%Y-%m-%d %H:%M:%S"))
            , 'cpu_usage': cpu_percent, 'mem_usage': mem_usage
        }

        s = json.dumps(reading)
        print(s)
        event_data_batch.add(EventData(s))
        producer.send_batch(event_data_batch)

except KeyboardInterrupt:
    # exit loop and close producer
    producer.close()


"""
Event Hub에 보내는 순서 
1. Producer client 생성 with connection string, event hub instance 이름
2. event_data_batch 생성
3. 보낼 데이터 정의 
4. event_data_batch에 add
5. producer.send_batch 에 event_data_batch 넣어서 보내기
"""