import json, requests, os
from os.path import dirname
from azure.eventhub import EventHubProducerClient, EventData

# binance api 정의
api = "https://api1.binance.com/api/v3/ticker?symbols="
symbols = '["AVAXUSDT","BNBUSDT","DOGEUSDT","TRXUSDT","ETHUSDT","DOTUSDT","BTCUSDT","XRPUSDT","ADAUSDT","SOLUSDT"]'
url = api + symbols

# config 정보로 event hub 정보 정의
conf_File_path = os.getcwd() + "\\azure_eventHub_to_stream"  + "\\config.json"
f = open(conf_File_path, "r") 
json_data = json.load(f)
connection_str = json_data['conn_str']
event_hub_path = json_data['event_hub_path']

# create producer 
producer = EventHubProducerClient.from_connection_string(conn_str=connection_str, eventhub_name=event_hub_path)
event_data_batch = producer.create_batch()
# binance api 호출 하여 json 데이터 만들기
res = requests.get(url)
json_data = res.json()

s = json.dumps(json_data)
print(s)
event_data_batch.add(EventData(s))
producer.send_batch(event_data_batch)
producer.close()
