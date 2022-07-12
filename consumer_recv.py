import asyncio, logging, json
from azure.eventhub import EventHubConsumerClient

# config 정보 json 파일 읽기
f = open("./config.json", "r") 
json_data = json.load(f)

# Extract Event Hub Connection String and Event Hub Instance name
connection_str = json_data['conn_str']
event_hub_path = json_data['event_hub_path']
consumer_group = 'preview_data_consumer_group'
consumer = EventHubConsumerClient.from_connection_string(connection_str, consumer_group, eventhub_name=event_hub_path)


logger = logging.getLogger("azure.eventhub")
logging.basicConfig(level=logging.INFO)

async def on_event(partition_context, event):
    logger.info("Received event from partition {}".format(partition_context.partition_id))
    await partition_context.update_checkpoint(event)

async def main():
    await consumer.receive(on_event=on_event,starting_position="-1")


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())