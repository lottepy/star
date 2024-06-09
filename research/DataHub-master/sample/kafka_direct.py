import time
import json
from kafka.consumer import KafkaConsumer
consumer = KafkaConsumer('market_orderbook', bootstrap_servers=['47.91.249.15:9092'])
iuids = ['SG_60_CNX8']
while True:
	for record in consumer:
		if record.key.decode() in iuids:
			print(json.loads(record.value.decode()))
	time.sleep(5)
