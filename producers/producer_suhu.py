from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

gudang_list = ['G1', 'G2', 'G3']

while True:
    for gudang in gudang_list:
        suhu = random.randint(75, 90)  # simulasi suhu
        data = {"gudang_id": gudang, "suhu": suhu}
        producer.send('sensor-suhu-gudang', value=data)
        print(f"Kirim data suhu: {data}")
        time.sleep(1)