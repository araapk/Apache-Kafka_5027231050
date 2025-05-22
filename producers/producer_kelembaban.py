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
        kelembaban = random.randint(60, 80)  # simulasi kelembaban
        data = {"gudang_id": gudang, "kelembaban": kelembaban}
        producer.send('sensor-kelembaban-gudang', value=data)
        print(f"Kirim data kelembaban: {data}")
        time.sleep(1)