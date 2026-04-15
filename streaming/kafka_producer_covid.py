import time
import json
import random
from kafka import KafkaProducer

def generate_covid_data():
    return {
        "departamento": random.choice(["Valle", "Antioquia", "Bogotá", "Cundinamarca", "Atlántico"]),
        "casos": random.randint(0, 500),
        "muertes": random.randint(0, 20),
        "recuperados": random.randint(0, 400),
        "timestamp": int(time.time())
    }

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

while True:
    data = generate_covid_data()
    producer.send('covid19_colombia', value=data)
    print("Enviado:", data)
    time.sleep(1)
