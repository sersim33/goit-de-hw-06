
from kafka import KafkaProducer
from configs import kafka_config
import json
import uuid
import time
import random

# Створення Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Генерація випадкового ID для датчика
sensor_id = random.randint(1000, 9999)

# Назва топіку
my_name = "Sergii_S"
topic_name = f'{my_name}_building_sensors'

# Надсилання повідомлень до Kafka
for i in range(10):  # Відправка 10 повідомлень
    try:
        # Генерація випадкових даних
        data = {
            "sensor_id": sensor_id,  # ID датчика
            "timestamp": time.time(),  # Часова мітка
            "temperature": random.randint(25, 45),  # Випадкова температура
            "humidity": random.randint(15, 85)  # Випадкова вологість
        }
        
        # Відправлення повідомлення в Kafka
        producer.send(topic_name, key=str(uuid.uuid4()), value=data)
        producer.flush()  # Очікування, поки всі повідомлення будуть відправлені
        
        # Виведення інформації про відправлене повідомлення
        print(f"Message {i+1} sent to topic '{topic_name}' with data: {data}")
        
        # Затримка між відправками
        time.sleep(2)
    except Exception as e:
        print(f"An error occurred: {e}")

# Закриття producer
producer.close()
