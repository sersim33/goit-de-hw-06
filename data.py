from kafka import KafkaConsumer
from configs import kafka_config
import json

# Створення Kafka Consumer
consumer = KafkaConsumer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')) if v else None,  # Перевірка на порожнє значення
    key_deserializer=lambda v: json.loads(v.decode('utf-8')) if v else None,  # Перевірка на порожнє значення
    auto_offset_reset='earliest',  # Зчитування повідомлень з початку
    enable_auto_commit=True,       # Автоматичне підтвердження зчитаних повідомлень
    group_id='my_consumer_group_2'   # Ідентифікатор групи споживачів
)

# Підписка на два топіки
topic_names = ['Sergii_S_temperature_alerts', 'Sergii_S_humidity_alerts']
consumer.subscribe(topic_names)

print(f"Subscribed to topics: {', '.join(topic_names)}")

# Обробка повідомлень з топіків
try:
    for message in consumer:
        if message.value:  # Перевірка на наявність значення в повідомленні
            print(f"Received message from topic '{message.topic}': {message.value}")
        else:
            print(f"Empty message received from topic '{message.topic}'")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer.close()  # Закриття consumer

