
from kafka import KafkaConsumer, KafkaProducer
from configs import kafka_config
import json
import time

# Створення Kafka Consumer
consumer = KafkaConsumer(
    'Sergii_S_building_sensors',  # Підписка на топік
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')) if v else None,
    key_deserializer=lambda v: json.loads(v.decode('utf-8')) if v else None,
    auto_offset_reset='earliest',  # Зчитування повідомлень з початку
    enable_auto_commit=True,       # Автоматичне підтвердження зчитаних повідомлень
    group_id='my_consumer_group_3' # Ідентифікатор групи споживачів
)

# Створення Kafka Producer для відправки сповіщень в топіки 'temperature_alerts' та 'humidity_alerts'
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Топіки для сповіщень
temperature_alert_topic = 'Sergii_S_temperature_alerts'
humidity_alert_topic = 'Sergii_S_humidity_alerts'

# Підписка на топік
print(f"Subscribed to topic 'Sergii_S_building_sensors'")

# Лічильник повідомлень
message_count = 0
max_messages = 10

# Обробка повідомлень з топіку
try:
    for message in consumer:
        if message_count >= max_messages:
            print("Processed 10 messages, stopping consumer.")
            break  # Зупиняємо споживача після 10 повідомлень

        if message.value:  # Перевірка, чи є значення в повідомленні
            data = message.value
            sensor_id = data.get('sensor_id')
            temperature = data.get('temperature')
            humidity = data.get('humidity')
            timestamp = data.get('timestamp')

            # Перевірка температури
            if temperature > 40:
                alert = {
                    "sensor_id": sensor_id,
                    "temperature": temperature,
                    "timestamp": timestamp,
                    "message": f"Temperature threshold exceeded: {temperature}°C"
                }
                producer.send(temperature_alert_topic, value=alert)
                print(f"Temperature alert sent: {alert}")

            # Перевірка вологості
            if humidity > 80 or humidity < 20:
                alert = {
                    "sensor_id": sensor_id,
                    "humidity": humidity,
                    "timestamp": timestamp,
                    "message": f"Humidity threshold exceeded: {humidity}%"
                }
                producer.send(humidity_alert_topic, value=alert)
                print(f"Humidity alert sent: {alert}")

            message_count += 1  # Збільшення лічильника повідомлень
            time.sleep(1)  # Затримка перед обробкою наступного повідомлення
        else:
            print("Received empty message, skipping...")

except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer.close()  # Закриття consumer
    producer.close()  # Закриття producer
