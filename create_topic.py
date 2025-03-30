from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config

# Створення клієнта Kafka
admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)

# Визначення топіків з префіксом
my_name = "Sergii_S"  # Префікс для унікальності
topics = {
    f"{my_name}_building_sensors": {"partitions": 2, "replication": 1},
    f"{my_name}_temperature_alerts": {"partitions": 2, "replication": 1},
    f"{my_name}_humidity_alerts": {"partitions": 2, "replication": 1}
}

# Отримуємо список існуючих топіків
existing_topics = admin_client.list_topics()

# Створюємо нові топіки, якщо вони ще не існують
new_topics = []
for topic_name, config in topics.items():
    if topic_name not in existing_topics:
        new_topics.append(NewTopic(
            name=topic_name, 
            num_partitions=config["partitions"], 
            replication_factor=config["replication"]
        ))

# Додаємо нові топіки
if new_topics:
    try:
        admin_client.create_topics(new_topics=new_topics, validate_only=False)
        print("Topics created successfully:", [t.name for t in new_topics])
    except Exception as e:
        print("An error occurred while creating topics:", e)
else:
    print("All topics already exist.")

# Перевіряємо тільки свої топіки (ті, що починаються з 'Sergii_S_')
print("\nYour topics:")
for topic in admin_client.list_topics():
    if topic.startswith(my_name):  # Фільтруємо за префіксом
        print(topic)

# Закриваємо клієнт
admin_client.close()





