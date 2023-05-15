# Kafka producer streaming book line by line
import sys
from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer, KafkaConsumer, KafkaClient
import time

admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092")

# reset topic
topicName = "BookStream"
try:
    time.sleep(1) 
    admin_client.delete_topics([topicName])
    print("Topic deleted")
    time.sleep(1) 
except Exception as e:
    print(e)
topic = NewTopic(name=topicName, num_partitions=1, replication_factor=1)
admin_client.create_topics(new_topics=[topic], validate_only=False)
print("Topic created")

#create producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

with open ("boek.txt", "r") as myfile:
    data = myfile.readlines()
    print("boek is started: ", len(data), "lines read")
    # read and send data line by line
    for line in data:
        producer.send(topicName, line.encode('utf-8'))
        time.sleep(1) 
        
producer.close()
