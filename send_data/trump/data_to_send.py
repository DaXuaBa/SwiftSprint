from kafka import KafkaProducer
from datetime import datetime
import time
from json import dumps
import pandas as pd
from configparser import ConfigParser

conf_file_name = "X:\\2023-24\\Nam_4_HK2\\github\SwiftSprint\\send_data\\trump\\stream_app.conf"
config_obj = ConfigParser()
config_read_obj = config_obj.read(conf_file_name)

kafka_host_name = config_obj.get('kafka', 'host')
kafka_port_no = config_obj.get('kafka', 'port_no')
kafka_topic_name = config_obj.get('kafka', 'input_topic_name')

KAFKA_TOPIC_NAME_CONS = kafka_topic_name
KAFKA_BOOTSTRAP_SERVERS_CONS = kafka_host_name + ':' + kafka_port_no

if __name__ == "__main__":
    print("Kafka Producer Application Started ... ")

    kafka_producer_obj = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
                                       value_serializer=lambda x: dumps(x).encode('utf-8'))

    data = pd.read_csv("X:\\2023-24\\Nam_4_HK2\\hashtag_donaldtrump.csv", usecols=['tweet', 'state', 'state_code'], index_col=None, header=0, engine='python')

    states = [
        'Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado',
        'Connecticut', 'Delaware', 'Florida', 'Georgia', 'Hawaii', 'Idaho',
        'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana',
        'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi',
        'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire', 'New Jersey',
        'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma',
        'Oregon', 'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota',
        'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington',
        'West Virginia', 'Wisconsin', 'Wyoming'
    ]

    message = None
    for i, row in data.iterrows():
        message = {}
        message["tweet"] = row['tweet']
        message["state"] = row['state']
        message["state_code"] = row['state_code']
        
        if message["state"] in states:
            print("Message: ", message)
            kafka_producer_obj.send(KAFKA_TOPIC_NAME_CONS, message)
            time.sleep(1)

    print("Kafka Producer Application Completed. ")