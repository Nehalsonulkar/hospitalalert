from src.kafka_producer.json_producer import product_data_using_file
from src.constant import SAMPLE_DIR
import os
if __name__ == '__main__':
    
    topics = os.listdir(SAMPLE_DIR)
    print(f'topics: [{topics}]')
    for topic in topics:
        sample_topic_data_dir = os.path.join(SAMPLE_DIR,topic)
        sample_file_path = os.path.join(sample_topic_data_dir,os.listdir(sample_topic_data_dir)[0])
        product_data_using_file(topic=topic,file_path=sample_file_path)

    ## SAMPLE_DIR contains directories, each representing a Kafka topic.
    ## Each topic directory contains at least one file, which is the data file to be sent to Kafka.
    ## product_data_using_file is a function that takes a topic and a file_path and produces the data in the file to the specified Kafka topic.