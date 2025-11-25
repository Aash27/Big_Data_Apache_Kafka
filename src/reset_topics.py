# from kafka.admin import KafkaAdminClient, NewTopic
# import os
# from dotenv import load_dotenv
# import time

# load_dotenv()
# BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

# def reset():
#     admin = KafkaAdminClient(bootstrap_servers=BROKER)
#     topics = ["news.raw", "news.cleaned", "news.enriched", "news.filtered"]
    
#     print("Deleting old topics to clear ghost data...")
#     try:
#         admin.delete_topics(topics)
#         print("Topics deleted. Waiting 5s for Kafka to cleanup...")
#         time.sleep(5)
#     except Exception as e:
#         print(f"Delete warning (might not exist yet): {e}")

#     print("Recreating clean topics...")
#     new_topics = [NewTopic(name=t, num_partitions=1, replication_factor=1) for t in topics]
#     admin.create_topics(new_topics)
#     print("Done. Kafka is clean.")

# if __name__ == "__main__":
#     reset()












from kafka.admin import KafkaAdminClient, NewTopic
import os
import time
from dotenv import load_dotenv

load_dotenv()
BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

def reset():
    print("Connecting to Kafka...")
    admin = KafkaAdminClient(bootstrap_servers=BROKER)
    
    topics = ["news.raw", "news.cleaned", "news.enriched", "news.filtered"]
    
    print("1. Deleting old topics (purging bad data)...")
    try:
        admin.delete_topics(topics)
        time.sleep(5) # Wait for Kafka to actually delete them
    except Exception as e:
        print(f"   (Topics might not exist, skipping delete)")

    print("2. Recreating fresh topics...")
    new_topics = [NewTopic(name=t, num_partitions=1, replication_factor=1) for t in topics]
    try:
        admin.create_topics(new_topics)
    except Exception as e:
        print(f"   (Error creating: {e})")
        
    print("Done. The pipeline is clean. You can start the scripts now.")

if __name__ == "__main__":
    reset()
