import argparse
import time
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError

def create_kafka_topic(bootstrap_servers="localhost:9092", topic_name="logs", num_partitions=1, replication_factor=1, recreate=False):
    print(f"[TopicAdmin] Connecting to Redpanda/Kafka at {bootstrap_servers}...")
    
    # Retry loop to wait for Redpanda broker readiness
    admin_client = None
    for attempt in range(10):
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=bootstrap_servers,
                client_id="topic_admin_client"
            )
            print("[TopicAdmin] Successfully connected to broker.")
            break
        except Exception as e:
            print(f"[TopicAdmin] Broker not ready yet ({e}), retrying in 2 seconds... (Attempt {attempt+1}/10)")
            time.sleep(2)
            
    if not admin_client:
        raise RuntimeError("Failed to connect to Kafka broker after multiple attempts.")
        
    try:
        if recreate:
            try:
                print(f"[TopicAdmin] Deleting existing topic '{topic_name}'...")
                admin_client.delete_topics([topic_name])
                time.sleep(1) # wait for deletion propagate
                print(f"[TopicAdmin] Topic '{topic_name}' deleted.")
            except UnknownTopicOrPartitionError:
                print(f"[TopicAdmin] Topic '{topic_name}' did not exist.")
            except Exception as e:
                print(f"[TopicAdmin] Warning deleting topic: {e}")

        topic_list = [NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)]
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print(f"[TopicAdmin] Topic '{topic_name}' created successfully with {num_partitions} partition(s) and replication factor {replication_factor}.")
    except TopicAlreadyExistsError:
        print(f"[TopicAdmin] Topic '{topic_name}' already exists.")
    except Exception as e:
        print(f"[TopicAdmin] Error creating topic: {e}")
    finally:
        admin_client.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create Kafka/Redpanda topic.")
    parser.add_argument("--broker", type=str, default="localhost:9092", help="Kafka broker address")
    parser.add_argument("--topic", type=str, default="logs", help="Topic name")
    parser.add_argument("--partitions", type=int, default=1, help="Number of partitions")
    parser.add_argument("--replication", type=int, default=1, help="Replication factor")
    parser.add_argument("--recreate", action="store_true", help="Recreate topic if it exists")
    args = parser.parse_args()

    create_kafka_topic(
        bootstrap_servers=args.broker,
        topic_name=args.topic,
        num_partitions=args.partitions,
        replication_factor=args.replication,
        recreate=args.recreate
    )
