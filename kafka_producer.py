import time
import json
import argparse
from kafka import KafkaProducer
from uxsim import World

# Convert values to JSON-safe types

def convert(obj):
    if isinstance(obj, (int, float, str)):
        return obj
    if hasattr(obj, "item"):
        return obj.item()
    return obj

# Build simulation world
def build_world():
    W = World(
        name="uxsim-kafka",
        deltan=15,
        tmax=60,
        print_mode=0,
        save_mode=0,
        show_mode=0
    )

    # Nodes
    N1 = W.addNode("N1", 0, 1)
    N2 = W.addNode("N2", 1, 1)
    N3 = W.addNode("N3", 2, 1)

    S1 = W.addNode("S1", 0, 0)
    S2 = W.addNode("S2", 1, 0)
    S3 = W.addNode("S3", 2, 0)

    # Links
    W.addLink("N1S1", N1, S1, length=500, free_flow_speed=50)
    W.addLink("N2S2", N2, S2, length=500, free_flow_speed=50)
    W.addLink("N3S3", N3, S3, length=500, free_flow_speed=50)

    # Demand
    W.adddemand(N1, S1, 0, 600, 5)
    W.adddemand(N2, S2, 0, 600, 5)
    W.adddemand(N3, S3, 0, 600, 5)

    return W

# Run full simulation 
def run_simulation(W):
    W.exec_simulation()
    return W.analyzer.vehicles_to_pandas()


# Kafka Producer
def kafka_producer_loop(interval, topic="vehicle_positions"):

    producer = KafkaProducer(
        bootstrap_servers="localhost:19092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5,
        acks="all"
    )

    print("Kafka producer started")

    W = build_world()
    df = run_simulation(W)

    if df is None or len(df) == 0:
        print("No data generated")
        return

    print(f"Simulation ready: {len(df)} records")

    try:
        while True:

            # Filter moving vehicles
            moving = df[df["v"].fillna(0) > 0]

            batch_size = 0

            for _, row in moving.iterrows():
                msg = {k: convert(v) for k, v in row.to_dict().items()}
                producer.send(topic, msg)
                batch_size += 1

            producer.flush()
            print(f"Sent batch: {batch_size} records")

            time.sleep(interval)

    except Exception as e:
        print("Producer error:", e)

    finally:
        producer.close()

# Main
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--interval", type=int, default=5)
    args = parser.parse_args()

    kafka_producer_loop(args.interval)