from datetime import UTC, datetime
import time
import random
import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def generate_trafic_data(sensor_id):
    return {
        "sensor_id": sensor_id,
        "timestamp": datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%S"),
        "vehicle_count": random.randint(0, 50),
        "average_speed": round(random.uniform(10, 100), 2),
        "congestion_level": random.choice(["LOW", "MEDIUM", "HIGH"]),
    }

if __name__ == "__main__":
    try:
        print("Sending Fake traffic data")
        while True:
            for sensor_id in range(101, 106):
                event = generate_trafic_data(sensor_id)
                producer.send("trafic_data", value=event)
                # print(event)
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopped sending data.")
    except Exception as e:
        print("problem sending trafic data")
