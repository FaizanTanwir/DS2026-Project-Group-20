''' This script is producing unlimited data for a production environment. '''

''' MQTT-based device simulator '''

import random
import time
from datetime import datetime
import threading
import json
import paho.mqtt.client as mqtt

BROKER = "127.0.0.1"
PORT = 1883
TOPIC = "iot/sensors"

client = mqtt.Client()
client.connect(BROKER, PORT, 60)
client.loop_start()


def simulate_device(device_id):
    while True:
        data = {
            "device_id": f"sensor_{device_id}",
            "temperature": random.uniform(20, 80),
            "humidity": random.uniform(30, 90),
            "timestamp": datetime.now().isoformat()
        }

        client.publish(TOPIC, json.dumps(data))
        print(f"{device_id} → published")

        time.sleep(random.uniform(0.5, 2))


def start_simulation(device_count=100):
    threads = []

    for i in range(device_count):
        t = threading.Thread(target=simulate_device, args=(i,))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()


if __name__ == "__main__":
    start_simulation(100)
