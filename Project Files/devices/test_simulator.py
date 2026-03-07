''' This script is producing only limited data to test the gateway. It is not a full simulation. '''

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

# Global counter (thread-safe)
total_messages_sent = 0
counter_lock = threading.Lock()


def simulate_device(device_id, messages=50):
    global total_messages_sent

    for _ in range(messages):
        data = {
            "device_id": f"sensor_{device_id}",
            "temperature": random.uniform(20, 80),
            "humidity": random.uniform(30, 90),
            "timestamp": datetime.now().isoformat()
        }

        client.publish(TOPIC, json.dumps(data))

        # Increment counter safely
        with counter_lock:
            total_messages_sent += 1

        print(f"{device_id} → published")

        time.sleep(random.uniform(0.5, 2))


def start_simulation(device_count=100, messages=50):
    global total_messages_sent

    threads = []

    # 🔹 Record start time
    start_wall_time = datetime.now()
    start_perf_time = time.perf_counter()

    print(f"\n🚀 Simulation started at: {start_wall_time.isoformat()}")
    print(f"Devices: {device_count} | Messages per device: {messages}\n")

    for i in range(device_count):
        t = threading.Thread(target=simulate_device, args=(i, messages))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    # 🔹 Record end time
    end_wall_time = datetime.now()
    end_perf_time = time.perf_counter()

    duration = end_perf_time - start_perf_time

    print("\n✅ Simulation finished.")
    print(f"Started at : {start_wall_time.isoformat()}")
    print(f"Finished at: {end_wall_time.isoformat()}")
    print(f"Total duration: {duration:.2f} seconds")
    print(f"Total messages sent: {total_messages_sent}")

    if duration > 0:
        throughput = total_messages_sent / duration
        print(f"Throughput: {throughput:.2f} messages/second")


if __name__ == "__main__":
    start_simulation()
