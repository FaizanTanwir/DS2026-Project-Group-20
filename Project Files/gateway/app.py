from fastapi import FastAPI
from pydantic import BaseModel
from queue import Queue
import threading
import time
import joblib
import os
import numpy as np
import json
import paho.mqtt.client as mqtt
from datetime import datetime
import requests
import socket
import redis
import atexit
import warnings

# Spark imports (for cloud model)
from pyspark.sql import SparkSession, Row
from pyspark.ml import PipelineModel

# -----------------------------------------------------
# GLOBAL WARNINGS FILTER (removes sklearn user warning to keep logs clean)
# -----------------------------------------------------
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", message=".*sklearn.utils.parallel.delayed.*")

app = FastAPI()

# =====================================================
# BASIC CONFIG
# =====================================================

GATEWAY_ID = socket.gethostname()

CLOUD_ANOMALY_URL = "http://cloud:9000/store-anomaly"
CLOUD_SENSOR_DATA_URL = "http://cloud:9000/store-sensor-data"
CLOUD_STATE_URL = "http://cloud:9000/update-state"
MIDDLEWARE_LOG_URL = "http://middleware:7000/logs"

MQTT_BROKER = "mqtt-broker"
MQTT_PORT = 1883
MQTT_TOPIC = "iot/sensors"

# redis_client = redis.Redis(host="redis", port=6379, decode_responses=True) # helps to form centralized and stateless architecture.

# -----------------------------------------------------
# GLOBAL HTTP SESSION (Fix socket leak + speed boost)
# -----------------------------------------------------
http_session = requests.Session()

# =====================================================
# MODEL PATHS
# =====================================================

EDGE_MODEL_PATH = "/ml_models/anomaly_model.pkl"     # Initial training
CLOUD_MODEL_PATH = "/ml_models/spark_rf_model"          # Cloud retrained model (if available)

MODEL_VERSION = 0
model_type = "sklearn"   # default
model = None
spark = None

# -----------------------------------------------------
# LOCKS
# -----------------------------------------------------
model_lock = threading.Lock()
state_lock = threading.Lock()
file_lock = threading.Lock()

# -----------------------------------------------------
# SPARK BATCH SETTINGS
# -----------------------------------------------------
BATCH_SIZE = 20
BATCH_TIMEOUT = 0.5
spark_batch_buffer = []
batch_lock = threading.Lock()

# -----------------------------------------------------
# Graceful shutdown (to close Spark session if open)
# -----------------------------------------------------
@atexit.register
def shutdown():
    global spark
    if spark:
        print("Shutting down Spark session...")
        spark.stop()
# =====================================================
# MODEL LOADING LOGIC (LOCKED + DETERMINISTIC)
# =====================================================

def initialize_model():
    global model, model_type, spark

    with model_lock:

        # If Spark model exists → always use Spark
        if os.path.exists(CLOUD_MODEL_PATH):
            try:
                print("Loading Spark model...")

                if spark is None:
                    spark = SparkSession.builder \
                        .master("local[*]") \
                        .appName(f"GatewayInference-{GATEWAY_ID}") \
                        .getOrCreate()
                    
                    spark.sparkContext.setLogLevel("ERROR")

                model = PipelineModel.load(CLOUD_MODEL_PATH)
                model_type = "spark"
                print("✅ Spark model loaded successfully.")
                return
            except Exception as e:
                print("❌ Spark load failed:", e)

        # Fallback (only if Spark unavailable)
        print("Loading fallback sklearn model...")
        model = joblib.load(EDGE_MODEL_PATH)
        if hasattr(model, "n_jobs"):
            model.n_jobs = 1 # Ensure single-threaded for inference to avoid resource contention
        model_type = "sklearn"
        print("⚠️ Fallback sklearn model loaded.")

# =====================================================
# LOCAL STATE
# =====================================================

local_state = {
    "gateway_id": GATEWAY_ID,
    "version": 0,
    "total_processed": 0,
    "total_anomalies": 0
}

# -----------------------------
# Leader Election
# -----------------------------
leader_lock = threading.Lock()
is_leader = False

# -----------------------------
# Durable Outbound Queue (To send lost gateway data to cloud when it recovers)
# -----------------------------
PENDING_FILE = "pending_queue.jsonl"

# Task queue
task_queue = Queue()
# Number of worker threads
WORKER_COUNT = 4

# -----------------------------------------------------
# SENSOR MODEL
# -----------------------------------------------------
class SensorData(BaseModel):
    device_id: str
    temperature: float
    humidity: float
    timestamp: str # ISO format string

# =====================================================
# BATCH SPARK PROCESSOR THREAD
# =====================================================
def spark_batch_processor(worker_id):
    global spark_batch_buffer

    while True:
        time.sleep(BATCH_TIMEOUT)

        with batch_lock:
            if len(spark_batch_buffer) == 0:
                continue

            batch = spark_batch_buffer[:]
            spark_batch_buffer = []

        with model_lock:
            current_model = model
            current_spark = spark

        try:
            df = current_spark.createDataFrame([
                Row(
                    temperature=float(d.temperature),
                    humidity=float(d.humidity)
                ) for d in batch
            ])

            predictions = current_model.transform(df) \
                .select("prediction") \
                .collect()

            for data, pred in zip(batch, predictions):
                classification = "ANOMALY" if pred[0] == 1.0 else "NORMAL"
                process_result(worker_id, data, classification, "spark")

        except Exception as e:
            print("Spark batch failed:", e)

# -----------------------------------------------------
# COMMON RESULT PIPELINE
# -----------------------------------------------------
def process_result(worker_id, data, classification, used_model):

    processed_record = {
        "gateway_id": GATEWAY_ID,
        "device_id": data.device_id,
        "temperature": data.temperature,
        "humidity": data.humidity,
        "timestamp": data.timestamp,
        "classification": classification
    }

    # Send sensor data
    try:
        http_session.post(
            CLOUD_SENSOR_DATA_URL, 
            json=processed_record, 
            timeout=1)
    except:
        pass

    # If anomaly → durable queue
    if classification == "ANOMALY":
        record_json = json.dumps(processed_record)

        # Write to durable queue FIRST (before sending to cloud) to ensure no loss
        with file_lock:
            with open(PENDING_FILE, "a") as f:
                f.write(record_json + "\n")

        try:
            http_session.post(CLOUD_ANOMALY_URL, json=processed_record, timeout=2)
            # If successful → remove from queue
            remove_record_from_file(record_json)
        except:
            print("Cloud unreachable, anomaly queued.")
            pass

    # Update local versioned state
    with state_lock:
        local_state["total_processed"] += 1
        if classification == "ANOMALY":
            local_state["total_anomalies"] += 1
        local_state["version"] += 1

    print(f"[{GATEWAY_ID}] Worker {worker_id} → {classification} ({used_model})")

# =====================================================
# WORKER FUNCTION (SMART INFERENCE)
# =====================================================

def worker(worker_id):
    global model, model_type

    while True:
        data = task_queue.get()
        # Simulate ML processing delay
        time.sleep(0.1)

        with model_lock:
            current_model_type = model_type
            current_model = model

        # -------------------------
        # Spark Inference → batch
        # -------------------------
        if current_model_type == "spark":
            with batch_lock:
                spark_batch_buffer.append(data)

                if len(spark_batch_buffer) >= BATCH_SIZE:
                    batch = spark_batch_buffer[:]
                    spark_batch_buffer.clear()

                    # Immediate processing
                    for item in batch:
                        spark_batch_buffer.append(item)

        # -------------------------
        # Sklearn Inference → immediate (Fallback)
        # -------------------------
        else:
            features = np.array([[data.temperature, data.humidity]])
            prediction = current_model.predict(features)
            classification = "ANOMALY" if prediction[0] == -1 else "NORMAL"
            process_result(worker_id, data, classification, "sklearn")

        # Middleware logging
        try:
            requests.post(
                MIDDLEWARE_LOG_URL,
                params={
                    "source": GATEWAY_ID,
                    "message": f"{data.device_id} → {classification}"
                },
                timeout=2
            )
        except:
            pass

        task_queue.task_done()


# =====================================================
# DURABLE QUEUE HELPERS (To send lost gateway data to cloud when it recovers)
# =====================================================

def remove_record_from_file(record_json):
    with file_lock:
        if not os.path.exists(PENDING_FILE):
            return

        with open(PENDING_FILE, "r") as f:
            lines = f.readlines()

        with open(PENDING_FILE, "w") as f:
            for line in lines:
                if line.strip() != record_json:
                    f.write(line)


def replay_pending_records():
    if not os.path.exists(PENDING_FILE):
        return

    with file_lock:
        with open(PENDING_FILE, "r") as f:
            lines = f.readlines()

    for line in lines:
        try:
            record = json.loads(line.strip())
            requests.post(CLOUD_ANOMALY_URL, json=record, timeout=2)
            remove_record_from_file(line.strip())
        except Exception:
            break


# =====================================================
# MODEL UPDATE THREAD (SMART SWITCH)
# =====================================================

def check_for_model_update():
    global model, MODEL_VERSION

    while True:
        time.sleep(10)
        try:
            r = requests.get("http://cloud:9000/latest-edge-model", timeout=2)
            data = r.json()

            if data.get("exists") and data["version"] > MODEL_VERSION:
                print("🔄 New Spark model detected.")
                initialize_model()
                MODEL_VERSION = data["version"]
                print("Model upgraded to Spark version.")
        except:
            pass


# =====================================================
# MQTT Callbacks
# =====================================================

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        data = SensorData(**payload)
        task_queue.put(data)
    except Exception as e:
        print("MQTT message error:", e)


def start_mqtt():
    client = mqtt.Client(client_id=f"gateway_{GATEWAY_ID}")
    client.on_message = on_message
    client.on_connect = lambda client, userdata, flags, rc: print(f"[{GATEWAY_ID}] Connected to MQTT with rc={rc}")

    while True:
        try:
            client.connect(MQTT_BROKER, MQTT_PORT, 60)
            break
        except Exception as e:
            print("Waiting for MQTT broker...", e)
            time.sleep(3)

    #client.subscribe(MQTT_TOPIC) #Broadcast mode
    client.subscribe(f"$share/gateway-group/{MQTT_TOPIC}") # Accidental single subscriber
    client.loop_start()

    print(f"MQTT subscriber started for {GATEWAY_ID}")

# =====================================================
# Fault Tolerance & Coordination Threads (Leader Election, State Sync)
# =====================================================
#------------------------------
# Leader Election Loop (Gateways coordinate with leader to push states)
#------------------------------
def leader_election_loop():
    global is_leader

    while True:
        try:
            response = requests.get(
                "http://cloud:9000/gateway-states",
                timeout=2
            )

            gateways = response.json()

            if not gateways:
                time.sleep(3)
                continue

            ids = sorted([g["gateway_id"] for g in gateways])
            elected = ids[0]

            with leader_lock:
                is_leader = (GATEWAY_ID == elected)

            if is_leader:
                print(f"[{GATEWAY_ID}] I am LEADER")

            else:
                print(f"[{GATEWAY_ID}] Leader is {elected}")

        except Exception:
            pass

        time.sleep(5)

# -----------------------------
# State Sync Thread
# -----------------------------
def sync_state_to_cloud():
    global is_leader

    while True:
        time.sleep(3)

        # with leader_lock: # this part handled the state persistence by leader but removed due to conflicts. Now, leader only coordinates the state sync but all gateways can push their states to cloud to avoid single point of failure and conflicts.
        #     leader_status = is_leader

        # if not leader_status:
        #     continue   # Only leader syncs state to cloud to avoid conflicts

        try:
            with state_lock:
                payload = local_state.copy()

            requests.post(CLOUD_STATE_URL, json=payload, timeout=2)

        except Exception:
            print(f"[{GATEWAY_ID}] Cloud unreachable — will retry")

# -----------------------------
# Recover State From Cloud (Retrieve sensor data and versioned state from cloud when gateway restarts)
# -----------------------------
def recover_state_from_cloud():

    for _ in range(10):
        try:
            response = requests.get(
                f"http://cloud:9000/gateway-state/{GATEWAY_ID}",
                timeout=2
            )

            data = response.json()

            if data.get("exists"):
                with state_lock:
                    local_state["version"] = data["version"]
                    local_state["total_processed"] = data["total_processed"]
                    local_state["total_anomalies"] = data["total_anomalies"]

                print(f"[{GATEWAY_ID}] State recovered.")
            return

        except Exception:
            time.sleep(2)

    print(f"[{GATEWAY_ID}] Could not recover state.")



# =====================================================
# STARTUP
# =====================================================

@app.on_event("startup")
def start_workers():

    recover_state_from_cloud()
    initialize_model()
    replay_pending_records()

    for i in range(WORKER_COUNT):
        t =threading.Thread(target=worker, args=(i,), daemon=True)
        t.start()
        print(f"Worker {i} started")

    threading.Thread(target=sync_state_to_cloud, daemon=True).start()
    print("State sync thread started")

    threading.Thread(target=leader_election_loop, daemon=True).start()
    print("Leader election thread started")

    threading.Thread(target=spark_batch_processor, args=(i,), daemon=True).start()
    print("Spark batch processor thread started")

    threading.Thread(target=check_for_model_update, daemon=True).start()
    print("Model update checker thread started")

    start_mqtt()


# =====================================================
# API ENDPOINTS
# =====================================================

@app.get("/")
def root():
    return {"message": f"IoT cluster with 3 Gateways running with versioned State enabled",
            "gateway": GATEWAY_ID,
            "model_active": model_type,
            "processed": local_state["total_processed"]}


@app.get("/data")
def get_data():
    with state_lock:
        return local_state.copy()
    
# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)

# The instruction below is valid for HTTPs style code. Not for updated MQQT code.
# Uncomment the above lines to run the server directly with this script using http://127.0.0.1:8000 link.
# Otherwise, keep it commented and write the command in terminal: uvicorn gateway.app:app --reload.