from fastapi import FastAPI
from fastapi.responses import FileResponse
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, Float, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError
from sqlalchemy import create_engine, Column, Integer, String, Float, Index, DateTime
import time
from datetime import datetime
from collections import Counter
import joblib
import os


app = FastAPI()

MODEL_PATH = "/ml_models/spark_rf_model"
MODEL_VERSION_FILE = "/ml_models/version.txt"

DATABASE_URL = "postgresql://iotuser:iotpass@postgres:5432/iotdb"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)

Base = declarative_base()

# Retry DB connection until ready
@app.on_event("startup")
def init_db():
    while True:
        try:
            Base.metadata.create_all(bind=engine)
            print("Connected to PostgreSQL successfully")
            break
        except OperationalError:
            print("Waiting for PostgreSQL...")
            time.sleep(2)
# -----------------------------
# DB Model
# -----------------------------
class Anomaly(Base):
    __tablename__ = "anomalies"

    id = Column(Integer, primary_key=True, index=True)
    gateway_id = Column(String, index=True)
    device_id = Column(String, index=True)
    temperature = Column(Float)
    humidity = Column(Float)
    timestamp = Column(DateTime, index=True)
    classification = Column(String, index=True)

    # Add composite index (important for analytics)
    __table_args__ = (
        Index("idx_device_time", "device_id", "timestamp"),
    )

# -----------------------------
# Full Sensor Dataset (NEW)
# -----------------------------
class SensorDataRecord(Base):
    __tablename__ = "sensor_data"

    id = Column(Integer, primary_key=True, index=True)
    gateway_id = Column(String, index=True)
    device_id = Column(String, index=True)
    temperature = Column(Float)
    humidity = Column(Float)
    timestamp = Column(DateTime, index=True)
    classification = Column(String)

    __table_args__ = (
        Index("idx_sensor_device_time", "device_id", "timestamp"),
    )

# -----------------------------
# Gateway State Table (NEW)
# -----------------------------
class GatewayState(Base):
    __tablename__ = "gateway_state"

    gateway_id = Column(String, primary_key=True)
    version = Column(Integer)
    total_processed = Column(Integer)
    total_anomalies = Column(Integer)
    last_updated = Column(DateTime)

# -----------------------------
# Pydantic Model
# -----------------------------
class AnomalyRecord(BaseModel):
    gateway_id: str
    device_id: str
    temperature: float
    humidity: float
    timestamp: str
    classification: str

class GatewayStateRecord(BaseModel):
    gateway_id: str
    version: int
    total_processed: int
    total_anomalies: int


@app.get("/")
def root():
    return {"message": "Cloud service with PostgreSQL running"}

# -----------------------------
# Store Anomaly
# -----------------------------
@app.post("/store-anomaly")
def store_anomaly(data: AnomalyRecord):

    db = SessionLocal()

    anomaly = Anomaly(
        gateway_id=data.gateway_id,
        device_id=data.device_id,
        temperature=data.temperature,
        humidity=data.humidity,
        timestamp=datetime.fromisoformat(data.timestamp),
        classification=data.classification)
    db.add(anomaly)
    db.commit()
    db.close()

    return {"status": "Anomaly stored in PostgreSQL"}

# -----------------------------
# Store FULL Sensor Record
# -----------------------------
@app.post("/store-sensor-data")
def store_sensor_data(data: AnomalyRecord):

    db = SessionLocal()

    record = SensorDataRecord(
        gateway_id=data.gateway_id,
        device_id=data.device_id,
        temperature=data.temperature,
        humidity=data.humidity,
        timestamp=datetime.fromisoformat(data.timestamp),
        classification=data.classification
    )
    db.add(record)
    db.commit()
    db.close()

    return {"status": "Sensor record stored"}

# -----------------------------
#  Anomalous Data Endpoint
# -----------------------------
@app.get("/anomalies")
def get_anomalies():

    db = SessionLocal()
    results = db.query(Anomaly).all()
    total = len(results)

    per_gateway = {}

    for r in results:
        per_gateway.setdefault(r.gateway_id, 0)
        per_gateway[r.gateway_id] += 1

    db.close()

    return {
        "total_anomalies": total,
        "per_gateway": per_gateway,
        "data": [
            {
                "gateway_id": r.gateway_id,
                "device_id": r.device_id,
                "temperature": r.temperature,
                "humidity": r.humidity,
                "timestamp": r.timestamp,
                "classification": r.classification,
            }
            for r in results
        ],
    }

# -----------------------------
#  Reconciliation Endpoint
# -----------------------------
@app.post("/update-state")
def update_state(data: GatewayStateRecord):

    db = SessionLocal()

    existing = db.query(GatewayState).filter_by(
        gateway_id=data.gateway_id
    ).first()

    if existing:
        # Accept only newer version
        if data.version > existing.version:
            existing.version = data.version
            existing.total_processed = data.total_processed
            existing.total_anomalies = data.total_anomalies
            existing.last_updated = datetime.utcnow()
    else:
        new_state = GatewayState(
            gateway_id=data.gateway_id,
            version=data.version,
            total_processed=data.total_processed,
            total_anomalies=data.total_anomalies,
            last_updated=datetime.utcnow()
        )
        db.add(new_state)

    db.commit()
    db.close()

    return {"status": "reconciled"}


@app.get("/gateway-states")
def get_states():
    db = SessionLocal()
    results = db.query(GatewayState).all()
    db.close()

    return [
        {
            "gateway_id": r.gateway_id,
            "version": r.version,
            "total_processed": r.total_processed,
            "total_anomalies": r.total_anomalies,
            "last_updated": r.last_updated
        }
        for r in results
    ]

@app.get("/gateway-state/{gateway_id}")
def get_gateway_state(gateway_id: str):

    db = SessionLocal()
    state = db.query(GatewayState).filter_by(
        gateway_id=gateway_id
    ).first()
    db.close()

    if not state:
        return {"exists": False}

    return {
        "exists": True,
        "gateway_id": state.gateway_id,
        "version": state.version,
        "total_processed": state.total_processed,
        "total_anomalies": state.total_anomalies
    }

# -----------------------------
#  Model Version Endpoint
# -----------------------------
@app.get("/latest-edge-model")
def get_latest_model():

    if not os.path.exists(MODEL_PATH):
        return {"exists": False}

    version = 1
    if os.path.exists(MODEL_VERSION_FILE):
        with open(MODEL_VERSION_FILE) as f:
            version = int(f.read().strip())

    return {
        "exists": True,
        "version": version
    }

# -----------------------------
# Advanced Cloud Analytics Endpoint (APIs)
# -----------------------------
# These endpoints are being replaced by Grafana dashboards. 
# -----------------------------

# # Simple summary analytics 
# @app.get("/analytics/summary")
# def analytics_summary():
#     db = SessionLocal()
#     total = db.query(SensorDataRecord).count()
#     anomalies = db.query(SensorDataRecord)\
#         .filter_by(classification="ANOMALY")\
#         .count()
#     db.close()

#     return {
#         "total_records": total,
#         "total_anomalies": anomalies
#     }

# # Analytics per gateway (number of records processed by each gateway)
# @app.get("/analytics/per-gateway")
# def analytics_per_gateway():
#     db = SessionLocal()
#     results = db.query(
#         SensorDataRecord.gateway_id
#     ).all()

#     counts = {}
#     for r in results:
#         counts.setdefault(r.gateway_id, 0)
#         counts[r.gateway_id] += 1

#     db.close()
#     return counts

# @app.get("/analytics/hourly-trend")
# def hourly_trend():
#     db = SessionLocal()
#     results = db.query(SensorDataRecord).all()
#     db.close()

#     hours = [r.timestamp[:13] for r in results]
#     return dict(Counter(hours))
