from fastapi import FastAPI, Depends, HTTPException, Request, Form
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.responses import RedirectResponse
from sqlalchemy import create_engine, Column, Integer, String, Boolean, DateTime, Text
from sqlalchemy.orm import sessionmaker, declarative_base
from jose import jwt, JWTError
from passlib.context import CryptContext
from datetime import datetime, timedelta
import secrets
from sqlalchemy.exc import OperationalError
from sqlalchemy import text
import time
import requests
# ===============================
# CONFIG
# ===============================

DATABASE_URL = "postgresql://iotuser:iotpass@postgres:5432/iotdb"

SECRET_KEY = "DS2026-Group20"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

app = FastAPI()
templates = Jinja2Templates(directory="middleware/templates")

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
security = HTTPBearer()

# ===============================
# DB MODELS (SCHEMA: middleware)
# ===============================

class User(Base):
    __tablename__ = "users"
    __table_args__ = {"schema": "middleware"}

    id = Column(Integer, primary_key=True)
    username = Column(String, unique=True)
    password = Column(String)


class Device(Base):
    __tablename__ = "devices"
    __table_args__ = {"schema": "middleware"}

    id = Column(Integer, primary_key=True)
    device_id = Column(String, unique=True)
    api_key = Column(String, unique=True)
    active = Column(Boolean, default=True)
    registered_at = Column(DateTime, default=datetime.utcnow)


class Log(Base):
    __tablename__ = "logs"
    __table_args__ = {"schema": "middleware"}

    id = Column(Integer, primary_key=True)
    source = Column(String)
    message = Column(Text)
    timestamp = Column(DateTime, default=datetime.utcnow)

# ===============================
# STARTUP
# ===============================

@app.on_event("startup")
def startup():
    max_retries = 10
    retry_delay = 3

    for attempt in range(max_retries):
        try:
            with engine.connect() as conn:
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS middleware"))
                conn.commit()
            print("Connected to PostgreSQL")
            break
        except OperationalError:
            print(f"Postgres not ready. Retry {attempt+1}/{max_retries}")
            time.sleep(retry_delay)
    else:
        raise Exception("Could not connect to PostgreSQL after retries.")

    
    # ---- Create tables ----
    Base.metadata.create_all(bind=engine)

    # ---- Create default admin user ----

    db = SessionLocal()
    existing = db.query(User).filter_by(username="admin").first()

    if not existing:
        hashed = pwd_context.hash("DS2026-Group20")
        admin = User(username="admin", password=hashed)
        db.add(admin)
        db.commit()
        print("Admin user created.")

    db.close()

# ===============================
# AUTH UTILS
# ===============================

def verify_password(plain, hashed):
    return pwd_context.verify(plain, hashed)

def create_token(data: dict):
    expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    data.update({"exp": expire})
    return jwt.encode(data, SECRET_KEY, algorithm=ALGORITHM)

def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    token = credentials.credentials
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")

# ===============================
# LOGIN
# ===============================

@app.post("/login")
def login(username: str = Form(...), password: str = Form(...)):
    db = SessionLocal()
    user = db.query(User).filter_by(username=username).first()
    db.close()

    if not user or not verify_password(password, user.password):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    token = create_token({"sub": user.username})
    return {"access_token": token}

# ===============================
# DEVICE PROVISIONING
# ===============================

@app.post("/devices/register")
def register_device(device_number: int, user=Depends(get_current_user)):
    if device_number < 0:
        raise HTTPException(status_code=400, detail="Device number must be positive")

    device_id = f"sensor_{device_number}"

    db = SessionLocal()

    existing = db.query(Device).filter_by(device_id=device_id).first()
    if existing:
        db.close()
        raise HTTPException(status_code=400, detail="Device already exists")

    api_key = secrets.token_hex(16)

    device = Device(
        device_id=device_id,
        api_key=api_key
    )

    db.add(device)
    db.commit()
    db.close()

    return {"device_id": device_id, "api_key": api_key}


@app.get("/devices")
def list_devices(user=Depends(get_current_user)):
    db = SessionLocal()
    devices = db.query(Device).all()
    db.close()

    return [
        {
            "device_id": d.device_id,
            "active": d.active,
            "registered_at": d.registered_at
        }
        for d in devices
    ]

@app.post("/toggle-device/{device_id}")
def toggle_device(device_id: str):
    db = SessionLocal()
    device = db.query(Device).filter_by(device_id=device_id).first()

    if not device:
        db.close()
        raise HTTPException(status_code=404, detail="Device not found")

    device.active = not device.active
    db.commit()
    db.close()

    return RedirectResponse(url="/", status_code=303)

# ===============================
# DEVICE VALIDATION (for gateways)
# ===============================

@app.get("/validate-device")
def validate_device(api_key: str):
    db = SessionLocal()
    device = db.query(Device).filter_by(api_key=api_key, active=True).first()
    db.close()

    if not device:
        raise HTTPException(status_code=403, detail="Invalid device")

    return {"status": "valid"}

# ===============================
# CENTRAL LOGGING
# ===============================

@app.post("/logs")
def create_log(source: str, message: str):
    db = SessionLocal()

    log = Log(source=source, message=message)
    db.add(log)
    db.commit()
    db.close()

    return {"status": "logged"}

# ===============================
# DASHBOARD
# ===============================

@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request):
    db = SessionLocal()

    devices = db.query(Device).all()
    logs = db.query(Log).order_by(Log.timestamp.desc()).limit(20).all()
    db.close()

    # fetch gateway cluster state from cloud
    try:
        r = requests.get("http://cloud:9000/gateway-states", timeout=2)
        gateways = r.json()
    except:
        gateways = []

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "devices": devices,
            "logs": logs,
            "gateways": gateways
        }
    )