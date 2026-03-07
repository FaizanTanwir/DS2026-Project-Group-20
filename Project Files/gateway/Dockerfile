FROM python:3.11-slim

WORKDIR /app

# Install Java for local Spark for gateways
RUN apt-get update && \ 
    apt-get install -y default-jdk && \ 
    rm -rf /var/lib/apt/lists/* 

# Install base requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Spark training requirements
COPY gateway/requirements.txt gateway_spark_requirements.txt
RUN pip install --no-cache-dir -r gateway_spark_requirements.txt

COPY . .

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]