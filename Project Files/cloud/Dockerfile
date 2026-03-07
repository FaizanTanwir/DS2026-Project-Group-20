FROM python:3.11-slim-bookworm

WORKDIR /app

# Install Java for PySpark
RUN apt-get update && \
    apt-get install -y default-jdk && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# Install base cloud requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Spark training requirements
COPY cloud/spark_jobs/requirements.txt spark_requirements.txt
RUN pip install --no-cache-dir -r spark_requirements.txt

COPY . .

CMD ["uvicorn", "cloud.cloud_app:app", "--host", "0.0.0.0", "--port", "9000"]