# DS2026 Project

## Group 20
### Members:
1. Arshman Tariq (2509523)
2. Juuso Antero Alatalo (Y6909125)
3. Muhammad Faizan Tanveer (2509524)
4. Seyedehsahar Fatemi Abhari (2508265)

### Project Abstract:
The rapid growth of Internet-of-Things (IoT) devices requires scalable and intelligent infrastructures capable of processing large volumes of sensor data with minimal latency. This project presents a distributed 5G-enabled IoT gateway architecture that integrates edge computing, cloud analytics, and machine learning for real-time anomaly detection. The system simulates thousands of IoT devices generating environmental sensor data, i.e., Temperature and Pressure, and processes this data using a microservices-based distributed architecture. 
Sensor data is transmitted from simulated devices to edge gateways using the MQTT protocol. Each gateway performs real-time anomaly detection using a lightweight machine learning model, i.e. Isolation Forest, deployed at the edge to provide immediate inference locally, while a more computationally intensive model, like Random Forest, is periodically trained in the cloud using Apache Spark on historical sensor data stored in PostgreSQL. This enables an adaptive edge–cloud continuum.
The system is fully containerised using Docker and orchestrated through Docker Compose to simulate a distributed data center environment. Multiple gateway instances can be scaled to process incoming data streams while ensuring fault tolerance through local state queues and distributed task offloading. Redis was initially used for temporary state sharing before transitioning to a more distributed architecture with gateway coordination and consensus mechanisms.
A middleware layer provides administrative control, including authentication, device provisioning, logging, and monitoring dashboards. Cloud analytics are implemented using Grafana connected to PostgreSQL, enabling real-time visualisation of sensor trends, anomalies, and gateway performance. System evaluation demonstrates low-latency processing, strong message delivery reliability under partial failures, and robust throughput across multiple gateway configurations. The results highlight the effectiveness of distributed edge computing combined with cloud analytics for large-scale IoT environments.
