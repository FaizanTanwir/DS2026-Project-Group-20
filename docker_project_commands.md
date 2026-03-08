------------------------------------------------------------------------------------------------------------------------------------------------

##### **Docker and Project Command Cheat Sheet**

------------------------------------------------------------------------------------------------------------------------------------------------



###### 1\. Running Services Locally (Without Docker)



uvicorn gateway.app:app –reload \[Starts the gateway FastAPI server locally with auto-reload when code changes.]



uvicorn cloud.cloud\_app:app –port 9000 –reload \[Starts the cloud FastAPI server locally on port 9000 with auto-reload.]



------------------------------------------------------------------------------------------------------------------------------------------------



###### 2\. Starting the Full System with Docker



docker compose up \[Starts all services defined in docker-compose.yml.]



docker compose up –scale gateway=X \[Starts the system and runs multiple gateway containers. Replace X with the number of gateways you want.]



docker compose up –build \[Rebuilds images if needed and starts all containers.]



docker compose up –build –scale gateway=X \[Rebuilds images and runs the system with multiple gateway containers. Replace X with the number of gateways you want.]



------------------------------------------------------------------------------------------------------------------------------------------------



###### 3\. Viewing Logs



docker compose logs -f gateway \[Shows live logs from all gateway containers.]



------------------------------------------------------------------------------------------------------------------------------------------------



###### 4\. Running and Validating Important Project Tasks



docker compose exec cloud python cloud/spark\_jobs/batch\_training.py \[‼️Runs the Spark batch training job inside the cloud container to retrain the ML model.]



docker compose exec cloud java -version \[Checks if Java is installed inside the cloud container (required for Spark).]



docker compose exec spark-worker find / -name “spark\_rf\_model” \[Searches the Spark worker container to find the trained Spark model directory.]



------------------------------------------------------------------------------------------------------------------------------------------------



###### 5\. Stopping or Restarting Containers



docker compose stop <container name> \[Stops a specific running container.]



docker compose restart <container name> \[Restarts a specific container.]



docker compose down \[Stops and removes all containers in the project.]



docker compose down -v \[⚠️ Stops containers and deletes associated volumes (removes stored data like databases).]



------------------------------------------------------------------------------------------------------------------------------------------------



###### 6\. Rebuilding Docker Images



docker compose build –no-cache \[Rebuilds images from scratch without using cached layers.]



docker compose up –build \[Builds updated images and runs containers.]



------------------------------------------------------------------------------------------------------------------------------------------------



###### 7\. Cleaning Docker (Free Disk Space)



docker system prune -a –volumes -f \[Deletes unused containers, networks, images, and volumes to free disk space.]



------------------------------------------------------------------------------------------------------------------------------------------------



###### 8\. Accessing the PostgreSQL Database



docker exec -it project-postgres-1 psql -U iotuser -d iotdb \[Opens the PostgreSQL command line inside the database container.]



SELECT COUNT(\*) FROM anomalies; \[SQL query to count how many anomaly records exist in the anomalies table.]



