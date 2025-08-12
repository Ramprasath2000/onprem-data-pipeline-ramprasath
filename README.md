Here are the complete steps to run your entire Dockerized Airflow, Kafka, MySQL, and Spark project from the very beginning:
1. Make sure Docker Desktop is installed and running.
2. Open a terminal in your project root directory
(This is where your docker-compose.yml file is located.)
3. Build all your Docker images:
bash
docker-compose build
4. Start all services:
bash
docker-compose up -d
5. Check that all containers are running:
bash
docker ps
Make sure you see containers like airflow-webserver, airflow-scheduler, mysql, etc.

Note: Make sure airflow db exist in database machine
If not
mysql -h 127.0.0.1 -P 3307 -u root -p

CREATE DATABASE airflow;

6. Initialize Airflow metadata database (run this once, or after purging volumes):
bash
docker-compose run --rm airflow-init
You should see Initialization done if it worked.
7. Create an Airflow admin user for the web UI:
First, get the name of your webserver container (e.g. final_project-airflow-webserver-1):
bash
docker ps
Then enter the webserver container:
bash
docker exec -it final_project-airflow-webserver-1 /bin/bash
Inside the container, run:
bash
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password yourpassword
(Change username/email/password as desired.)
8. Access the Airflow UI:
Open your browser and go to:
text
http://localhost:8082
Login with the username and password you just created.
9. Deploy your DAGs and begin orchestration.
•	Place DAG .py files in your dags/ folder.
•	Watch Airflow UI to see them appear.
10. Stop all services when done:
bash
docker-compose down
If you have any issues at any step (containers not running, UI not opening, etc.), let me know your error and I'll help troubleshoot!
Related
What do I need to check if my project doesn't start correctly after these steps
How can I customize the setup process for different project configurations
Why is my Airflow UI not accessible after following these steps
What are common issues with Docker volumes and network settings in this context
How do I automate these steps for repeated project setups
Thread is getting long. Start a new one for better answers.



