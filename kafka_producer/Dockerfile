FROM python:3.8-slim
WORKDIR /app
COPY weather_to_kafka.py .
RUN pip install kafka-python requests
CMD ["python", "weather_to_kafka.py"]
