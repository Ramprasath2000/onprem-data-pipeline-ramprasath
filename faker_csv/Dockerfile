FROM python:3.8-slim
WORKDIR /app
COPY faker_to_csv.py .
RUN pip install faker
CMD ["python", "faker_to_csv.py"]
