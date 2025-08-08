FROM python:3.8-slim
WORKDIR /app
COPY mock_mysql_data.py .
RUN pip install pymysql faker
CMD ["python", "mock_mysql_data.py"]
