FROM docker.io/library/python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir confluent-kafka
COPY producer.py .
CMD ["python", "-u", "producer.py"]
