FROM python:3.11-slim

LABEL maintainer="oilgas-monitoring-team"
LABEL description="Oil & Gas sensor data generator for NiFi platform"

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ src/

ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "src.main"]
