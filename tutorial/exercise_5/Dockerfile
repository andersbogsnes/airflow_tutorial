FROM python:3.8.6-slim

ENV PATH="/opt/venv:$PATH"
COPY requirements.txt requirements.txt

RUN python -m venv /opt/venv && pip install -r requirements.txt

WORKDIR /app
COPY src /app

ENTRYPOINT ["python", "main.py"]
