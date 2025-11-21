# syntax=docker/dockerfile:1
FROM python:3.11-slim

WORKDIR /app

# system deps for building some Python wheels
RUN apt-get update && apt-get install -y build-essential gcc

# copy requirements first to take advantage of caching
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# copy source
COPY ./src /app/src

# make sure model is included (if present in repo)
# if you prefer mounting at runtime, you can remove the COPY line below
COPY ./price_model.joblib /app/src/neuraestate/ml/price_model.joblib

ENV PYTHONUNBUFFERED=1
ENV DATABASE_URL=postgresql://postgres:postgres@db:5432/neuraestate

EXPOSE 8000

CMD ["uvicorn", "neuraestate.api.main:app", "--host", "0.0.0.0", "--port", "8000", "--app-dir", "src"]
