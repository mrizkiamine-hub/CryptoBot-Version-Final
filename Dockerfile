FROM python:3.8-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends gcc libpq-dev \
  && rm -rf /var/lib/apt/lists/*

COPY requirements_step4.txt /app/requirements_step4.txt
RUN pip install --no-cache-dir -r /app/requirements_step4.txt

COPY app /app/app
COPY models /app/models

EXPOSE 8000
CMD ["bash","-lc","uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}"]