FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# keep in sync with hash_folder() in ../driver.py, which tags the image
COPY *.py *.json *.txt ./

# each Cloud Run job passes its aggregator script, e.g. `seqr.py`
ENTRYPOINT ["python"]
