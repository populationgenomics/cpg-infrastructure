FROM python:3.11-slim

# same uv version as the repo's workflows
COPY --from=ghcr.io/astral-sh/uv:0.12.9 /uv /bin/

ENV PYTHONUNBUFFERED=1
WORKDIR /app

COPY requirements.txt .
RUN uv pip install --system --no-cache -r requirements.txt

# keep in sync with hash_folder() in ../driver.py, which tags the image
COPY *.py *.json *.txt ./

# each Cloud Run job passes its aggregator script, e.g. `seqr.py`
ENTRYPOINT ["python"]
