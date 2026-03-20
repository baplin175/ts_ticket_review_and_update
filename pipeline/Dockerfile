FROM python:3.13-slim

WORKDIR /app

# Install Python dependencies (minimal)
RUN pip install --no-cache-dir flask gunicorn requests azure-storage-blob

# Copy only the files needed by the pipeline
COPY config.py ts_client.py matcha_client.py ./
COPY pass1_parser.py pass2_parser.py pass3_parser.py ./
COPY pass4/ ./pass4/
COPY prompts/ ./prompts/
COPY pipeline/ ./pipeline/

EXPOSE 80

CMD ["gunicorn", "--bind", "0.0.0.0:80", "--workers", "1", "--threads", "4", "--timeout", "600", "pipeline.app:app"]
