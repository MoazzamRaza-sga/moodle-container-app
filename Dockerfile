FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY test_mysql_via_ssh.py .

# non-root user (optional)
RUN useradd -m appuser
USER appuser

ENTRYPOINT ["python", "-u", "mysql_to_parquet_course.py"]
