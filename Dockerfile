FROM python:3.11-slim

WORKDIR /moodleapp
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# non-root user (optional)
RUN useradd -m appuser
USER appuser

ENTRYPOINT ["python", "-u", "mysql_to_parquet_course.py"]
