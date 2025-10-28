FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    cron \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY config.json .
COPY src/ ./src/

# Create logs directory
RUN mkdir -p logs

# Make main.py executable
RUN chmod +x src/main.py

# Copy crontab file
COPY crontab /etc/cron.d/realty-bot-cron

# Give execution rights on the cron job
RUN chmod 0644 /etc/cron.d/realty-bot-cron

# Apply cron job
RUN crontab /etc/cron.d/realty-bot-cron

# Create the log file to be able to run tail
RUN touch /var/log/cron.log

# Run cron in foreground
CMD cron && tail -f /var/log/cron.log
