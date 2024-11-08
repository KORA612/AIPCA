# 3. Celery Setup and Configuration

import os

from config.celery import Celery
from celery.schedules import crontab

# Set the Django settings module for Celery
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

app = Celery("ai_content_aggregator")
app.config_from_object("django.conf:settings", namespace="CELERY")

# Load task modules from all registered Django apps
app.autodiscover_tasks()

# Celery Beat Configuration
app.conf.beat_schedule = {
    "scrape_scheduled_sources": {
        "task": "scrape_scheduled_sources",
        "schedule": crontab(minute="*/15"),  # Run every 15 minutes
    },
    "process_pending_content": {
        "task": "process_pending_content",
        "schedule": crontab(minute="*/10"),  # Run every 10 minutes
    },
}

# Celery Task Hierarchy
CELERY_TASKS = {
    "high_priority": {
        "queue": "high_priority",
        "tasks": ["process_user_input", "generate_response"],
    },
    "scraping": {"queue": "scraping", "tasks": ["scrape_url", "extract_content"]},
    "processing": {
        "queue": "processing",
        "tasks": ["process_content", "adjust_sentiment"],
    },
}

# Celery Configuration
CELERY_CONFIG = {
    "task_serializer": "json",
    "result_serializer": "json",
    "accept_content": ["json"],
    "enable_utc": True,
    "worker_prefetch_multiplier": 1,
    "task_acks_late": True,
}


# Task Decorator
@app.task(bind=True, default_retry_delay=60, max_retries=3)
def process_user_input(self, query_id: str):
    # Implementation for processing user input
    pass


# Example Task
@app.task(bind=True, default_retry_delay=120, max_retries=2)
def scrape_url(self, url: str, domain: str):
    # Implementation for web scraping
    pass
