"""
Dag for triggering modules in folder (project_mfm_monitoring)
Dag for scraping project websites for the most recent data belonging to each german state
"""

import os
from datetime import datetime
from airflow import DAG
from airflow.utils.helpers import chain
from airflow.operators.python_operator import PythonOperator
from prod.src.projects.internal.mattermost import alert_mattermost_channel
from prod.src.projects.internal.email import notify_args
from path.to.python.module.pythonmodulename import (
    trigger_project_scraper,
)

from path.to.python.module.pythonmodulename import (
    trigger_project_historiser,
)
from pendulum import timezone

local_tz = timezone("Europe/Berlin")

if os.getenv("ADMIN_EMAIL"):
    email_recipients = [os.getenv("ADMIN_EMAIL")]
else:
    email_recipients = ["***"]

mattermost_channel = "***"

default_postgres_params = {
    "***": None,
}

# Default arguments
default_args = {
    "owner": "***",
    "depends_on_past": False,
    "email": email_recipients,
    "retries": 0,
    "max_active_runs": 1,
    "on_failure_callback": None,
}

dag_params = {
    "dag_id": "***",
    "default_args": {**notify_args, **default_args},
    "concurrency": 1,
    "schedule_interval": "0 0 1 * *",  # Run once a month at midnight of the first day of the month
    "start_date": datetime(2024, 7, 5, tzinfo=local_tz),
    "catchup": False,
    "tags": ["scraping", "Historsierung", "Metadaten"],
}

with DAG(**dag_params) as dag:
    scrape_data_from_website = PythonOperator(
        task_id="***",
        provide_context=True,
        python_callable=None,
        params=default_postgres_params,
    )

    historise = PythonOperator(
        task_id="***",
        provide_context=True,
        python_callable=None,
        params=default_postgres_params,
    )

    chain(scrape_data_from_website, historise)
