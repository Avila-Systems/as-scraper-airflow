from datetime import datetime, timedelta
from airflow.models import DAG
from scrapers.yellowpages import YellowPagesScraper
from as_scraper_airflow.operators import ScraperToLogsOperator


with DAG(
    dag_id="yellow_pages",
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description="A simple Scraper DAG",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 8, 4),
    catchup=False,
) as dag:
    t1 = ScraperToLogsOperator(
        scraper_cls=YellowPagesScraper,
        urls=['https://www.yellowpages.com/sitemap'],
        task_id='scrape',
        save_errors=True,
    )
