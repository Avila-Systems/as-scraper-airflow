from json import loads
import logging
from math import ceil
from typing import Any, List, Optional
import pandas as pd
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud.bigquery import Client, LoadJobConfig
from as_scraper.errors import ScraperError
from as_scraper_airflow.errors import TaskError
from as_scraper_airflow.operators import ScraperOperator

log = logging.getLogger(__name__)


class ScraperToBigqueryOperator(ScraperOperator):
    '''
    Execute scraper and store results and errors in bigquery.

    Parameters:
    -----------
    destination_table : `str`
        The BigQuery destination table. It needs to be in the format
        <dataset_name>.<table_name>
    bigquery_conn_id : `str`
        The Airflow connection ID for BigQuery.
    schema : `str`
        The BigQuery Schema for the destination table.
    error_table : `Optional[str]`
        The BigQuery error table. It needs to be in the format
        <dataset_name>.<table_name>. Required if `store_errors` is True.
    error_schema : `Optional[str]`
        The BigQuery Schema for scraper errors. Required if `store_errors` is True.
    '''

    def __init__(
        self,
        destination_table: str,
        bigquery_conn_id: str,
        schema: str,
        error_table: Optional[str] = None,
        error_schema: Optional[str] = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.destination_table = destination_table
        self.bigquery_conn_id = bigquery_conn_id
        self.schema = schema
        self.error_table = error_table
        self.error_schema = error_schema
        self._bq_client = None

    @property
    def bq_client(self) -> Client:
        if self._bq_client is None:
            bq_hook = BigQueryHook(
                bigquery_conn_id=self.bigquery_conn_id, use_legacy_sql=False)
            self._bq_client = bq_hook.get_client()
        return self._bq_client

    def store_results(self, df: pd.DataFrame) -> None:
        log.info('Uploading %d results to BigQuery', len(df))
        job_config = LoadJobConfig(schema=loads(self.schema), write_disposition='WRITE_TRUNCATE',
                                   create_disposition='CREATE_IF_NEEDED', max_bad_records=ceil(len(df) * 0.05),)
        load_job = self.bq_client.load_table_from_json(df.to_dict(
            orient='records'), self.destination_table, job_config=job_config,)
        load_job.result()
        log.info('Load job ended at %s', load_job.ended)
        if load_job.error_result is not None:
            log.error(load_job.error_result)
            log.error(load_job.errors)

    def test_storage_connection(self) -> Any:
        return self.bq_client()

    def store_errors(self, errors: List[ScraperError], context: Any) -> None:
        log.info('Uploading %d errors to BigQuery', len(errors))
        start_date = context['dag_run'].start_date
        if self.local_tz:
            start_date = start_date.astimezone(tz=self.local_tz)
        start_date = start_date.isoformat()
        errors = [TaskError(context['run_id'], start_date, self.task_id,
                            f'{url}: {message}')._asdict() for url, message in errors]
        job_config = LoadJobConfig(schema=loads(
            self.error_schema), write_disposition='WRITE_APPEND', create_disposition='CREATE_IF_NEEDED',)
        load_job = self.bq_client.load_table_from_json(
            errors, destination=self.error_table, job_config=job_config)
        load_job.result()
