import logging
from typing import Any, List
import pandas as pd
from as_scraper.base.errors import ScraperError
from as_scraper.operators.scraper import ScraperOperator

log = logging.getLogger(__name__)


class ScraperToLogsOperator(ScraperOperator):
    '''
    Execute scraper and log results in airflow logs.
    '''

    def __init__(self, *args, **kwargs):
        super().__init__(args, kwargs)

    def store_results(self, df: pd.DataFrame) -> None:
        log.info(df)

    def store_errors(self, errors: List[ScraperError], context: Any) -> None:
        log.info(errors, context)

    def test_storage_connection(self) -> Any:
        pass
