import logging
from typing import Any, List, Optional, Type, Union
from airflow.models.baseoperator import BaseOperator
from airflow.exceptions import AirflowException
import pandas as pd
from as_scraper.base.crawlers.crawler import Crawler
from as_scraper.base.errors import ScraperError
from as_scraper.base.scraper import Scraper

log = logging.getLogger(__name__)


class ScraperOperator(BaseOperator):
    '''
    Execute a Scraper or series of Scrapers.

    Parameters:
    -----------
    scraper_cls : `Union[Type[Scraper], List[Type[Scraper]]]`
        A scraper or list of scrapers to run.
    urls : `Optional[List[str]]`
        List of urls to scrap.
    crawler_cls : `Optional[Type[Crawler]]`
        The crawler to run. It is an alternative to the `urls` parameter. A crawler generates
        the urls that will be scraped in the execution.
    save_errors : `Optional[bool]`
        Wether to store scraping errors or not. Defaults to `False`
    drop_duplicates : `Optional[List[str]]`
        A list of columns of the resulting pandas dataframe of the scraping. If given, results
        will drop duplicate values based on the columns specified in this parameter.
    local_tz : `Optional[Any]`
        Pendulum timezone object. Used to assign timezone to output datetime.
    fail_if_empty_results : `Optional[bool]`
        If true, throw an error if the execution returns no results. Defaults to True.
    '''

    ui_color: str = '#eb9319'
    ui_fgcolor: str = '#5c4b1f'

    def __init__(
        self,
        scraper_cls: Union[Type[Scraper], List[Type[Scraper]]],
        urls: Optional[List[str]] = None,
        crawler_cls: Optional[Type[Crawler]] = None,
        save_errors: Optional[bool] = False,
        drop_duplicates: Optional[List[str]] = None,
        local_tz: Optional[Any] = None,
        fail_if_empty_results: Optional[bool] = True,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.scraper_cls = scraper_cls
        self.urls = urls
        self.crawler_cls = crawler_cls
        self.save_errors = save_errors
        self.drop_duplicates = drop_duplicates
        self.local_tz = local_tz
        self.fail_if_empty_results = fail_if_empty_results

    def execute(self, context: Any):
        self.test_storage_connection()
        scraper_input = pd.DataFrame(columns=['url'])
        if self.crawler_cls is not None:
            crawler = self.crawler_cls()
            crawled_urls = [{'url': url} for url in crawler.crawl()]
            scraper_input = pd.concat(
                scraper_input, pd.DataFrame(crawled_urls))
        if self.urls is not None:
            input_urls = [{'url': url} for url in self.urls]
            scraper_input = pd.concat(scraper_input, pd.DataFrame(input_urls))
        if not isinstance(self.scraper_cls, list):
            self.scraper_cls = [self.scraper_cls]
        # Algorithm to run multiple scraper classes one after each other.
        # The last scraper will have the target data for this operator.
        errors = []
        scraper_output = None
        for scraper_cls in self.scraper_cls:
            scraper = scraper_cls()
            scraper_output, _errors = scraper.execute(scraper_input)
            errors.extend(_errors)
            scraper_input = scraper_output
        if self.drop_duplicates is not None:
            filtered_scraper_output = scraper_output.drop_duplicates(
                subset=self.drop_duplicates)
            dropped = len(scraper_output) - len(filtered_scraper_output)
            log.warning('Dropped %d rows due to duplicate detection', dropped)
            scraper_output = filtered_scraper_output
        start_date = context['dag_run'].start_date
        if self.local_tz:
            start_date = start_date.astimezone(tz=self.local_tz)
        start_date = start_date.isoformat()
        execution_df = scraper_output.assign(scraped_date=start_date)
        if self.save_errors and len(errors):
            self.store_errors(errors, context)
        if len(execution_df):
            self.store_results(execution_df)
        elif self.fail_if_empty_results:
            raise AirflowException('No results from scraper run')

    def store_results(self, df: pd.DataFrame) -> None:
        '''
        Store results for the scraper run.

        Parameters:
        -----------
        df : `pd.DataFrame`
            The pandas dataframe containing the execution results
        '''
        raise NotImplementedError('Implement store_results')

    def store_errors(self, errors: List[ScraperError], context: Any) -> None:
        '''
        Store errors from scraper run.

        Parameters:
        -----------
        errors : `List[ScraperError]`
            List of errors to store.
        '''
        raise NotImplementedError('Implement store_errors')

    def test_storage_connection(self) -> Any:
        '''
        Get connection for storage service. Used before the scraper run to test
        connection before consuming resources.
        '''
        raise NotImplementedError('Implement test_storage_connection')
