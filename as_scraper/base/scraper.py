from abc import ABCMeta, abstractmethod
import logging
from typing import List, Optional, Tuple
from selenium.webdriver import Firefox, FirefoxOptions
from selenium.webdriver.firefox.webdriver import Service
from requests.adapters import HTTPAdapter
from requests import Session
import pandas as pd
from tqdm import tqdm
from as_scraper.base.errors import ScraperError
from as_scraper.base.headers import HEADERS
from as_scraper.base.exceptions import ThresholdException

log = logging.getLogger(__name__)


class Scraper(metaclass=ABCMeta):
    '''
    Base class for scrapers.

    Parameters:
    -----------
    urls : `Optional[List[str]]`
      List of urls to scrape. If not given at instantiation, it should be given at execution time via
      a pandas dataframe. The pandas dataframe should have an `url` column or an error will be
      thrown.
    test_mode : `Optional[bool]`
      If true and `LOAD_JAVASCRIPT=True`, then the execution will make a browser simulation without
      the `--headless` mode. This is intended only for testing outside the Airflow environment.
    '''
    COLUMNS = ['url']
    LOAD_JAVASCRIPT = False
    LOG_PATH = None
    ERROR_THRESHOLD = 0.05
    RESET_AFTER = None
    LOAD_TIMEOUT = None
    TEST_MODE = False

    def __init__(self, urls: Optional[List[str]] = None, test_mode: Optional[bool] = None):
        self.headers = HEADERS
        self.urls = urls
        self._driver = None
        self._session = None
        if test_mode is not None:
            self.TEST_MODE = test_mode
            self.ERROR_THRESHOLD = 0

    @property
    def driver(self):
        'Create a Firefox driver for selenium'
        if self._driver is None:
            firefox_options = FirefoxOptions()
            firefox_options.set_preference('browser.cache.disk.enable', False)
            firefox_options.set_preference(
                'browser.cache.memory.enable', False)
            firefox_options.set_preference(
                'browser.cache.offline.enable', False)
            firefox_options.set_preference('network.http.use-cache', False)
            firefox_options.set_preference('permissions.default.image', 2)
            firefox_options.set_preference(
                'dom.ipc.plugins.enabled.libflashplayer.so', 'false')
            firefox_options.set_preference(
                'general.useragent.override', HEADERS['User-Agent'])
            if not self.TEST_MODE:
                firefox_options.add_argument('--headless')
            else:
                log.warning('Creating driver without headless mode')
                if self.LOG_PATH is None:
                    self.LOG_PATH = 'geckodriver.log'
            service = Service(log_path=self.LOG_PATH)
            self._driver = Firefox(options=firefox_options, service=service)
            if self.LOAD_TIMEOUT is not None:
                self._driver.set_page_load_timeout(self.LOAD_TIMEOUT)
        return self._driver

    @property
    def session(self):
        'configure session for requests library'
        if self._session is None:
            adapter = HTTPAdapter(max_retries=3)
            self._session = Session()
            self._session.mount('https://', adapter)
        return self._session

    def _load_html_selenium(self, url: str):
        '''
        Load the html for an url using Selenium library.

        Parameters:
        -----------
        url : `str`
          Url to make a get request.
        '''
        self.driver.execute_script("window.open('');")
        while len(self.driver.window_handles) > 1:
            self.driver.switch_to.window(self.driver.window_handles[0])
            self.driver.close()
        else:
            self.driver.switch_to.window(self.driver.window_handles[0])
        try:
            self.driver.get(url)
        except Exception as e:
            log.error(
                '%s. handing loaded content to scrape handler. Url: %s', type(e), url)

    def _load_html_requests(self, url: str) -> bytes:
        '''
        Load the html for an url using requests library.

        Parameters:
        -----------
        url : `str`
          Url to make a get request.

        Returns:
        --------
        content : `bytes`
          Response content.
        '''
        ok_status_code = 200
        response = self.session.get(url, headers=self.headers)
        if response.status_code == ok_status_code:
            return response.content
        log.error('Failed http get request for url %s', url)

    @abstractmethod
    def scrape_handler(
        self,
        url: str,
        html: Optional[str] = None,
        driver: Optional[Firefox] = None,
        **kwargs,
    ) -> pd.DataFrame:
        '''Scrap a given html and return the scraped data.

        Parameters:
        -----------
        url : `str`
          Url which was requested for scraping.
        html : `Optional[str] = None`
          Html response from the url. It is only set when the scraping is done with requests.
        driver : `Optional[selenium.webdriver.Firefox]`
          Firefox driver used for selenium.
        **kwargs
          Extra parameters that the scraper can use for flow control or data enrichment.

        Returns:
        --------
        df : `pd.DataFrame`
          A pandas dataframe with a single row containing the scraped data from the url. The dataframe
          should have every column in the class COLUMNS variable.
        '''
        raise NotImplementedError('Implement scrape_handler')

    def _execute_selenium(
        self,
        initial_df: Optional[pd.DataFrame] = None
    ) -> Tuple[pd.DataFrame, List[ScraperError]]:
        '''
        Execute the scraper with Selenium library.

        Parameters:
        -----------
        initial_df : `Optional[pd.DataFrame]`
          A Pandas dataframe containing a `url` column and potentially additional columns. Additional
          columns are treated as extra parameters that the scraper can use for flow control or data
          enrichment.

          If `initial_df` is `None`, the urls will be fetched from the `urls` parameter given in the
          class instantiaton. If no urls are found an error will be thrown.

        Returns:
        --------
        df, errors : `Tuple[pd.DataFrame, List[ScraperError]]`
          A tuple having the scraping results in the first parameter and a list of errors captured in
          the process in the second parameter.
        '''
        df = pd.DataFrame(columns=self.COLUMNS)
        errors = []
        urls_and_extras = self._parse_urls_and_extras(initial_df)
        for i, url_and_extras in tqdm(list(enumerate(urls_and_extras)), mininterval=60, unit='url'):
            url, extras = url_and_extras
            self._load_html_selenium(url)
            try:
                urls_df = self.scrape_handler(
                    url, None, self._driver, **extras)
                df = pd.concat([df, urls_df], ignore_index=True,
                               verify_integrity=True)
            except Exception as e:
                errors.append(ScraperError(url, str(e)))
                if len(errors) > len(urls_and_extras) * self.ERROR_THRESHOLD:
                    if not self.TEST_MODE:
                        self.driver.quit()
                    raise ThresholdException(self.ERROR_THRESHOLD * 100) from e
            if self.RESET_AFTER is not None and (i + 1) % self.RESET_AFTER == 0:
                log.info('Reseting Selenium driver')
                self.driver.quit()
                self._driver = None
        self.driver.quit()
        self._driver = None
        return df, errors

    def _execute_requests(
            self,
            initial_df: Optional[pd.DataFrame] = None) -> Tuple[pd.DataFrame, List[ScraperError]]:
        '''
        Execute the scraper with requests library.

        Parameters:
        -----------
        initial_df : `Optional[pd.DataFrame]`
          A Pandas dataframe containing a `url` column and potentially additional columns. Additional
          columns are treated as extra parameters that the scraper can use for flow control or data
          enrichment.

          If `initial_df` is `None`, the urls will be fetched from the `urls` parameter given in the
          class instantiaton. If no urls are found an error will be thrown.

        Returns:
        --------
        df, errors : `Tuple[pd.DataFrame, List[ScraperError]]`
          A tuple having the scraping results in the first parameter and a list of errors captured in
          the process in the second parameter.
        '''
        df = pd.DataFrame(columns=self.COLUMNS)
        errors = []
        urls_and_extras = self._parse_urls_and_extras(initial_df)
        for url_and_extras in tqdm(urls_and_extras, mininterval=60, unit='url'):
            url, extras = url_and_extras
            html = self._load_html_requests(url)
            if html is not None:
                try:
                    urls_df = self.scrape_handler(url, html, None, **extras)
                    df = pd.concat(
                        [df, urls_df], ignore_index=True, verify_integrity=True)
                except Exception as e:
                    errors.append(ScraperError(url, str(e)))
                    if len(errors) > len(self.urls) * self.ERROR_THRESHOLD:
                        raise ThresholdException(
                            self.ERROR_THRESHOLD * 100) from e
            else:
                errors.append(ScraperError(url, 'Html not loaded'))
        return df, errors

    def _parse_urls_and_extras(self, df: Optional[pd.DataFrame] = None) -> List[Tuple[str, dict]]:
        '''
        Extract urls and extra parameters for scraper execution.

        Parameters:
        -----------
        df : `Optional[pd.DataFrame]`
          A Pandas dataframe containing a `url` column and potentially additional columns. Additional
          columns are treated as extra parameters that the scraper can use for flow control or data
          enrichment.

          If `initial_df` is `None`, the urls will be fetched from the `urls` parameter given in the
          class instantiaton. If no urls are found an error will be thrown.

        Returns:
        --------
        urls_and_extras : `List[Tuple[str, dict]]`
          A list of pairs consisting on a url and a dict with extra arguments.
        '''
        if df is None:
            if self.urls is None:
                raise AttributeError('set urls or df for scraper to execute')
            return [(url, {}) for url in self.urls]
        else:
            if 'url' not in df.columns:
                raise AttributeError('df must have a `url` column')
            urls = list(df.url)
            extras = df.loc[:, df.columns != 'url'].to_dict(orient='records')
            if len(extras) > 0:
                return [(urls[i], extras[i]) for i in range(len(urls))]
            else:
                return [(url, {}) for url in urls]

    def execute(self, df: Optional[pd.DataFrame] = None) -> Tuple[pd.DataFrame, List[ScraperError]]:
        '''Scrape the setted dataframe and return a new pandas dataframe with the scraped data
        Parameters:
        -----------
        df : `Optional[pandas.DataFrame]`
          Optional pandas dataframe containing additional data for the scraper. The dataframe
          needs to have an `url` column, which will be used as the list of urls to scrap for
          the scraper run, overruling the `self.urls` list.
        '''
        return self._execute_selenium(df) if self.LOAD_JAVASCRIPT else self._execute_requests(df)
