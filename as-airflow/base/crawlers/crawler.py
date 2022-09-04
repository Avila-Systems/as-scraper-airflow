from abc import ABCMeta
from typing import List
import requests
from base.headers import HEADERS


class Crawler(metaclass=ABCMeta):
    '''
    Base class for all Crawlers
    '''

    def crawl(self) -> List[str]:
        '''
        Crawl the sitemap to extract urls.

        Returns:
        --------
        urls : `List[str]`
          A list of urls.
        '''
        raise NotImplementedError('Implement crawl')

    def _load_html(self, url: str) -> bytes:
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
        response = requests.get(url, headers=HEADERS)
        if response.status_code == ok_status_code:
            return response.content
        else:
            response.raise_for_status()
