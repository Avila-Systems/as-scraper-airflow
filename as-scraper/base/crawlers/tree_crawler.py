from typing import List
from bs4 import BeautifulSoup
from base.crawlers import Crawler


class TreeCrawler(Crawler):
    '''
    Crawl a website through a sitemap that has a tree structure.

    Tree sitemaps are sitemaps containing further sitemaps in its root.

    The class is configured for Tree sitemaps of 1 level of depth.
    '''

    # Override this field to define sitemap to crawl.
    sitemap_parent: str = None

    def crawl(self) -> List[str]:
        '''
        Crawl the sitemap to extract urls.

        Returns:
        --------
        urls : `List[str]`
          A list of urls.
        '''
        urls: List[str] = []
        sitemaps = self._get_sitemaps_from_parent()
        filtered_sitemaps = list(filter(self.should_crawl, sitemaps))
        for sitemap in filtered_sitemaps:
            sitemap_content = self._load_html(sitemap)
            soup = BeautifulSoup(sitemap_content, 'xml')
            urls.extend([loc.get_text() for loc in soup.select('url > loc')])
        return urls

    def _get_sitemaps_from_parent(self) -> List[str]:
        '''
        Extract sitemaps from a sitemap root.

        Returns:
        --------
        sitemaps : `List[str]`
          A list of sitemaps found in the `sitemap_parent`.
        '''
        if not self.sitemap_parent:
            raise AttributeError('sitemap_index must be set')
        sitemap_content = self._load_html(self.sitemap_parent)
        soup = BeautifulSoup(sitemap_content, 'xml')
        return [loc.get_text() for loc in soup.find_all('loc')]

    def should_crawl(self, url: str) -> bool:
        '''
        Decide wether a given sitemap url should be crawled or not.

        Parameters:
        -----------
        url : `str`
          Sitemap url

        Returns:
        --------
        should_crawl : `bool`
          True if the sitemap should be crawled.
        '''
        return True
