from typing import Optional
from selenium.webdriver import Firefox
from selenium.webdriver.common.by import By
import pandas as pd
from as_scraper.base.scraper import Scraper

class YellowPagesScraper(Scraper):
    COLUMNS = ['name', 'url']
    LOAD_JAVASCRIPT = True

    def scrape_handler(self, url: str, html: Optional[str] = None, driver: Optional[Firefox] = None, **kwargs) -> pd.DataFrame:
        rows = []
        div_tag = driver.find_element(By.CLASS_NAME, "row-content")
        div_tag = div_tag.find_element(By.CLASS_NAME, "row")
        section_tags = div_tag.find_elements(By.TAG_NAME, "section")
        for section_tag in section_tags:
            a_tags = section_tag.find_elements(By.TAG_NAME, "a")
            for a_tag in a_tags:
                city_name = a_tag.text
                city_url = a_tag.get_attribute("href")
                rows.append({"name": city_name, "url": city_url})
        df = pd.DataFrame(rows, columns=self.COLUMNS)
        return df
