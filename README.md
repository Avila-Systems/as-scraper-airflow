# as-scraper
Python library for scraping inside Airflow.

# Installation

The **as-scraper** library uses Geckodriver (Firefox) for scraping with the Selenium library.
In order to use it, you need to have an airflow image having the Geckodriver dependency.

We have the [as-airflow](https://github.com/Avila-Systems/as-airflow) Docker image for you to have airflow ready with the Geckodriver dependency.

To use this library follow the next steps:

### 1. Download the `docker-compose.yml` file from the Airflow docs.

Airflow provides the [docker-compose.yml](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html#docker-compose-yaml) file you need for this library.

You can directly copy the `docker-compose.yml` file from [here](https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml) or run the following command to download it:

```bash
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.3.4/docker-compose.yaml'
```

### 2. Modify the `docker-compose.yml` file.

After that, simply go into the *docker-compose.yml* file and change the airflow image used:

```yaml
...
version: '3'
x-airflow-common:
  &airflow-common
  # In order to add custom dependencies or upgrade provider packages you can use your extended image.
  # Comment the image line, place your Dockerfile in the directory where you placed the docker-compose.yaml
  # and uncomment the "build" line below, Then run `docker-compose build` to build the images.
  image: ${AIRFLOW_IMAGE_NAME:-almiavicas/as-airflow:2.2.3}
  ...
```

And that's it! You can now start using the as-scraper library.

# Usage

If you are starting a new Airflow project, before running your containers you need to run the following command to configure volumes:

```bash
mkdir dags/ logs/ plugins/
```

You can now run `docker-compose up` and you'll have your Airflow environment up & running.

## Creating a simple scraper

Lets say that we want to scrap [yellowpages.com](https://www.yellowpages.com). Our target data would be the popular cities that we can find in the [sitemap](https://www.yellowpages.com/sitemap) url.

Our output data will have two columns: `name` of the city and `url` which is linked to the city. For example, for *Houston*, we would want the following output:

| name | url |
|:-----|:----|
|Houston|https://www.yellowpages.com/houston-tx|

### Declaring our Scraper Class

So first we create a scraper that extends from the Scraper class, and define the `COLUMNS` variable to `['name', 'url']`.

Create the *plugins/scrapers/yellow_pages.py* file and type the following code into it:

```python
from as_scraper.base.scraper import Scraper


class YellowPagesScraper(Scraper):
    COLUMNS = ['name', 'url']

```

### Deciding wether to load javascript or not

Now, there are two execution options when running scrapers. We can either *load javascript* which uses the **Selenium** library, or not load javascript and use the *requests* library for http requests.

For this example, let's go ahead and use the **Selenium** library. To configure this, simply add the following variable to your scraper:

```python
from as_scraper.base.scraper import Scraper


class YellowPagesScraper(Scraper):
    COLUMNS = ['name', 'url']
    LOAD_JAVASCRIPT = True

```

### Defining the `scrape_handler`

And the magic comes in the next step. We will define the `scrape_handler` method in our class, which will have the responsibility to scrape a given url and extract the data from it.

> All scrapers must define the `scrape_handler` method.

```python
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

```

### Creating the DAG.

Now we want to create a DAG that will trigger the scraper. For that we will use the **ScraperToLogsOperator**

Create the *dags/yellow_pages.py* file and copy the following content into it:

```python


```