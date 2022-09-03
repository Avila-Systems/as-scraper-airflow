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
