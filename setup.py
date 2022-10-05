from setuptools import find_packages, setup


def get_long_description():
    with open('README.md') as file:
        return file.read()


setup(
    name='as-scraper-airflow',
    version='1.0.1',
    description='Python library for scraping inside Airflow.',
    long_description=get_long_description(),
    long_description_content_type='text/markdown',
    author='Alvaro Avila',
    author_email='almiavicas@gmail.com',
    url='https://github.com/Avila-Systems/as-scraper-airflow',
    project_urls={
        'Github Project': 'https://github.com/Avila-Systems/as-scraper-airflow',
        'Issue Tracker': 'https://github.com/Avila-Systems/as-scraper-airflow/issues',
    },
    packages=find_packages(
        include=['as_scraper_airflow*'],
    ),
    install_requires=[
        'apache-airflow>=2.2.3',
        'apache-airflow-providers-google',
        'as-scraper',
    ],
    python_requires=">=3.6",
    classifiers=[
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Operating System :: POSIX',
        'License :: OSI Approved :: MIT License',
        'Topic :: Software Development :: Libraries'
    ]
)
