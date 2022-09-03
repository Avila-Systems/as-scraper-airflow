from setuptools import find_packages, setup


def get_long_description():
    with open('README.md') as file:
        return file.read()


def get_install_requires():
    with open('requirements.txt') as file:
        return file.readlines()


setup(
    name='as-scraper',
    version='1.0.0',
    description='Python library for scraping inside Airflow.',
    long_description=get_long_description(),
    long_description_content_type='text/markdown',
    author='Alvaro Avila',
    author_email='almiavicas@gmail.com',
    url='https://github.com/Avila-Systems/as-scraper',
    project_urls={
        'Github Project': 'https://github.com/Avila-Systems/as-scraper',
        'Issue Tracker': 'https://github.com/Avila-Systems/as-scraper/issues',
    },
    packages=find_packages(
        include=['base', 'base.*', 'operators', 'operators.*'],
    ),
    install_requires=get_install_requires(),
    python_requires=">=3.6",
    classifiers=[
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Operating System :: POSIX',
        'License :: MIT License',
        'Topic :: Software Development :: Libraries'
    ]
)
