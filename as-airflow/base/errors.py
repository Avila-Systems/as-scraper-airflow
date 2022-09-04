from collections import namedtuple


ScraperError = namedtuple('ScraperError', 'url message')
TaskError = namedtuple('TaskError', 'run_id start_date task_id message')
