from datetime import datetime


def get_dag_start_date():
    """
    Static start time based upon https://airflow.apache.org/docs/stable/faq.html#what-s-the-deal-with-start-date.

    :return:
    :rtype:
    """
    return datetime(2020, 2, 1)
