import requests
import logging
from src.utils.config_parser import default_config, parse_config

logger = logging.getLogger(__name__)


def fetch_news_by_date(month, year):
    try:
        secret_config = parse_config("secret")
        response = requests.get(
            f"{default_config['NYT_API']}/{year}/{month}.json?api-key={secret_config['API']['API_KEY']}",
            headers={'api-key': secret_config['API']['API_KEY']}
        )
        result = response.json()
        return result
    except Exception as error:
        logger.error(str(error))


def fetch_news(start_month, start_year, end_month, end_year):
    if start_month < 1 or start_month > 12 or end_month < 1 or end_month > 12 or start_month > end_month or start_year > end_year:
        raise ValueError

    month = start_month
    for year in range(start_year, end_year + 1):
        while month <= 12 and (year < end_year or month <= end_month):
            data = fetch_news_by_date(month, year)
            month += 1
        month = month % 12


