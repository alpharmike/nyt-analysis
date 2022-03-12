import json
import requests
import logging
from src.utils.config_parser import default_config, parse_config
from kafka import KafkaProducer

logger = logging.getLogger(__name__)


class ScrapeHandler:
    def __init__(self):
        self._producer: KafkaProducer = None
        self._init_kafka()

    def _init_kafka(self):
        try:
            self._producer = KafkaProducer(
                bootstrap_servers=default_config["KAFKA"]["BOOTSTRAP_SERVERS"],
                max_request_size=int(default_config["KAFKA"]["MAX_REQUEST_SIZE"]),
                buffer_memory=int(default_config["KAFKA"]["BUFFER_MEMORY"]),
                value_serializer=lambda value: json.dumps(value).encode('utf-8')
            )
        except Exception as error:
            logger.error(str(error))

    def _fetch_news_by_date(self, month, year):
        try:
            secret_config = parse_config("secret")
            response = requests.get(
                f"{default_config['NYT_API']['ARCHIVE']}/{year}/{month}.json?api-key={secret_config['API']['API_KEY']}",
                timeout=360,
                headers={'api-key': secret_config['API']['API_KEY']}
            )
            print(response)
            result = response.json()
            return result
        except Exception as error:
            logger.error(str(error))

    def fetch_news(self, start_month, start_year, end_month, end_year, live_update=False):
        if start_month < 1 or start_month > 12 or end_month < 1 or end_month > 12 or start_month > end_month or start_year > end_year:
            raise ValueError

        month = start_month
        for year in range(start_year, end_year + 1):
            while month <= 12 and (year < end_year or month <= end_month):
                data = self._fetch_news_by_date(month, year)
                self._producer.send(default_config["KAFKA"]["ARCHIVE_TOPIC"], data)
                month += 1
            month = month % 12
