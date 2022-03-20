from datetime import datetime
import json
import time

import requests
import logging

from apscheduler.schedulers.background import BackgroundScheduler

from src.utils.config_parser import default_config, parse_config
from src.utils.utils import parse_date
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
            result = response.json()
            logger.info(f"Fetched news for month: {month}, year: {year}")
            return result
        except Exception as error:
            logger.error(str(error))

    def send_data_to_kafka(self, data):
        for news_doc in data:
            self._producer.send(default_config["KAFKA"]["ARCHIVE_TOPIC"], news_doc)
            time.sleep(1)

    def fetch_news(self, start_month, start_year, end_month, end_year, archive=True, live_update=False):
        if archive and (start_month < 1 or start_month > 12 or end_month < 1 or end_month > 12 or start_month > end_month or start_year > end_year):
            raise ValueError

        if archive:
            month = start_month
            for year in range(start_year, end_year + 1):
                while month <= 12 and (year < end_year or month <= end_month):
                    try:
                        data = self._fetch_news_by_date(month, year)
                        news_docs = data["response"]["docs"]
                        self.send_data_to_kafka(news_docs)
                    except Exception as error:
                        logger.error(f"Could not parse NYT API response - {str(error)}")
                    month += 1
                month = month % 12

        if live_update:
            self.polling_update()

    def polling_update(self):
        scheduler = BackgroundScheduler()
        # Allows the task to be late for 30 minutes
        scheduler.add_job(self.update_news, 'interval', seconds=int(default_config["STREAMER"]["UPDATE_INTERVAL"]),
                          misfire_grace_time=1800, max_instances=10)
        scheduler.start()

    def update_news(self):
        current_time = datetime.now()
        current_month = current_time.month
        current_year = current_time.year
        try:
            data = self._fetch_news_by_date(str(current_month), str(current_year))
            news_docs = data["response"]["docs"]
            for news_doc in news_docs:
                if self.validate_news_publish_date(news_doc):
                    self._producer.send(default_config["KAFKA"]["ARCHIVE_TOPIC"], news_doc)
                    logger.info(f"News updated: Added news with id: {news_doc['_id']}")
        except Exception as error:
            logger.error(f"Could not fetch live news - {str(error)}")

    def validate_news_publish_date(self, news_doc):
        try:
            publish_date = news_doc["pub_date"]
            parsed_published_date = parse_date(publish_date, "%Y-%m-%dT%H:%M:%S%z")
            current_time = datetime.now()
            duration = current_time - parsed_published_date
            seconds_diff = duration.total_seconds()
            if seconds_diff <= int(default_config["STREAMER"]["UPDATE_INTERVAL"]):
                return True
            return False
        except Exception as error:
            logger.error(
                f"Could not validate news publish - (id: {news_doc['_id']}  date: {news_doc['pub_date']}) - {str(error)}")
            return False
