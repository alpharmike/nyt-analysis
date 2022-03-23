from apscheduler.schedulers.background import BackgroundScheduler
from src.scraper.scraper import ScrapeHandler
from abc import ABC, abstractmethod
from src.utils.utils import parse_input_date
from src.utils.globals import database
from src.streamer.transformer import run_spark_streamer
import logging


class Runner(ABC):

    @staticmethod
    def run(start_date, end_date, archive=True, live_update=False):
        logger = logging.getLogger(__name__)
        logger.info("Bootstrapping...")

        scraper: ScrapeHandler = ScrapeHandler()
        start_year, start_month = parse_input_date(start_date)
        end_year, end_month = parse_input_date(end_date)
        arguments = [
            start_month,
            start_year,
            end_month,
            end_year,
            archive,
            live_update
        ]

        scheduler = BackgroundScheduler()
        # Run date (run_date) is set to now if not provided
        scheduler.add_job(scraper.fetch_news, 'date', args=arguments, misfire_grace_time=None)
        scheduler.start()

        # Check for changes to database in background
        scheduler = BackgroundScheduler()
        # Run date (run_date) is set to now if not provided
        scheduler.add_job(database.watch_for_change, 'date', args=[], misfire_grace_time=None)
        scheduler.start()

        # Initialize and run spark streamer
        run_spark_streamer()
