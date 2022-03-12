from src.scraper.scraper import ScrapeHandler
from abc import ABC, abstractmethod
from src.utils.utils import parse_input_date


class Runner(ABC):

    @staticmethod
    def run(start_date, end_date, live_update):
        scraper: ScrapeHandler = ScrapeHandler()
        start_year, start_month = parse_input_date(start_date)
        end_year, end_month = parse_input_date(end_date)

        scraper.fetch_news(
            start_month,
            start_year,
            end_month,
            end_year,
            live_update
        )
