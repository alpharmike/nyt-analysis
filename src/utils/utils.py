from pathlib import Path
import sys
import argparse
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).parent.parent.parent


def parse_args(args):
    if len(args) > 1:
        args = sys.argv[1:]
    else:
        args = []

    parser = argparse.ArgumentParser(description='NYT News Analysis Script Arguments')
    parser.add_argument('-sd', '--start_date', type=str, default="2022-01", help='Start date for fetching news')
    parser.add_argument('-ed', '--end_date', type=str, default=datetime.now().strftime("%Y-%m"),
                        help='End date for fetching news')
    parser.add_argument('-lu', '--live_update', type=bool, default=False, help='Live news update')

    if len(args) > 0 and args[0] in ('-h', '--help'):
        print('NYT News Analysis Script\nUse the runner to scrape the old and fetch the latest news in realtime')
        sys.exit(0)

    parsed_args = parser.parse_args(args)

    return parsed_args


def parse_input_date(date: str):
    try:
        parsed_date = datetime.strptime(date, "%Y-%m")
        return parsed_date.year, parsed_date.month

    except Exception as error:
        logger.error(str(error))
