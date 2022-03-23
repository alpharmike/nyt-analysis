from src.utils.logger import init_logger
from src.utils.globals import database
from src.core.runner import Runner
from src.utils.utils import parse_args
import sys


def init():
    init_logger()


def main():
    init()
    args = parse_args(sys.argv)
    Runner.run(args.start_date, args.end_date, args.archive, args.live_update)


if __name__ == "__main__":
    main()
