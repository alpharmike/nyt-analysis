from src.utils.logger import init_logger
from src.core.runner import Runner
from src.utils.utils import parse_args
import sys


def init():
    init_logger()


def main():
    init()
    args = parse_args(sys.argv[1:])
    Runner.run(args.start_date, args.end_date, args.live_update)


if __name__ == "__main__":
    main()
