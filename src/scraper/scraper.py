import json
import requests
import logging
from src.utils.config_parser import default_config, parse_config
from kafka import KafkaProducer

logger = logging.getLogger(__name__)


DATA = {
    "abstract": "The Crimson Tide will face the Georgia Bulldogs on Jan. 10 for the national championship.",
    "web_url": "https://www.nytimes.com/2021/12/31/sports/ncaafootball/alabama-cincinnati-cotton-bowl-score.html",
    "snippet": "The Crimson Tide will face the Georgia Bulldogs on Jan. 10 for the national championship.",
    "lead_paragraph": "ARLINGTON, Texas — This was the moment the Cincinnati Bearcats had pined for all season. The roster is chock-full of local kids, overlooked by Ohio State in high school and largely underestimated now by the football elite, who put their place in the College Football Playoff down to a matter of necessity: The sport needed a fourth team in the field.",
    "print_section": "B",
    "print_page": "6",
    "source": "The New York Times",
    "multimedia": [
        {
            "rank": 0,
            "subtype": "xlarge",
            "caption": None,
            "credit": None,
            "type": "image",
            "url": "images/2021/12/31/sports/31cfb-cotton01/merlin_199794252_d5688c82-37da-401c-a721-d300940aebc6-articleLarge.jpg",
            "height": 400,
            "width": 600,
            "subType": "xlarge",
            "crop_name": "articleLarge",
            "legacy": {
                "xlarge": "images/2021/12/31/sports/31cfb-cotton01/merlin_199794252_d5688c82-37da-401c-a721-d300940aebc6-articleLarge.jpg",
                "xlargewidth": 600,
                "xlargeheight": 400
            }
        },
        {
            "rank": 0,
            "subtype": "jumbo",
            "caption": None,
            "credit": None,
            "type": "image",
            "url": "images/2021/12/31/sports/31cfb-cotton01/merlin_199794252_d5688c82-37da-401c-a721-d300940aebc6-jumbo.jpg",
            "height": 683,
            "width": 1024,
            "subType": "jumbo",
            "crop_name": "jumbo",
            "legacy": {}
        },
        {
            "rank": 0,
            "subtype": "superJumbo",
            "caption": None,
            "credit": None,
            "type": "image",
            "url": "images/2021/12/31/sports/31cfb-cotton01/merlin_199794252_d5688c82-37da-401c-a721-d300940aebc6-superJumbo.jpg",
            "height": 1365,
            "width": 2048,
            "subType": "superJumbo",
            "crop_name": "superJumbo",
            "legacy": {}
        },
        {
            "rank": 0,
            "subtype": "thumbnail",
            "caption": None,
            "credit": None,
            "type": "image",
            "url": "images/2021/12/31/sports/31cfb-cotton01/31cfb-cotton01-thumbStandard.jpg",
            "height": 75,
            "width": 75,
            "subType": "thumbnail",
            "crop_name": "thumbStandard",
            "legacy": {
                "thumbnail": "images/2021/12/31/sports/31cfb-cotton01/31cfb-cotton01-thumbStandard.jpg",
                "thumbnailwidth": 75,
                "thumbnailheight": 75
            }
        },
        {
            "rank": 0,
            "subtype": "thumbLarge",
            "caption": None,
            "credit": None,
            "type": "image",
            "url": "images/2021/12/31/sports/31cfb-cotton01/31cfb-cotton01-thumbLarge.jpg",
            "height": 150,
            "width": 150,
            "subType": "thumbLarge",
            "crop_name": "thumbLarge",
            "legacy": {}
        }
    ],
    "headline": {
        "main": "Alabama Rolls Past Cincinnati, 27-6, in College Football Playoff Semifinal",
        "kicker": "Alabama 27, Cincinnati 6",
        "content_kicker": None,
        "print_headline": "The Tide Rolls On",
        "name": None,
        "seo": None,
        "sub": None
    },
    "keywords": [
        {
            "name": "subject",
            "value": "Football (College)",
            "rank": 1,
            "major": "N"
        },
        {
            "name": "organizations",
            "value": "University of Alabama",
            "rank": 2,
            "major": "N"
        },
        {
            "name": "organizations",
            "value": "University of Cincinnati",
            "rank": 3,
            "major": "N"
        },
        {
            "name": "subject",
            "value": "Cotton Bowl (Football Game)",
            "rank": 4,
            "major": "N"
        },
        {
            "name": "subject",
            "value": "College Football Playoff National Championship",
            "rank": 5,
            "major": "N"
        }
    ],
    "pub_date": "2022-01-01T00:01:35+0000",
    "document_type": "article",
    "news_desk": "Sports",
    "section_name": "Sports",
    "subsection_name": "College Football",
    "byline": {
        "original": "By Billy Witz",
        "person": [
            {
                "firstname": "Billy",
                "lastname": "Witz",
                "role": "reported",
                "organization": "",
                "rank": 1
            }
        ],
    },
    "type_of_material": "News",
    "_id": "nyt://article/50aa9e4f-5d58-5577-9492-99f66707b3ce",
    "word_count": 1245,
    "uri": "nyt://article/50aa9e4f-5d58-5577-9492-99f66707b3ce"
}


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
            return result
        except Exception as error:
            logger.error(str(error))

    def fetch_news(self, start_month, start_year, end_month, end_year, live_update=False):
        if start_month < 1 or start_month > 12 or end_month < 1 or end_month > 12 or start_month > end_month or start_year > end_year:
            raise ValueError

        month = start_month
        for year in range(start_year, end_year + 1):
            while month <= 12 and (year < end_year or month <= end_month):
                # data = self._fetch_news_by_date(month, year)
                try:
                    # data = data["response"]["docs"]
                    # data = data[0]
                    self._producer.send(default_config["KAFKA"]["ARCHIVE_TOPIC"], DATA)
                except Exception as error:
                    logger.error(f"Could not parse NYT API response - ${str(error)}")
                month += 1
            month = month % 12
