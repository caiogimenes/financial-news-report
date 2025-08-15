import os
import datetime as dt
from collections import defaultdict
from finnhub import Client
from config import config
from base import Connector


class FinnhubConnector(Connector):
    def __init__(self):
        self.api_key = self.set_api_key()
        self.client = Client(api_key=os.environ['FINNHUB_API_KEY'])

        # Keep track of last updates - symbol: last_update_timestamp
        self.last_update = defaultdict(str)

        self._from = dt.datetime.fromisoformat(
            config.get("DEFAULT_DATETIME")
        ).timestamp()

    def set_api_key(self):
        return os.environ.get("FINNHUB_API_KEY")

    def update_last_fetch(self, symbol, datetime):
        self.last_update[symbol] = datetime

    def fetch_company_news(
            self,
            symbol: str,
            to: str = str(dt.date.today())  # Get most recent data
    ):
        to_as_timestamp = dt.datetime.fromisoformat(to).timestamp()
        _from_as_string = str(dt.date.fromtimestamp(self._from))

        if self._from >= to_as_timestamp:
            return

        company_news = self.client.company_news(
            symbol,
            _from=_from_as_string,
            to=to
        )

        for news in sorted(company_news, key=lambda x: x["datetime"]):
            last_update = self.last_update.get(symbol, 0)
            if news.get("datetime") > last_update:
                # The lines above prevents unnecessary requests to Finnhub
                self.update_last_fetch(symbol, news.get("datetime"))
                self._from = max(self._from, last_update)

                yield news

            # If there's no new data
            continue
