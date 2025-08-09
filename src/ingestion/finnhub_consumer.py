import os

import finnhub

class FinnhubConsumer:
    def __init__(self, api_key: str = None):
        if not api_key:
            os.environ["FINNHUB_API_KEY"] = 'd2bkv1pr01qvh3vc94hgd2bkv1pr01qvh3vc94i0' # Set your Finnhub API key here

        self.client = finnhub.Client(api_key=os.environ['FINNHUB_API_KEY'])
        # Initialize connection to Finnhub API

    def fetch_company_news(self, symbol: str = 'AAPL', _from: str = "2025-06-01", to: str = "2025-08-09"):
        for news in self.client.company_news(symbol, _from, to):
            yield news