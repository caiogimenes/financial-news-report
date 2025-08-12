import abc


class Connector(abc.ABC):
    @abc.abstractmethod
    def set_api_key(self):
        pass

    @abc.abstractmethod
    def update_last_fetch(self, symbol, timestamp):
        pass

    @abc.abstractmethod
    def fetch_company_news(self, symbol, _from, to):
        pass
