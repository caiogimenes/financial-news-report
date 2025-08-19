import ast
from llama_local_model import LlamaLocalModel
from insight import Insight


class ProcessFinnhubCompanyNews:
    def __init__(self):
        self.model = LlamaLocalModel()
        self.ner_client = None

    @staticmethod
    def _read_data(data):
        return ast.literal_eval(data)

    @staticmethod
    def _remove_unwanted(data):
        to_remove = [
            "id",
            "image",
            "source",
            "category"
        ]
        for key in to_remove:
            del data[key]
        return data

    @staticmethod
    def _merge_headline_summary(data):
        headline = data.get("headline")
        summary = data.get("summary")
        merged = headline + " " + summary
        data["fulltext"] = merged

        del data["headline"]
        del data["summary"]

        return data

    def preprocess(self, data):
        self._remove_unwanted(data)
        self._merge_headline_summary(data)
        return data

    def get_sentiment_analysis(self, data):
        result = self.model.call(data.get("fulltext"))
        data["sentiment"] = result["label"].strip()
        data["score"] = result["score"]

        return data

    def get_ner(self, data) -> dict:
        data["ner"] = ""
        return data

    def transform(self, data) -> Insight:
        data = self._read_data(data)
        data = self.preprocess(data)
        data = self.get_sentiment_analysis(data)
        data = self.get_ner(data)

        return Insight(
            datetime=data["datetime"],
            company=data["related"],
            fulltext=data["fulltext"],
            url=data["url"],
            sentiment=data["sentiment"],
            score=data["score"],
            ner=data["ner"],
        )
