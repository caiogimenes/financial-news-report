from dataclasses import dataclass


@dataclass
class Insight:
    datetime: str
    company: str
    fulltext: str
    url: str
    sentiment: str
    score: float
    ner: str

    @staticmethod
    def all():
        return ["datetime", "company", "fulltext", "url", "sentiment", "score", "ner"]
