from confluent_kafka import Consumer
import config
import ast
from huggingface_hub import InferenceClient


class ProcessFinnhubCompanyNews:
    def __init__(self, value):
        self.value = ast.literal_eval(value)
        self.sentiment_client = InferenceClient(
            provider="auto",
            api_key=config.HF_API_KEY,
        )
        self.ner_client = None

    def _remove_unwanted(self):
        to_remove = [
            "id",
            "image",
            "source",
            "url",
            "category"
        ]
        for key in to_remove:
            del self.value[key]

    def _merge_headline_summary(self):
        headline = self.value.get("headline")
        summary = self.value.get("summary")
        merged = headline + " " + summary
        self.value["fulltext"] = merged

        del self.value["headline"]
        del self.value["summary"]

    def preprocess(self):
        self._remove_unwanted()
        self._merge_headline_summary()

    def get_sentiment_analysis(self):
        result = self.sentiment_client.text_classification(
            self.value.get("fulltext"),
            model="tabularisai/multilingual-sentiment-analysis",
        )
        result.sort(key=lambda x: x.score, reverse=True)
        self.value["sentiment"] = result[0].label
        self.value["sentiment_score"] = result[0].score

    def get_ner(self) -> dict:
        return self.value

    def run_pipeline(self) -> dict:
        self.preprocess()
        self.get_sentiment_analysis()
        self.get_ner()

        return self.value


def consume():
    consumer = Consumer(config.config["kafka"])
    consumer.subscribe([config.SUBSCRIBE_TOPIC])
    try:
        while True:
            msg = consumer.poll(1)
            if msg and not msg.error():
                value = msg.value().decode()
                processor = ProcessFinnhubCompanyNews(value)
                new_value = processor.run_pipeline()
                print(new_value)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    consume()
