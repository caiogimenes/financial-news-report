from finnhub_consumer import FinnhubConsumer

def main():
    # Initialize the FinnhubConsumer with your API key
    finnhub_consumer = FinnhubConsumer()
    # Fetch company news for a specific symbol
    news = finnhub_consumer.fetch_company_news()

    # Print the fetched news
    print(next(news))

if __name__ == "__main__":
    main()