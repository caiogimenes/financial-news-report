# Financial News Report

This project implements a data pipeline for the real-time ingestion and processing of financial news. The solution is built on a microservices architecture, containerized with Docker, and leverages Python, Kafka, and PostgreSQL to create a decoupled and scalable data flow.

The primary goal is to extract valuable insights from unstructured news data by performing sentiment analysis using a local Large Language Model (LLM).

-----

## 🏗️ Architecture and Data Flow

The application is composed of independent services orchestrated via `docker-compose.yml`, ensuring a consistent development and production environment. Asynchronous communication between services is managed by Apache Kafka, which promotes loose coupling and resilience.

### Architectural Overview

The architecture is designed as a multi-stage data pipeline, where each service has a single responsibility. This separation of concerns allows for independent scaling and development of each component.

  * **`ingestion-layer`**: This service acts as the data producer. It is responsible for fetching raw data from an external API and introducing it into our system via Kafka.

  * **`processing-layer`**: This service consumes the raw data, enriches it through sentiment analysis, and then persists the processed information into the database.

  * **`db`**: A PostgreSQL instance serves as the data sink, storing the structured insights for future analysis and reporting.

### Data Flow

The pipeline operates as follows:

1.  **Ingestion**: The `ingestion-layer` periodically fetches company news from the Finnhub API. The collected data is then published as messages to a Kafka topic (`raw_data`), serving as the event source for the rest of the pipeline.

2.  **Processing**: The `processing-layer` consumes messages from the `raw_data` topic. For each news item, it performs the following steps:

      * **Preprocessing**: The news text is cleaned and prepared. Fields such as `headline` and `summary` are merged to form a unified `fulltext` for more effective analysis.
      * **Sentiment Analysis**: The preprocessed text is sent to a local Llama 3.2 model API. The model returns a sentiment classification (e.g., 'Positive', 'Negative') and a confidence score.
      * **Storage**: The enriched data is structured into an `Insight` object and persisted in a PostgreSQL table.

3.  **Database**: The PostgreSQL instance stores the processed insights. The `insights` table schema is designed to support future analysis and the generation of reports.

-----

## ⚙️ Implementation Details

  * **Finnhub Connector**: The `FinnhubConnector` manages the state of the last fetched news for each company symbol to avoid duplicate requests and optimize API usage. The class inherits from an abstract `Connector`, which allows for easy extension to other data sources in the future.

  * **Kafka Producer**: The `KafkaProducer` class abstracts the logic for producing messages to Kafka, and is used by both the ingestion and processing services.

  * **Testing**: The project includes unit tests for critical components, such as the database writer, using `pytest` to ensure code integrity.

-----

## 🏁 Running the Project

### Prerequisites

  * Python 3.13+
  * Docker and Docker Compose
  * An accessible Apache Kafka instance
  * An API endpoint for the Llama 3.2 model

### Configuration

1.  **Clone the repository**:
    ```bash
    git clone https://github.com/caiogimenes/financial-news-report.git
    cd financial-news-report
    ```
2.  **Environment Variables**: Create a `backend.env` file in the project root and configure the credentials for the Finnhub API, Kafka, and PostgreSQL, as well as the model endpoints.

### Initialization

To build the Docker images and start the containers, run the following command:

```bash
docker-compose up --build
```

The services will be started, and the pipeline will begin ingesting and processing data.

-----

## 🔮 Future Enhancements

  * **Named Entity Recognition (NER)**: Extend the processing service to include the extraction of entities (such as company names, people, and locations) from the news text.
  * **Reporting**: Utilize the data stored in PostgreSQL to generate reports and dashboards, possibly with tools like Quarto, to visualize sentiment trends over time.
  * **Monitoring**: Implement monitoring tools to observe the health of the services and the message flow in Kafka.
