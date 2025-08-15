# Financial News Report

This project ingests and processes financial news using Python. It leverages a microservices architecture with Docker to create a scalable and maintainable data pipeline. The pipeline fetches financial news from Finnhub, performs sentiment analysis using a local Llama model, and prepares the data for storage and reporting.

-----

## 🚀 Project Overview

The primary goal of this project is to provide a real-time financial news analysis tool. It continuously ingests news articles, processes them to extract sentiment, and will, in future implementations, store the results in a database for further analysis and reporting.

-----

## 🏗️ Project Structure

The project is organized into the following directories:

  - `src/ingestion/`: This service is responsible for collecting news from external sources (e.g., Finnhub) and sending them to a Kafka topic.
  - `src/processing/`: This service consumes news from Kafka, processes it, performs sentiment analysis, and prepares it for storage.
  - `src/storage/`: (Future Implementation) This service will be responsible for writing the processed data into the PostgreSQL database.

-----

## ⚙️ How it Works

The application follows a data pipeline architecture:

1.  **Ingestion**: The `ingestion-layer` service, defined in the `docker-compose.yml` file, periodically fetches company news using the `FinnhubConnector`. This data is then sent as messages to the `raw_data` Kafka topic.

2.  **Processing**: The `processing-layer` service consumes messages from the `raw_data` Kafka topic. For each message, it performs the following steps:

      * **Preprocessing**: Cleans the data by removing unnecessary fields and merging the headline and summary into a single `fulltext` field.
      * **Sentiment Analysis**: Uses a local Llama model to perform sentiment analysis on the `fulltext`, generating a sentiment label ('Very Positive', 'Positive', 'Neutral', 'Negative', 'Very Negative') and a confidence score.
      * **NER (Named Entity Recognition)**: This is a placeholder for future implementation.

-----

## 🏁 Getting Started

### Prerequisites

  * Python 3.13+
  * Docker
  * A running Kafka instance
  * A running instance of the Llama 3.2 model accessible via the `MODEL_ENDPOINT` URL.

### Installation & Configuration

1.  **Clone the repository:**

    ```bash
    git clone https://github.com/caiogimenes/financial-news-report.git
    cd financial-news-report
    ```

2.  **Environment Variables**: Create a `backend.env` file in the root directory and populate it with the necessary credentials and configuration. Refer to the `backend.env` file for the required variables.

3.  **Dependencies**: The required Python packages for each service are listed in their respective `requirements.txt` files. These will be installed automatically when building the Docker containers.

### Running the Application

To run the application, use Docker Compose:

```bash
docker-compose up --build
```

This command will build the Docker images for the `ingestion-layer` and `processing-layer` services and start the containers.

-----

## 🔮 Future Implementations

### Database Integration

The project is set up with a PostgreSQL database service in the `docker-compose.yml` file. The `src/storage/` directory and the `db_writer.py` file are placeholders for the implementation that will write the processed data from the `processed_insights` Kafka topic into the database.

### Reporting with Quarto

Once the data is stored in the PostgreSQL database, the next step is to generate insightful reports. This will be achieved using **Quarto**, an open-source scientific and technical publishing system. The reports will visualize the sentiment trends of financial news over time, providing valuable insights for market analysis.

-----

## 🔧 Services

The `docker-compose.yml` file defines the following services:

  - **`ingestion-layer`**: Builds and runs the ingestion service from `src/ingestion`.
  - **`processing-layer`**: Builds and runs the processing service from `src/processing`.
  - **`db`**: A PostgreSQL database instance to store the processed data.

-----

## 🔑 Environment Variables

The following environment variables need to be set in the `backend.env` file:

| Variable              | Description                                                                  |
| --------------------- | ---------------------------------------------------------------------------- |
| `BASE_URL`            | The base URL for the model runner.                                           |
| `MODEL_ENDPOINT`      | The endpoint for the chat completions API of the Llama model.                |
| `MODEL`               | The name of the model to be used (e.g., `ai/llama3.2`).                       |
| `POSTGRES_DB`         | The name of the PostgreSQL database.                                         |
| `POSTGRES_USER`       | The username for the PostgreSQL database.                                    |
| `POSTGRES_PASSWORD`   | The password for the PostgreSQL database.                                    |
| `FINNHUB_API_KEY`     | Your API key for the Finnhub API.                                            |
| `BOOTSTRAP_SERVERS`   | The address of the Kafka bootstrap servers.                                  |
| `SECURITY_PROTOCOL`   | The security protocol for Kafka (e.g., `SASL_SSL`).                          |
| `SASL_MECHANISM`      | The SASL mechanism for Kafka (e.g., `PLAIN`).                                |
| `SASL_USERNAME`       | The SASL username for Kafka.                                                 |
| `SASL_PASSWORD`       | The SASL password for Kafka.                                                 |
| `CLIENT_ID`           | The client ID for the Kafka producer and consumer.                           |
| `GROUP_ID`            | The group ID for the Kafka consumer.                                         |
| `AUTO_OFFSET_RESET`   | The offset reset policy for the Kafka consumer (e.g., `earliest`).           |
| `SUBSCRIBE_TOPIC`     | The Kafka topic to which the ingestion service produces and the processing service consumes (`raw_data`). |
| `WRITE_TOPIC`         | The Kafka topic to which the processing service will produce the processed insights. |

-----

## 📦 Dependencies

### Ingestion Service

  * `confluent-kafka`
  * `finnhub-python`
  * `requests`

### Processing Service

  * `confluent-kafka`
  * `psycopg2-binary`
  * `requests`

-----

## 📜 License

This project is licensed under the MIT License.

-----

## 🤝 Contributing

Contributions are welcome\! Please feel free to submit a pull request or open an issue.

-----

## 📬 Contact

For any questions or suggestions, please contact the project owner.
