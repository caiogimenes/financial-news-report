# Financial News Report

This project ingests and processes financial news using Python.

## Project Structure

- `ingestion/`: Responsible for collecting news from external sources (e.g., Finnhub) and sending them to Kafka.
- `processing/`: Consumes news from Kafka, processes them, and performs additional analysis.

## How to Run

### Prerequisites

- Python 3.13+
- Docker (optional)
- Kafka running

### Installation