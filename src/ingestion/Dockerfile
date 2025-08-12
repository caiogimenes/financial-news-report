FROM python:3.13

LABEL authors="caiogimenes"

WORKDIR /src/ingestion
# Copy the requirements file and install dependencies
COPY . ./

RUN pip install --no-cache-dir -r requirements.txt

# Define the command to run your application when the container starts
CMD ["python", "app.py"]