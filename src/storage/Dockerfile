FROM python:3.13

LABEL authors="caiogimenes"

WORKDIR /app

# Copy the requirements file and install dependencies
COPY ../../requirements.txt ./app/requeriments.txt

RUN pip install --no-cache-dir -r requirements.txt

COPY ../.. .

# Define the command to run your application when the container starts
CMD ["python", "your_app.py"]