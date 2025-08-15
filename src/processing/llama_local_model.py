import os
import ast
import requests


class LlamaLocalModel:
    def __init__(self):
        self.endpoint = os.environ.get("MODEL_ENDPOINT")
        self.model = os.environ.get("MODEL")

    def call(self, data):
        instruction = """"
            You are an API for sentiment analysis of financial news in english language.
            Your only task is to analyse the given text and return a sentiment.
            The sentiment can be 'Very Positive', 'Positive', 'Neutral', 'Negative', 'Very Negative'
            Besides the label, return a confidence score from 0.0 to 1.0, where 1.0 is total confidence.
            NEVER chats or adds any other explaining text.
            Answer JUST with an only JSON valid object that contains the 'label' and 'score' keys.
            Make sure the output is always formatted this way: {"label": "string", "score": float} 
        """
        chat_request = {
            "model": self.model,
            "messages": [
                {
                    "role": "system",
                    "content": instruction
                },
                {
                    "role": "user",
                    "content": data
                }
            ]
        }

        headers = {"Content-Type": "application/json"}

        # Send request to LLM API
        response = requests.post(
            self.endpoint,
            headers=headers,
            json=chat_request,
            timeout=30
        )

        # Check if the status code is not 200 OK
        if response.status_code != 200:
            raise Exception(
                f"API returned status code {response.status_code}: {response.text}"
            )

        # Parse the response
        chat_response = response.json()

        # Extract the assistant's message
        if chat_response.get('choices') and len(chat_response['choices']) > 0:
            result = chat_response['choices'][0]['message']['content'].strip()
            return ast.literal_eval(result)

        raise Exception("No response choices returned from API")
