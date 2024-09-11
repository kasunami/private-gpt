# kafka_processor.py

from kafka import KafkaConsumer, KafkaProducer
from pydantic import BaseModel, ValidationError
from typing import Optional, List
from private_gpt.di import global_injector
from private_gpt.open_ai.extensions.context_filter import ContextFilter
from private_gpt.open_ai.openai_models import OpenAIMessage

from private_gpt.server.chat.chat_router import ChatBody, chat_completion
from private_gpt.server.chat.chat_service import ChatService
from private_gpt.settings.settings import settings

import json

class CompletionsBody(BaseModel):
    prompt: str
    system_prompt: Optional[str] = None
    use_context: bool = False
    context_filter: Optional[ContextFilter] = None
    include_sources: bool = True
    stream: bool = False

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "prompt": "How do you fry an egg?",
                    "system_prompt": "You are a rapper. Always answer with a rap.",
                    "stream": False,
                    "use_context": False,
                    "include_sources": False,
                }
            ]
        }
    }


def convert_body_to_messages(body: CompletionsBody) -> List[OpenAIMessage]:
    messages = [OpenAIMessage(content=body.prompt, role="user")]
    if body.system_prompt:
        messages.insert(0, OpenAIMessage(content=body.system_prompt, role="system"))
    return messages


def process_message(self, message_value: str) -> bool:
    try:
        body = CompletionsBody.parse_raw(message_value)
        chat_body = ChatBody(
            messages=convert_body_to_messages(body),
            use_context=body.use_context,
            stream=body.stream,
            include_sources=body.include_sources,
            context_filter=body.context_filter,
        )

        chat_service: ChatService = global_injector.get(ChatService)

        # Handle streaming responses and send each chunk immediately
        for chunk in chat_completion(chat_service, chat_body):
            content = chunk.choices[0].delta.content
            if content is not None:
                # Send the chunk to Kafka as it arrives
                self.producer.send(self.output_topic, value=content.encode('utf-8'))
                self.producer.flush()

        return True  # Indicate successful completion

    except ValidationError as e:
        # Log the error or handle it as needed
        print(f"Error processing message: {e}")
        return False  # Indicate failure

class KafkaProcessor:
    def __init__(self, kafka_address, kafka_port, input_topic, output_topic):
        self.bootstrap_servers = f"{kafka_address}:{kafka_port}"

        self.consumer_config = {
            'bootstrap_servers': self.bootstrap_servers,
            'group_id': 'completions-group',
            'auto_offset_reset': 'earliest',
            'enable_auto_commit': False  # Adjust if needed
        }

        self.producer_config = {
            'bootstrap_servers': self.bootstrap_servers
        }

        self.input_topic = input_topic
        self.output_topic = output_topic

        self.consumer = KafkaConsumer(self.input_topic, **self.consumer_config)
        self.producer = KafkaProducer(**self.producer_config)

    def consume_messages(self):
        while True:
            messages = self.consumer.poll(600000, 1)
            if not messages:
                continue

            tp = list(messages.keys())[0]
            msg = messages[tp][0]

            print(f"Received message from partition {tp.partition}: {msg.value.decode('utf-8')}")

            # Pause fetching to process the current message
            self.consumer.pause()

            # Pass 'self' to process_message
            success = process_message(self, msg.value.decode('utf-8'))

            if success:
                # Commit and resume fetching only if processing was successful
                self.consumer.commit()
                self.consumer.resume()
            else:
                # Handle the failure case appropriately (e.g., log the error, retry, etc.)
                print("Error processing message. Skipping commit and continuing...")

    def start(self):
        try:
            self.consume_messages()
        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()

# Create a default instance of KafkaProcessor
kafka_processor = KafkaProcessor(
    kafka_address=settings().kafka.address,
    kafka_port=settings().kafka.port,
    input_topic='prompt_request',
    output_topic='prompt_response'
)