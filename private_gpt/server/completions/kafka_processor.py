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


def process_message(message_value: str) -> str:
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
        completion_response = chat_completion(chat_service, chat_body)

        content = completion_response.choices[0].message.content

        # Print the content for debugging before processing
        print("Content before JSON parsing:", content)

        try:
            # Attempt to parse the content directly as JSON
            data = json.loads(content)
        except json.JSONDecodeError as e:
            # If direct parsing fails, provide more context in the error message
            print(f"Error decoding JSON directly: {e}. Content: {content}")

            # Attempt preprocessing and retry, handling potential errors
            try:
                data = json.loads(content.encode().decode('unicode_escape'))
            except json.JSONDecodeError as e2:
                # Log the error with additional context
                print(f"Error decoding JSON after preprocessing: {e2}. Content: {content}")
                # Return an error response with details
                return json.dumps({
                    "status": "error",
                    "exception": "JSONDecodeError",
                    "message": "Failed to parse completion response as JSON",
                    "original_content": content  # Include the original content for further analysis
                })

        return json.dumps({
            "status": "success",
            "data": data
        })

    except ValidationError as e:
        # Return a JSON structure with error details and status
        return json.dumps({
            "status": "error",
            "exception": str(e),
            "location": "process_message - Parsing input message"
        })

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
            messages = self.consumer.poll(1000, 1)
            if not messages:
                continue
            # Since we're fetching one message at a time, there should be only one TopicPartition
            tp = list(messages.keys())[0]
            msg = messages[tp][0]  # Get the single ConsumerRecord

            print(f"Received message from partition {tp.partition}: {msg.value.decode('utf-8')}")
            # Pause and wait for current message to process
            self.consumer.pause()

            completion_response = process_message(msg.value.decode('utf-8'))
            self.producer.send(self.output_topic, value=completion_response.encode('utf-8'))
            self.consumer.commit()
            self.producer.flush()

            # Resume fetching messages
            self.consumer.resume()

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