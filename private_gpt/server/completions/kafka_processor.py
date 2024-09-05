# kafka_processor.py
from kafka import KafkaConsumer, KafkaProducer
from pydantic import BaseModel, ValidationError
from typing import Optional, List
from private_gpt.open_ai.extensions.context_filter import ContextFilter
from private_gpt.open_ai.openai_models import (
    OpenAICompletion,
    OpenAIMessage,
)
from private_gpt.server.chat.chat_router import ChatBody, chat_completion

from private_gpt.server.chat.chat_service import ChatService
from private_gpt.components.llm.llm_component import LLMComponent
from private_gpt.components.vector_store.vector_store_component import VectorStoreComponent
from private_gpt.components.embedding.embedding_component import EmbeddingComponent
from private_gpt.components.node_store.node_store_component import NodeStoreComponent
from private_gpt.settings.settings import Settings

import json

# Kafka configuration variables
KAFKA_ADDRESS = '192.168.88.176'
KAFKA_PORT = 9092  # Updated port to match your Kafka setup

class CompletionsBody(BaseModel):
    prompt: str
    system_prompt: Optional[str] = "Always format your response as a valid JSON object, even if the request doesn't explicitly ask for it."
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


def process_message(message_value: str) -> str:  # Return type is now str (JSON string)
    try:
        body = CompletionsBody.parse_raw(message_value)
        chat_body = ChatBody(
            messages=convert_body_to_messages(body),
            use_context=body.use_context,
            stream=body.stream,
            include_sources=body.include_sources,
            context_filter=body.context_filter,

        )
        completion_response = chat_completion(None, chat_body)
        # Wrap the successful response in a JSON structure with status
        return json.dumps({
            "status": "success",
            "data": completion_response.model_dump_json()  # Assuming model_dump_json returns a dict
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

        # 1. Obtain Dependencies (Adjust based on your project)
        try:
            settings = Settings()  # Load settings
            llm_component = LLMComponent(settings)
            vector_store_component = VectorStoreComponent(settings)
            embedding_component = EmbeddingComponent(settings)
            node_store_component = NodeStoreComponent(settings)
        except Exception as e:
            # Handle potential errors during component initialization
            print(f"Error initializing components: {e}")
            # ... (Send error message to Kafka or handle appropriately)
            return

        # 2. Create ChatService Instance
        try:
            chat_service = ChatService(
                settings=settings,
                llm_component=llm_component,
                vector_store_component=vector_store_component,
                embedding_component=embedding_component,
                node_store_component=node_store_component
            )
        except Exception as e:
            # Handle potential errors during ChatService creation
            print(f"Error creating ChatService: {e}")
            # ... (Send error message to Kafka or handle appropriately)
            return

        for msg in self.consumer:
            print(f"Received message: {msg.value.decode('utf-8')}")

            completion_response = process_message(msg.value.decode('utf-8'))
            self.producer.send(self.output_topic, value=completion_response.encode('utf-8'))
            self.producer.flush()

    def start(self):
        try:
            self.consume_messages()
        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()

# Create a default instance of KafkaProcessor
kafka_processor = KafkaProcessor(
    kafka_address=KAFKA_ADDRESS,
    kafka_port=KAFKA_PORT,
    input_topic='prompt_request',
    output_topic='prompt_response'
)