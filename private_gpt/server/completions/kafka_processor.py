# kafka_processor.py
import asyncio
import json
import logging
import time
from typing import Optional, List

from kafka import KafkaConsumer, KafkaProducer
from pydantic import BaseModel, ValidationError

from private_gpt.di import global_injector
from private_gpt.open_ai.extensions.context_filter import ContextFilter
from private_gpt.open_ai.openai_models import OpenAIMessage
from private_gpt.server.chat.chat_router import ChatBody, chat_completion
from private_gpt.server.chat.chat_service import ChatService
from private_gpt.settings.settings import settings

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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


async def process_message(self, message_value: str) -> bool:
    logger.info("Started processing message.")
    logger.info(f"Message content: {message_value}")
    try:
        body = CompletionsBody.model_validate_json(message_value)
        logger.info("Parsed CompletionsBody from message.")

        chat_body = ChatBody(
            messages=convert_body_to_messages(body),
            use_context=body.use_context,
            stream=body.stream,
            include_sources=body.include_sources,
            context_filter=body.context_filter,
        )
        logger.info("Created ChatBody for chat_completion.")

        chat_service: ChatService = global_injector.get(ChatService)
        logger.info("Retrieved ChatService instance from global_injector.")

        # Get the StreamingResponse from chat_completion
        streaming_response = chat_completion(chat_service, chat_body)
        logger.info("Received StreamingResponse from chat_completion.")

        # Iterate over the lines (chunks) in the StreamingResponse using async for
        async for line in streaming_response.body_iterator:
            # Strip the 'data: ' prefix if it exists
            if line.startswith("data: "):
                line = line[len("data: "):]

            try:
                logger.info(f"Sending content to Kafka: {line}")
                self.producer.send(self.output_topic, value=line.encode('utf-8'))
                self.producer.flush()

            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error: {e} - Raw line: {line}")

            except KeyError as e:
                logger.error(f"Key error: {e} - Chunk: {line}")

        logger.info("Finished processing message successfully.")

    except ValidationError as e:
        logger.error(f"Validation error processing message: {e}")
        return False
    except Exception as e:  # Catch any other unexpected exceptions
        logger.error(f"Unexpected error processing message: {e}")
        return False

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

    def initialize_kafka(self):
        try:
            self.consumer = KafkaConsumer(self.input_topic, **self.consumer_config)
            self.producer = KafkaProducer(**self.producer_config)
            logger.info("Connected to Kafka broker successfully.")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka broker: {e}")
            raise

    def consume_messages(self):
        for msg in self.consumer:
            logger.info(f"Received message from topic: {msg.topic}, partition: {msg.partition}, offset: {msg.offset}")

            # Pause fetching to process the current message
            self.consumer.pause()

            # Create an event loop to run the async function
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                success = loop.run_until_complete(process_message(self, msg.value.decode('utf-8')))
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                success = False
            finally:
                loop.close()

            if success:
                # Commit if processing was successful
                self.consumer.commit()
            else:
                # Handle the failure case appropriately (e.g., log the error, retry, etc.)
                logger.info("Error processing message. Skipping commit and continuing...")

            # Resume fetching messages
            self.consumer.resume()

    def start(self):
        while True:
            try:
                self.initialize_kafka()
                self.consume_messages()
            except KeyboardInterrupt:
                logger.info("KafkaProcessor interrupted by user.")
                break
            except Exception as e:
                logger.error(f"Exception in KafkaProcessor: {e}")
                logger.info("Retrying connection in 30 seconds...")
                time.sleep(30)
            finally:
                try:
                    self.consumer.close()
                except Exception as e:
                    logger.error(f"Error closing Kafka consumer: {e}")


# Create a default instance of KafkaProcessor
kafka_processor = KafkaProcessor(
    kafka_address=settings().kafka.address,
    kafka_port=settings().kafka.port,
    input_topic='prompt_request',
    output_topic='prompt_response'
)
kafka_processor.start()