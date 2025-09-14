import os
import json
import logging
import asyncio
from datetime import datetime
from typing import Dict, Any, Optional, List
from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaError
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData
import base64

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class KafkaToEventHubConsumer:
    """Pure Python Kafka consumer that forwards messages to Azure Event Hub"""
    
    def __init__(self):
        # Kafka configuration
        self.kafka_config = {
            'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
            'group.id': os.getenv('KAFKA_CONSUMER_GROUP_ID_FABRIC', 'eventhub-consumer-group'),
            'auto.offset.reset': os.getenv('AUTO_OFFSET_RESET', 'earliest'),
            'enable.auto.commit': False,
            'auto.commit.interval.ms': 5000,
            'session.timeout.ms': 30000,
            'heartbeat.interval.ms': 10000,
            'api.version.request': True,
            'broker.version.fallback': '2.8.0'
        }
        
        # Event Hub configuration
        self.eventhub_namespace_endpoint = os.getenv('EVENTHUB_NAMESPACE_ENDPOINT')
        self.eventhub_name = os.getenv('EVENTHUB_NAME')
        self.eventhub_connection_string = os.getenv('EVENTHUB_CONNECTION_STRING')
        
        # Validate Event Hub configuration
        if not self.eventhub_connection_string and not (self.eventhub_namespace_endpoint and self.eventhub_name):
            raise ValueError("Either EVENTHUB_CONNECTION_STRING or both EVENTHUB_NAMESPACE_ENDPOINT and EVENTHUB_NAME must be provided")
        
        # Processing configuration
        self.batch_size = int(os.getenv('BATCH_SIZE', 10))  # Event Hub can handle larger batches efficiently
        self.topic = os.getenv('KAFKA_TOPIC', 'logis-db.TicketingDB.logis.tickets')
        self.flush_interval_seconds = int(os.getenv('FLUSH_INTERVAL_SECONDS', 30))  # 30 seconds for near real-time
        
        # Initialize Kafka consumer
        self.consumer = Consumer(self.kafka_config)
        
        # Event Hub producer will be initialized in async context
        self.eventhub_producer = None
        
        # Batch storage
        self.records_batch = []
        self.last_flush_time = datetime.utcnow()
        
        logger.info("Kafka to Event Hub consumer initialized successfully")
    
    async def _initialize_eventhub_producer(self):
        """Initialize the Event Hub producer client"""
        try:
            if self.eventhub_connection_string:
                # Use connection string if provided
                self.eventhub_producer = EventHubProducerClient.from_connection_string(
                    conn_str=self.eventhub_connection_string,
                    eventhub_name=self.eventhub_name if self.eventhub_name else None
                )
            else:
                # Use namespace endpoint and Event Hub name
                # Note: This would require additional authentication setup (like DefaultAzureCredential)
                # For now, we'll focus on connection string approach
                raise ValueError("Connection string authentication is required. Please provide EVENTHUB_CONNECTION_STRING")
            
            logger.info(f"Event Hub producer initialized for: {self.eventhub_name}")
            
        except Exception as e:
            logger.error(f"Failed to initialize Event Hub producer: {str(e)}")
            raise
    
    def _parse_debezium_message(self, message_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Parse Debezium CDC message and extract ticket data"""
        try:
            payload = message_data.get('payload', {})
            operation = payload.get('op')  # 'c' = create, 'u' = update, 'd' = delete, 'r' = read
            source_info = payload.get('source', {})
            
            # Determine which record to use based on operation
            if operation == 'd':  # Delete operation
                record_data = payload.get('before', {})
                if not record_data:
                    logger.warning("Delete operation but no 'before' data found")
                    return None
                operation_type = 'DELETE'
                deleted = True
            else:  # Create, Update, or Read operations
                record_data = payload.get('after', {})
                if not record_data:
                    logger.warning(f"Operation '{operation}' but no 'after' data found")
                    return None
                operation_type = 'INSERT' if operation == 'c' or operation == 'r' else 'UPDATE'
                deleted = False
            
            # Parse the ticket fields according to your schema
            parsed_data = {
                'id': record_data.get('id'),
                'ticket_number': record_data.get('ticket_number'),
                'customer_name': record_data.get('customer_name'),
                'customer_email': record_data.get('customer_email'),
                'customer_phone': record_data.get('customer_phone'),
                'customer_type': record_data.get('customer_type'),
                'subject': record_data.get('subject'),
                'description': record_data.get('description'),
                'category': record_data.get('category'),
                'priority': record_data.get('priority'),
                'status': record_data.get('status'),
                'source': record_data.get('source'),
                'assigned_agent': record_data.get('assigned_agent'),
                'created_at': self._convert_debezium_timestamp(record_data.get('created_at')),
                'updated_at': self._convert_debezium_timestamp(record_data.get('updated_at')),
                'resolved_at': self._convert_debezium_timestamp(record_data.get('resolved_at')),
                'closed_at': self._convert_debezium_timestamp(record_data.get('closed_at')),
                'due_date': self._convert_debezium_timestamp(record_data.get('due_date')),
                'tags': record_data.get('tags'),
                'attachments_count': record_data.get('attachments_count', 0),
                'satisfaction_rating': record_data.get('satisfaction_rating'),
                'resolution_time_hours': self._decode_decimal(record_data.get('resolution_time_hours')),
                'deleted': deleted,
                'operation_type': operation_type,
                'cdc_operation': operation,
                'cdc_timestamp': payload.get('ts_ms'),
                'cdc_lsn': source_info.get('commit_lsn'),
                'processed_at': datetime.utcnow().isoformat(),
                # Add Event Hub specific metadata
                'source_kafka_topic': self.topic,
                'source_kafka_partition': None,  # Will be set when processing
                'source_kafka_offset': None      # Will be set when processing
            }
            
            # Validate required fields
            if not parsed_data['id']:
                logger.warning("Message missing required 'id' field")
                return None
                
            return parsed_data
            
        except Exception as e:
            logger.error(f"Failed to parse Debezium message: {str(e)}")
            return None
    
    def _convert_debezium_timestamp(self, timestamp_ns: Optional[int]) -> Optional[str]:
        """Convert Debezium nanosecond timestamp to ISO format"""
        if timestamp_ns is None:
            return None
            
        try:
            # Debezium timestamps are in nanoseconds since epoch
            timestamp_seconds = timestamp_ns / 1_000_000_000
            dt = datetime.fromtimestamp(timestamp_seconds)
            return dt.isoformat()
            
        except Exception as e:
            logger.warning(f"Failed to convert timestamp {timestamp_ns}: {str(e)}")
            return None
    
    def _decode_decimal(self, decimal_bytes: Optional[str]) -> Optional[float]:
        """Decode Kafka Connect decimal field"""
        if decimal_bytes is None:
            return None
        try:
            # Handle different decimal encoding formats
            if isinstance(decimal_bytes, (int, float)):
                return float(decimal_bytes)
            
            if isinstance(decimal_bytes, str):
                try:
                    # Try parsing as regular number first
                    return float(decimal_bytes)
                except ValueError:
                    # Try base64 decoding if it's encoded
                    try:
                        decoded = base64.b64decode(decimal_bytes)
                        # This is a simplified decoder - adjust based on your actual data format
                        # For proper decimal decoding, you'd need the scale and precision info
                        return float(len(decoded))  # Placeholder implementation
                    except Exception:
                        return None
            
            return None
        except Exception as e:
            logger.warning(f"Failed to decode decimal {decimal_bytes}: {str(e)}")
            return None
    
    async def _send_batch_to_eventhub(self, force_flush: bool = False):
        """Send batch of records to Azure Event Hub"""
        if not self.records_batch:
            return
            
        # Check if we should flush based on batch size or time interval
        current_time = datetime.utcnow()
        time_since_last_flush = (current_time - self.last_flush_time).total_seconds()
        
        should_flush = (
            len(self.records_batch) >= self.batch_size or
            time_since_last_flush >= self.flush_interval_seconds or
            force_flush
        )
        
        if not should_flush:
            return
            
        try:
            # Prepare Event Hub event batch
            event_data_batch = []
            
            for record in self.records_batch:
                # Convert record to JSON string
                event_json = json.dumps(record, ensure_ascii=False)
                
                # Create EventData object
                event_data = EventData(event_json)
                
                # Add custom properties for better routing and filtering
                event_data.properties = {
                    'operation_type': record.get('operation_type', 'UNKNOWN'),
                    'ticket_id': str(record.get('id', '')),
                    'ticket_status': record.get('status', ''),
                    'ticket_priority': record.get('priority', ''),
                    'cdc_operation': record.get('cdc_operation', ''),
                    'processed_timestamp': record.get('processed_at', ''),
                    'source_system': 'kafka-cdc-consumer'
                }
                
                # Add partition key for better distribution (optional)
                # You can customize this based on your Event Hub partition strategy
                partition_key = f"ticket_{record.get('id', 'unknown')}"
                event_data.properties['partition_key'] = partition_key
                
                event_data_batch.append(event_data)
            
            # Send batch to Event Hub
            async with self.eventhub_producer:
                await self.eventhub_producer.send_batch(event_data_batch)
            
            logger.info(f"Successfully sent {len(self.records_batch)} records to Event Hub '{self.eventhub_name}'")
            
            # Clear batch and update flush time
            self.records_batch = []
            self.last_flush_time = current_time
            
        except Exception as e:
            logger.error(f"Failed to send batch to Event Hub: {str(e)}")
            # Don't clear the batch on failure - we'll retry on next flush
            raise
    
    def _should_process_message(self, parsed_data: Dict[str, Any]) -> bool:
        """Apply any filtering logic here"""
        # Add any business logic filters here
        # For example, skip deleted records if you don't want them in Event Hub
        # if parsed_data.get('deleted', False):
        #     return False
        
        return True
    
    async def consume_messages(self, timeout_seconds: int = None):
        """Main method to consume messages from Kafka and send to Event Hub"""
        # Initialize Event Hub producer
        await self._initialize_eventhub_producer()
        
        # Subscribe to topic
        self.consumer.subscribe([self.topic])
        logger.info(f"Subscribed to topic: {self.topic}")
        
        messages_processed = 0
        start_time = datetime.utcnow()
        
        try:
            logger.info("Starting message consumption from Kafka...")
            
            while True:
                try:
                    msg = self.consumer.poll(timeout=10.0)
                    
                    if msg is None:
                        # Check for timeout
                        if timeout_seconds:
                            elapsed_time = (datetime.utcnow() - start_time).total_seconds()
                            if elapsed_time > timeout_seconds and messages_processed == 0:
                                logger.warning(f"No messages received within {timeout_seconds} seconds timeout")
                                break
                        
                        # Try to flush any pending records based on time interval
                        await self._send_batch_to_eventhub()
                        continue
                    
                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            logger.info(f"End of partition reached {msg.topic()}[{msg.partition()}] at offset {msg.offset()}")
                        else:
                            logger.error(f"Consumer error: {msg.error()}")
                        continue
                    
                    # Process message
                    logger.debug(f"Received message from {msg.topic()}[{msg.partition()}] at offset {msg.offset()}")
                    
                    try:
                        # Parse the JSON message
                        message_data = json.loads(msg.value().decode('utf-8'))
                        
                        # Parse the Debezium CDC message
                        parsed_data = self._parse_debezium_message(message_data)
                        
                        if parsed_data and self._should_process_message(parsed_data):
                            # Add Kafka metadata
                            parsed_data['source_kafka_partition'] = msg.partition()
                            parsed_data['source_kafka_offset'] = msg.offset()
                            
                            self.records_batch.append(parsed_data)
                            messages_processed += 1
                            
                            logger.info(f"Processed {parsed_data['operation_type']} operation for ticket ID: {parsed_data.get('id')} (batch size: {len(self.records_batch)})")
                            
                            # Try to send batch if conditions are met
                            await self._send_batch_to_eventhub()
                        else:
                            logger.debug("Message filtered out or failed to parse")
                            
                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to parse JSON message: {str(e)}")
                        continue
                    except Exception as e:
                        logger.error(f"Error processing individual message: {str(e)}")
                        continue
                
                except Exception as e:
                    logger.error(f"Error in main processing loop: {str(e)}")
                    continue
        
        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user")
        
        finally:
            # Flush any remaining records
            if self.records_batch:
                logger.info(f"Flushing final batch of {len(self.records_batch)} records")
                await self._send_batch_to_eventhub(force_flush=True)
            
            # Clean up
            if self.eventhub_producer:
                await self.eventhub_producer.close()
            
            if self.consumer:
                self.consumer.close()
            
            logger.info(f"Consumer shutdown complete. Total messages processed: {messages_processed}")

async def main():
    """Main function to run the consumer"""
    try:
        # Validate required environment variables
        required_vars = [
            'KAFKA_BOOTSTRAP_SERVERS',
            'KAFKA_TOPIC'
        ]
        
        # Check Event Hub configuration
        if not os.getenv('EVENTHUB_CONNECTION_STRING'):
            if not (os.getenv('EVENTHUB_NAMESPACE_ENDPOINT') and os.getenv('EVENTHUB_NAME')):
                required_vars.extend(['EVENTHUB_CONNECTION_STRING'])
        
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {missing_vars}")
        
        # Log configuration
        logger.info("Configuration:")
        logger.info(f"  Kafka Bootstrap Servers: {os.getenv('KAFKA_BOOTSTRAP_SERVERS')}")
        logger.info(f"  Kafka Topic: {os.getenv('KAFKA_TOPIC')}")
        logger.info(f"  Consumer Group ID: {os.getenv('KAFKA_CONSUMER_GROUP_ID_FABRIC', 'eventhub-consumer-group')}")
        logger.info(f"  Event Hub Name: {os.getenv('EVENTHUB_NAME')}")
        logger.info(f"  Event Hub Namespace: {os.getenv('EVENTHUB_NAMESPACE_ENDPOINT', 'Using connection string')}")
        logger.info(f"  Batch Size: {os.getenv('BATCH_SIZE', 10)}")
        logger.info(f"  Flush Interval: {os.getenv('FLUSH_INTERVAL_SECONDS', 30)} seconds")
        
        consumer = KafkaToEventHubConsumer()
        
        # Get timeout from environment (optional)
        timeout_seconds = None
        if os.getenv('CONSUMER_TIMEOUT_SECONDS'):
            timeout_seconds = int(os.getenv('CONSUMER_TIMEOUT_SECONDS'))
        
        await consumer.consume_messages(timeout_seconds=timeout_seconds)
        
    except Exception as e:
        logger.error(f"Consumer failed with error: {str(e)}")
        raise

if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())