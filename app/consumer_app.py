import os
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional, List
from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaError
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from io import BytesIO
import base64

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class KafkaToS3ParquetConsumer:
    """Pure Python Kafka consumer that writes Parquet files to S3"""
    
    def __init__(self):
        # Kafka configuration
        self.kafka_config = {
            'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
            'group.id': os.getenv('KAFKA_CONSUMER_GROUP_ID', 's3-consumer-group'),
            'auto.offset.reset': os.getenv('AUTO_OFFSET_RESET', 'earliest'),
            'enable.auto.commit': False,
            'auto.commit.interval.ms': 5000,
            'session.timeout.ms': 30000,
            'heartbeat.interval.ms': 10000,
            'api.version.request': True,
            'broker.version.fallback': '2.8.0'
        }
        
        # S3 configuration
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION', 'eu-west-2')
        )
        
        self.s3_bucket = os.getenv('S3_BUCKET')
        self.s3_prefix = os.getenv('S3_PREFIX', 'tickets')
        
        # Processing configuration
        self.batch_size = int(os.getenv('BATCH_SIZE', 2))  # Ultra-low latency: only 2 records per batch
        self.topic = os.getenv('KAFKA_TOPIC', 'logis-db.TicketingDB.logis.tickets')
        self.flush_interval_seconds = int(os.getenv('FLUSH_INTERVAL_SECONDS', 30))  # 30 seconds for near real-time
        
        # Initialize consumer
        self.consumer = Consumer(self.kafka_config)
        
        # Batch storage
        self.records_batch = []
        self.last_flush_time = datetime.utcnow()
        
        logger.info("Kafka to S3 Parquet consumer initialized successfully")
    
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
                'processed_at': datetime.utcnow().isoformat()
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
    
    def _create_parquet_schema(self) -> pa.Schema:
        """Define the Parquet schema for tickets"""
        return pa.schema([
            pa.field('id', pa.int64()),
            pa.field('ticket_number', pa.string()),
            pa.field('customer_name', pa.string()),
            pa.field('customer_email', pa.string()),
            pa.field('customer_phone', pa.string()),
            pa.field('customer_type', pa.string()),
            pa.field('subject', pa.string()),
            pa.field('description', pa.string()),
            pa.field('category', pa.string()),
            pa.field('priority', pa.string()),
            pa.field('status', pa.string()),
            pa.field('source', pa.string()),
            pa.field('assigned_agent', pa.string()),
            pa.field('created_at', pa.string()),
            pa.field('updated_at', pa.string()),
            pa.field('resolved_at', pa.string()),
            pa.field('closed_at', pa.string()),
            pa.field('due_date', pa.string()),
            pa.field('tags', pa.string()),
            pa.field('attachments_count', pa.int32()),
            pa.field('satisfaction_rating', pa.int32()),
            pa.field('resolution_time_hours', pa.float64()),
            pa.field('deleted', pa.bool_()),
            pa.field('operation_type', pa.string()),
            pa.field('cdc_operation', pa.string()),
            pa.field('cdc_timestamp', pa.int64()),
            pa.field('cdc_lsn', pa.string()),
            pa.field('processed_at', pa.string())
        ])
    
    def _write_batch_to_s3(self, force_flush: bool = False):
        """Write batch of records to S3 as Parquet file"""
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
            # Prepare data for PyArrow table
            df_data = {}
            schema = self._create_parquet_schema()
            
            for field in schema:
                field_name = field.name
                df_data[field_name] = [r.get(field_name) for r in self.records_batch]
            
            # Create PyArrow table with explicit schema
            table = pa.table(df_data, schema=schema)
            
            # Generate S3 key with partitioning
            s3_key = f"{self.s3_prefix}/year={current_time.year}/month={current_time.month:02d}/day={current_time.day:02d}/hour={current_time.hour:02d}/tickets_{current_time.strftime('%Y%m%d_%H%M%S')}_{len(self.records_batch)}.parquet"
            
            # Write to memory buffer
            parquet_buffer = BytesIO()
            pq.write_table(
                table, 
                parquet_buffer,
                compression='snappy',  # Good compression with fast decompression
                use_dictionary=True,   # Better compression for repeated values
                write_statistics=True  # Enable column statistics for query optimization
            )
            parquet_buffer.seek(0)
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=s3_key,
                Body=parquet_buffer.getvalue(),
                ContentType='application/octet-stream',
                ServerSideEncryption='AES256'  # Enable server-side encryption
            )
            
            logger.info(f"Successfully uploaded {len(self.records_batch)} records to s3://{self.s3_bucket}/{s3_key}")
            
            # Clear batch and update flush time
            self.records_batch = []
            self.last_flush_time = current_time
            
        except Exception as e:
            logger.error(f"Failed to write batch to S3: {str(e)}")
            # Don't clear the batch on failure - we'll retry on next flush
            raise
    
    def _should_process_message(self, parsed_data: Dict[str, Any]) -> bool:
        """Apply any filtering logic here"""
        # Add any business logic filters here
        # For example, skip deleted records if you don't want them in S3
        # if parsed_data.get('deleted', False):
        #     return False
        
        return True
    
    def consume_messages(self, timeout_seconds: int = None):
        """Main method to consume messages from Kafka and write to S3"""
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
                        self._write_batch_to_s3()
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
                            self.records_batch.append(parsed_data)
                            messages_processed += 1
                            
                            logger.info(f"Processed {parsed_data['operation_type']} operation for ticket ID: {parsed_data.get('id')} (batch size: {len(self.records_batch)})")
                            
                            # Try to write batch if conditions are met
                            self._write_batch_to_s3()
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
                self._write_batch_to_s3(force_flush=True)
            
            # Clean up
            if self.consumer:
                self.consumer.close()
            
            logger.info(f"Consumer shutdown complete. Total messages processed: {messages_processed}")

def main():
    """Main function to run the consumer"""
    try:
        # Validate required environment variables
        required_vars = [
            'KAFKA_BOOTSTRAP_SERVERS',
            'KAFKA_TOPIC', 
            'AWS_ACCESS_KEY_ID',
            'AWS_SECRET_ACCESS_KEY',
            'S3_BUCKET'
        ]
        
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {missing_vars}")
        
        # Log configuration
        logger.info("Configuration:")
        logger.info(f"  Kafka Bootstrap Servers: {os.getenv('KAFKA_BOOTSTRAP_SERVERS')}")
        logger.info(f"  Kafka Topic: {os.getenv('KAFKA_TOPIC')}")
        logger.info(f"  Consumer Group ID: {os.getenv('KAFKA_CONSUMER_GROUP_ID', 's3-consumer-group')}")
        logger.info(f"  S3 Bucket: {os.getenv('S3_BUCKET')}")
        logger.info(f"  S3 Prefix: {os.getenv('S3_PREFIX', 'tickets')}")
        logger.info(f"  Batch Size: {os.getenv('BATCH_SIZE', 2)}")
        logger.info(f"  Flush Interval: {os.getenv('FLUSH_INTERVAL_SECONDS', 30)} seconds")
        
        consumer = KafkaToS3ParquetConsumer()
        
        # Get timeout from environment (optional)
        timeout_seconds = None
        if os.getenv('CONSUMER_TIMEOUT_SECONDS'):
            timeout_seconds = int(os.getenv('CONSUMER_TIMEOUT_SECONDS'))
        
        consumer.consume_messages(timeout_seconds=timeout_seconds)
        
    except Exception as e:
        logger.error(f"Consumer failed with error: {str(e)}")
        raise

if __name__ == "__main__":
    main()