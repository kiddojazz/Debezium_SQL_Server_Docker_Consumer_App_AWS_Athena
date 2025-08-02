import pyodbc
import time
import random
import uuid
from datetime import datetime, timedelta
from faker import Faker
from colorama import Fore, Style, init
from config import Config
import logging

# Initialize colorama for colored output
init(autoreset=True)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        # logging.FileHandler('ticket_producer.log'),
        # logging.StreamHandler()
    ]
)

class TicketProducer:
    def __init__(self):
        self.config = Config()
        self.fake = Faker()
        self.connection = None
        self.cursor = None
        self.total_generated = 0
        
    def connect_to_database(self):
        """Establish connection to Azure SQL Database"""
        try:
            self.connection = pyodbc.connect(self.config.connection_string)
            self.cursor = self.connection.cursor()
            print(f"{Fore.GREEN}✓ Connected to Azure SQL Database successfully{Style.RESET_ALL}")
            logging.info("Connected to Azure SQL Database")
            return True
        except Exception as e:
            print(f"{Fore.RED}✗ Failed to connect to database: {str(e)}{Style.RESET_ALL}")
            logging.error(f"Database connection failed: {str(e)}")
            return False
    
    def create_schema_and_table(self):
        """Create schema and tickets table if they don't exist"""
        try:
            # Create schema
            schema_sql = f"""
            IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{self.config.SCHEMA_NAME}')
            BEGIN
                EXEC('CREATE SCHEMA {self.config.SCHEMA_NAME}')
            END
            """
            self.cursor.execute(schema_sql)
            self.connection.commit()
            
            # Create tickets table
            table_sql = f"""
            IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES 
                        WHERE TABLE_SCHEMA = '{self.config.SCHEMA_NAME}' 
                        AND TABLE_NAME = 'tickets')
            CREATE TABLE {self.config.SCHEMA_NAME}.tickets (
                id INT IDENTITY(1,1) PRIMARY KEY,
                ticket_number NVARCHAR(50) UNIQUE NOT NULL,
                customer_name NVARCHAR(255) NOT NULL,
                customer_email NVARCHAR(255) NOT NULL,
                customer_phone NVARCHAR(50),
                customer_type NVARCHAR(100),
                subject NVARCHAR(500) NOT NULL,
                description NVARCHAR(MAX),
                category NVARCHAR(100) NOT NULL,
                priority NVARCHAR(50) NOT NULL,
                status NVARCHAR(50) NOT NULL,
                source NVARCHAR(50) NOT NULL,
                assigned_agent NVARCHAR(255),
                created_at DATETIME2 DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME2 DEFAULT CURRENT_TIMESTAMP,
                resolved_at DATETIME2 NULL,
                closed_at DATETIME2 NULL,
                due_date DATETIME2 NULL,
                tags NVARCHAR(500),
                attachments_count INT DEFAULT 0,
                satisfaction_rating INT NULL,
                resolution_time_hours DECIMAL(10,2) NULL
            );
            """
            
            self.cursor.execute(table_sql)
            self.connection.commit()
            print(f"{Fore.GREEN}✓ Schema and tickets table verified/created successfully{Style.RESET_ALL}")
            logging.info("Schema and tickets table verified/created")
            
        except Exception as e:
            print(f"{Fore.RED}✗ Failed to create schema/table: {str(e)}{Style.RESET_ALL}")
            logging.error(f"Schema/table creation failed: {str(e)}")
            raise
    
    def get_max_ticket_id(self):
        """Get the maximum ticket ID to continue sequence"""
        try:
            sql = f"SELECT COALESCE(MAX(id), 0) FROM {self.config.SCHEMA_NAME}.tickets"
            self.cursor.execute(sql)
            result = self.cursor.fetchone()
            max_id = result[0] if result else 0
            print(f"{Fore.CYAN}Current max ticket ID: {max_id}{Style.RESET_ALL}")
            return max_id
        except Exception as e:
            print(f"{Fore.YELLOW}Warning: Could not get max ticket ID: {str(e)}{Style.RESET_ALL}")
            return 0
    
    def get_existing_ticket_numbers(self):
        """Get existing ticket numbers to avoid duplicates"""
        try:
            self.cursor.execute(f"SELECT ticket_number FROM {self.config.SCHEMA_NAME}.tickets")
            return {row[0] for row in self.cursor.fetchall()}
        except Exception as e:
            print(f"{Fore.YELLOW}Warning: Could not fetch existing ticket numbers: {str(e)}{Style.RESET_ALL}")
            return set()
    
    def generate_unique_ticket_number(self, existing_numbers):
        """Generate a unique ticket number"""
        max_attempts = 10
        for attempt in range(max_attempts):
            # Use timestamp and random number for uniqueness
            timestamp_part = int(time.time() * 1000) % 1000000
            random_part = random.randint(100, 999)
            ticket_number = f"TKT-{timestamp_part}{random_part}"
            
            if ticket_number not in existing_numbers:
                existing_numbers.add(ticket_number)
                return ticket_number
            
            time.sleep(0.001)  # Small delay for timestamp variation
        
        # Fallback to UUID if all attempts failed
        return f"TKT-{str(uuid.uuid4()).replace('-', '').upper()[:12]}"
    
    def calculate_resolution_time(self, created_at, resolved_at):
        """Calculate resolution time in hours"""
        if resolved_at and created_at:
            delta = resolved_at - created_at
            return round(delta.total_seconds() / 3600, 2)
        return None
    
    def generate_tickets(self, count):
        """Generate ticket data"""
        existing_ticket_numbers = self.get_existing_ticket_numbers()
        tickets = []
        
        for i in range(count):
            # Generate timestamps
            created_at = self.fake.date_time_between(start_date='-7d', end_date='now')
            
            # Determine ticket status and related timestamps
            status = random.choice(self.config.TICKET_STATUSES)
            resolved_at = None
            closed_at = None
            satisfaction_rating = None
            
            # Calculate resolved/closed times based on status
            if status in ['Resolved', 'Closed']:
                resolved_at = created_at + timedelta(
                    hours=random.randint(1, 72),
                    minutes=random.randint(0, 59)
                )
                if status == 'Closed':
                    closed_at = resolved_at + timedelta(
                        hours=random.randint(1, 24),
                        minutes=random.randint(0, 59)
                    )
                    # Add satisfaction rating for closed tickets
                    satisfaction_rating = random.randint(1, 5) if random.random() > 0.3 else None
            
            # Generate priority and due date
            priority = random.choice(self.config.PRIORITY_LEVELS)
            
            # Set due date based on priority
            priority_hours = {'Critical': 4, 'High': 8, 'Medium': 24, 'Low': 72, 'Informational': 168}
            due_hours = priority_hours.get(priority, 24)
            due_date = created_at + timedelta(hours=due_hours)
            
            # Calculate resolution time
            resolution_time = self.calculate_resolution_time(created_at, resolved_at)
            
            ticket = {
                'ticket_number': self.generate_unique_ticket_number(existing_ticket_numbers),
                'customer_name': self.fake.name(),
                'customer_email': self.fake.email(),
                'customer_phone': self.fake.phone_number(),
                'customer_type': random.choice(self.config.CUSTOMER_TYPES),
                'subject': self.fake.sentence(nb_words=random.randint(4, 8)),
                'description': self.fake.text(max_nb_chars=random.randint(100, 800)),
                'category': random.choice(self.config.TICKET_CATEGORIES),
                'priority': priority,
                'status': status,
                'source': random.choice(self.config.TICKET_SOURCES),
                'assigned_agent': self.fake.name() if random.random() > 0.2 else None,
                'created_at': created_at,
                'updated_at': datetime.now(),
                'resolved_at': resolved_at,
                'closed_at': closed_at,
                'due_date': due_date,
                'tags': ','.join(self.fake.words(nb=random.randint(1, 4))),
                'attachments_count': random.randint(0, 5) if random.random() > 0.7 else 0,
                'satisfaction_rating': satisfaction_rating,
                'resolution_time_hours': resolution_time
            }
            tickets.append(ticket)
        
        return tickets
    
    def insert_tickets(self, tickets):
        """Insert tickets into database"""
        if not tickets:
            return 0
        
        try:
            # Build INSERT statement
            columns = list(tickets[0].keys())
            placeholders = ', '.join(['?' for _ in columns])
            column_names = ', '.join(columns)
            
            sql = f"INSERT INTO {self.config.SCHEMA_NAME}.tickets ({column_names}) VALUES ({placeholders})"
            
            # Prepare data tuples
            data_tuples = []
            for ticket in tickets:
                values = [ticket[col] for col in columns]
                data_tuples.append(values)
            
            # Execute batch insert
            self.cursor.executemany(sql, data_tuples)
            self.connection.commit()
            
            self.total_generated += len(tickets)
            print(f"{Fore.GREEN}✓ Inserted {len(tickets)} tickets (Total: {self.total_generated}){Style.RESET_ALL}")
            logging.info(f"Inserted {len(tickets)} tickets (Total: {self.total_generated})")
            return len(tickets)
            
        except Exception as e:
            print(f"{Fore.RED}✗ Failed to insert tickets: {str(e)}{Style.RESET_ALL}")
            logging.error(f"Failed to insert tickets: {str(e)}")
            self.connection.rollback()
            return 0
    
    def get_ticket_count(self):
        """Get current ticket count"""
        try:
            self.cursor.execute(f"SELECT COUNT(*) FROM {self.config.SCHEMA_NAME}.tickets")
            count = self.cursor.fetchone()[0]
            return count
        except Exception as e:
            print(f"{Fore.YELLOW}Warning: Could not get ticket count: {str(e)}{Style.RESET_ALL}")
            return 0
    
    def run_producer(self):
        """Main producer loop with specified timing"""
        if not self.connect_to_database():
            return
        
        try:
            self.create_schema_and_table()
            max_id = self.get_max_ticket_id()
            
            print(f"{Fore.MAGENTA}🎫 Starting Ticket Data Producer{Style.RESET_ALL}")
            print(f"{Fore.CYAN}Configuration:{Style.RESET_ALL}")
            print(f"  - Batch size: {self.config.BATCH_SIZE} tickets")
            print(f"  - Generation interval: {self.config.GENERATION_INTERVAL} seconds")
            print(f"  - Generation duration: {self.config.GENERATION_DURATION} seconds ({self.config.GENERATION_DURATION//60} minutes)")
            print(f"  - Pause duration: {self.config.PAUSE_DURATION} seconds ({self.config.PAUSE_DURATION//60} minutes)")
            
            cycle = 1
            while self.total_generated < self.config.MAX_RECORDS_PER_RUN:
                print(f"\n{Fore.MAGENTA}=== Cycle {cycle} Started ==={Style.RESET_ALL}")
                print(f"{Fore.CYAN}Current ticket count in database: {self.get_ticket_count():,}{Style.RESET_ALL}")
                
                # Generation phase (10 minutes)
                generation_start = time.time()
                generation_end = generation_start + self.config.GENERATION_DURATION
                batch_count = 0
                
                while time.time() < generation_end and self.total_generated < self.config.MAX_RECORDS_PER_RUN:
                    batch_start = time.time()
                    
                    # Generate and insert tickets
                    print(f"\n{Fore.YELLOW}Generating batch {batch_count + 1} ({self.config.BATCH_SIZE} tickets)...{Style.RESET_ALL}")
                    tickets = self.generate_tickets(self.config.BATCH_SIZE)
                    
                    if tickets:
                        inserted = self.insert_tickets(tickets)
                        if inserted > 0:
                            batch_count += 1
                    
                    # Check if we have time for another batch before sleeping
                    current_time = time.time()
                    if current_time >= generation_end:
                        print(f"{Fore.YELLOW}Generation time limit reached, ending cycle{Style.RESET_ALL}")
                        break
                    
                    # Wait for next interval (1 minute) - but only if we have enough time left
                    elapsed = current_time - batch_start
                    sleep_time = max(0, self.config.GENERATION_INTERVAL - elapsed)
                    
                    if sleep_time > 0:
                        # Check if sleeping would take us past the generation window
                        if current_time + sleep_time >= generation_end:
                            remaining_time = generation_end - current_time
                            print(f"{Fore.YELLOW}Only {remaining_time:.1f} seconds left in generation window, ending cycle{Style.RESET_ALL}")
                            break
                        else:
                            print(f"{Fore.CYAN}Waiting {sleep_time:.1f} seconds until next batch...{Style.RESET_ALL}")
                            time.sleep(sleep_time)
                
                print(f"\n{Fore.MAGENTA}=== Cycle {cycle} Completed - Generated {batch_count} batches ==={Style.RESET_ALL}")
                
                # Check if we've reached the limit
                if self.total_generated >= self.config.MAX_RECORDS_PER_RUN:
                    print(f"{Fore.GREEN}✓ Reached maximum records limit ({self.config.MAX_RECORDS_PER_RUN}){Style.RESET_ALL}")
                    break
                
                # Pause phase (5 minutes)
                print(f"{Fore.CYAN}Pausing for {self.config.PAUSE_DURATION} seconds ({self.config.PAUSE_DURATION//60} minutes)...{Style.RESET_ALL}")
                time.sleep(self.config.PAUSE_DURATION)
                
                cycle += 1
                
        except KeyboardInterrupt:
            print(f"\n{Fore.YELLOW}Producer stopped by user{Style.RESET_ALL}")
            logging.info("Producer stopped by user")
        except Exception as e:
            print(f"\n{Fore.RED}Producer failed with error: {str(e)}{Style.RESET_ALL}")
            logging.error(f"Producer failed: {str(e)}")
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Clean up database connections"""
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
            print(f"{Fore.GREEN}✓ Database connections closed{Style.RESET_ALL}")
            print(f"{Fore.MAGENTA}Total tickets generated: {self.total_generated}{Style.RESET_ALL}")
        except Exception as e:
            print(f"{Fore.YELLOW}Warning during cleanup: {str(e)}{Style.RESET_ALL}")


def main():
    """Main function to run the ticket producer"""
    producer = TicketProducer()
    
    print(f"{Fore.MAGENTA}{'='*50}{Style.RESET_ALL}")
    print(f"{Fore.MAGENTA}    TICKET DATA PRODUCER{Style.RESET_ALL}")
    print(f"{Fore.MAGENTA}{'='*50}{Style.RESET_ALL}")
    
    try:
        producer.run_producer()
        
    except Exception as e:
        print(f"{Fore.RED}Fatal error: {str(e)}{Style.RESET_ALL}")
        logging.error(f"Fatal error: {str(e)}")
    finally:
        producer.cleanup()


if __name__ == "__main__":
    main()