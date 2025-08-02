import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class Config:
    # Azure SQL Database settings
    AZURE_SQL_SERVER = os.getenv('AZURE_SQL_SERVER')
    AZURE_SQL_DATABASE = os.getenv('AZURE_SQL_DATABASE')
    AZURE_SQL_USERNAME = os.getenv('AZURE_SQL_USERNAME')
    AZURE_SQL_PASSWORD = os.getenv('AZURE_SQL_PASSWORD')
    AZURE_SQL_DRIVER = os.getenv('AZURE_SQL_DRIVER', 'ODBC Driver 18 for SQL Server')
    
    # Schema and table settings
    SCHEMA_NAME = os.getenv('SCHEMA_NAME', 'logis')
    
    # Data generation settings
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', 2))
    MAX_RECORDS_PER_RUN = int(os.getenv('MAX_RECORDS_PER_RUN', 1000))
    
    # Timing settings
    GENERATION_INTERVAL = int(os.getenv('GENERATION_INTERVAL', 60))  # 1 minute
    GENERATION_DURATION = int(os.getenv('GENERATION_DURATION', 600))  # 10 minutes
    PAUSE_DURATION = int(os.getenv('PAUSE_DURATION', 300))  # 5 minutes
    
    # Connection string
    @property
    def connection_string(self):
        return (
            f"DRIVER={{{self.AZURE_SQL_DRIVER}}};"
            f"SERVER={self.AZURE_SQL_SERVER};"
            f"DATABASE={self.AZURE_SQL_DATABASE};"
            f"UID={self.AZURE_SQL_USERNAME};"
            f"PWD={self.AZURE_SQL_PASSWORD};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=yes;"
            f"Connection Timeout=30;"
        )
    
    # Ticket status options
    TICKET_STATUSES = [
        'Open',
        'In Progress', 
        'Waiting for Customer',
        'Waiting for Third Party',
        'Resolved',
        'Closed',
        'Cancelled'
    ]
    
    # Priority levels
    PRIORITY_LEVELS = [
        'Critical',
        'High',
        'Medium', 
        'Low',
        'Informational'
    ]
    
    # Sample categories for tickets
    TICKET_CATEGORIES = [
        'Technical Support',
        'Billing',
        'Account Management',
        'Feature Request',
        'Bug Report',
        'General Inquiry',
        'Hardware Issue',
        'Software Issue',
        'Network Issue',
        'Security Issue',
        'Data Recovery',
        'Training',
        'Consultation',
        'Integration Support',
        'API Support',
        'Performance Issue',
        'Maintenance',
        'Upgrade Request',
        'Downgrade Request',
        'Cancellation Request'
    ]
    
    # Ticket sources
    TICKET_SOURCES = [
        'Web Portal',
        'Email',
        'Phone',
        'Chat',
        'API',
        'Mobile App',
        'Social Media'
    ]
    
    # Customer types
    CUSTOMER_TYPES = [
        'Enterprise',
        'SMB',
        'Individual',
        'Government',
        'Education',
        'Healthcare'
    ]