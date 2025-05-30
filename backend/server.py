from fastapi import FastAPI, APIRouter, HTTPException, UploadFile, File
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
from starlette.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
import os
import logging
from pathlib import Path
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
import uuid
from datetime import datetime, date
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import json
from faker import Faker
import random
from decimal import Decimal
import asyncio
import io

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

# MongoDB connection (for metadata storage)
mongo_url = os.environ['MONGO_URL']
client = AsyncIOMotorClient(mongo_url)
db = client[os.environ['DB_NAME']]

# Create the main app without a prefix
app = FastAPI(title="Financial Data Warehouse API", version="1.0.0")

# Create a router with the /api prefix
api_router = APIRouter(prefix="/api")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Pydantic Models
class SnowflakeConnection(BaseModel):
    account: str
    username: str
    password: str
    warehouse: str = "COMPUTE_WH"
    database: str = "FINANCIAL_DW"
    schema: str = "PUBLIC"

class TransactionRecord(BaseModel):
    transaction_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    customer_id: str
    merchant_id: str
    account_id: str
    amount: float
    transaction_type: str
    category: str
    description: str
    transaction_date: datetime
    status: str = "completed"
    created_at: datetime = Field(default_factory=datetime.utcnow)

class Customer(BaseModel):
    customer_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    first_name: str
    last_name: str
    email: str
    phone: str
    address: str
    city: str
    state: str
    zip_code: str
    date_of_birth: date
    account_opening_date: date
    risk_score: int = Field(default=50, ge=0, le=100)

class Merchant(BaseModel):
    merchant_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    merchant_name: str
    merchant_category: str
    address: str
    city: str
    state: str
    zip_code: str
    phone: str
    email: str

class Account(BaseModel):
    account_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    customer_id: str
    account_type: str
    account_number: str
    balance: float
    opening_date: date
    status: str = "active"

class ETLJob(BaseModel):
    job_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    job_name: str
    status: str = "pending"
    records_processed: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    error_message: Optional[str] = None

class QueryRequest(BaseModel):
    sql_query: str
    limit: Optional[int] = 1000

class DataGenerationRequest(BaseModel):
    customers: int = 100
    merchants: int = 50
    transactions: int = 1000

# Snowflake Connection Management
class SnowflakeManager:
    def __init__(self):
        self.connection = None
        self.connection_params = None
    
    def connect(self, params: SnowflakeConnection):
        """Establish connection to Snowflake"""
        try:
            self.connection_params = params
            self.connection = snowflake.connector.connect(
                account=params.account,
                user=params.username,
                password=params.password,
                warehouse=params.warehouse,
                database=params.database,
                schema=params.schema
            )
            logger.info("Successfully connected to Snowflake")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {str(e)}")
            raise HTTPException(status_code=400, detail=f"Snowflake connection failed: {str(e)}")
    
    def execute_query(self, query: str, fetch_results: bool = True):
        """Execute SQL query on Snowflake"""
        if not self.connection:
            raise HTTPException(status_code=400, detail="No active Snowflake connection")
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            
            if fetch_results:
                results = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description] if cursor.description else []
                return {"columns": columns, "data": results}
            else:
                return {"message": "Query executed successfully"}
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            raise HTTPException(status_code=400, detail=f"Query failed: {str(e)}")
    
    def load_dataframe(self, df: pd.DataFrame, table_name: str, if_exists: str = "append"):
        """Load pandas DataFrame to Snowflake table"""
        if not self.connection:
            raise HTTPException(status_code=400, detail="No active Snowflake connection")
        
        try:
            success, nchunks, nrows, _ = write_pandas(
                self.connection, 
                df, 
                table_name,
                if_exists=if_exists,
                auto_create_table=True
            )
            return {"success": success, "chunks": nchunks, "rows": nrows}
        except Exception as e:
            logger.error(f"DataFrame load failed: {str(e)}")
            raise HTTPException(status_code=400, detail=f"Data load failed: {str(e)}")

# Global Snowflake manager instance
snowflake_manager = SnowflakeManager()

# Data Generation Functions
def generate_sample_data(num_customers: int = 100, num_merchants: int = 50, num_transactions: int = 1000):
    """Generate sample financial data using Faker"""
    fake = Faker()
    
    # Generate Customers
    customers = []
    for _ in range(num_customers):
        customer = {
            'customer_id': str(uuid.uuid4()),
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'email': fake.email(),
            'phone': fake.phone_number(),
            'address': fake.street_address(),
            'city': fake.city(),
            'state': fake.state_abbr(),
            'zip_code': fake.zipcode(),
            'date_of_birth': fake.date_of_birth(minimum_age=18, maximum_age=80),
            'account_opening_date': fake.date_between(start_date='-5y', end_date='today'),
            'risk_score': random.randint(10, 90)
        }
        customers.append(customer)
    
    # Generate Merchants
    merchants = []
    merchant_categories = ['Restaurant', 'Grocery', 'Gas Station', 'Retail', 'Online', 'Pharmacy', 'Entertainment']
    for _ in range(num_merchants):
        merchant = {
            'merchant_id': str(uuid.uuid4()),
            'merchant_name': fake.company(),
            'merchant_category': random.choice(merchant_categories),
            'address': fake.street_address(),
            'city': fake.city(),
            'state': fake.state_abbr(),
            'zip_code': fake.zipcode(),
            'phone': fake.phone_number(),
            'email': fake.company_email()
        }
        merchants.append(merchant)
    
    # Generate Accounts
    accounts = []
    account_types = ['checking', 'savings', 'credit']
    for customer in customers:
        # Each customer gets 1-3 accounts
        num_accounts = random.randint(1, 3)
        for _ in range(num_accounts):
            account = {
                'account_id': str(uuid.uuid4()),
                'customer_id': customer['customer_id'],
                'account_type': random.choice(account_types),
                'account_number': fake.bban(),
                'balance': round(random.uniform(100, 50000), 2),
                'opening_date': customer['account_opening_date'],
                'status': 'active'
            }
            accounts.append(account)
    
    # Generate Transactions
    transactions = []
    transaction_types = ['purchase', 'withdrawal', 'deposit', 'transfer', 'payment']
    categories = ['Food & Dining', 'Shopping', 'Transportation', 'Bills', 'Entertainment', 'Healthcare', 'Travel']
    
    for _ in range(num_transactions):
        customer = random.choice(customers)
        merchant = random.choice(merchants)
        customer_accounts = [acc for acc in accounts if acc['customer_id'] == customer['customer_id']]
        account = random.choice(customer_accounts) if customer_accounts else accounts[0]
        
        transaction = {
            'transaction_id': str(uuid.uuid4()),
            'customer_id': customer['customer_id'],
            'merchant_id': merchant['merchant_id'],
            'account_id': account['account_id'],
            'amount': round(random.uniform(5, 2000), 2),
            'transaction_type': random.choice(transaction_types),
            'category': random.choice(categories),
            'description': f"{merchant['merchant_name']} - {fake.text(max_nb_chars=50)}",
            'transaction_date': fake.date_time_between(start_date='-1y', end_date='now'),
            'status': random.choice(['completed', 'pending', 'failed']) if random.random() < 0.1 else 'completed'
        }
        transactions.append(transaction)
    
    return {
        'customers': customers,
        'merchants': merchants,
        'accounts': accounts,
        'transactions': transactions
    }

# API Routes
@api_router.get("/")
async def root():
    return {"message": "Financial Data Warehouse API", "version": "1.0.0"}

@api_router.post("/snowflake/connect")
async def connect_snowflake(connection: SnowflakeConnection):
    """Connect to Snowflake database"""
    try:
        success = snowflake_manager.connect(connection)
        if success:
            return {"message": "Successfully connected to Snowflake", "status": "connected"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@api_router.get("/snowflake/status")
async def snowflake_status():
    """Check Snowflake connection status"""
    if snowflake_manager.connection:
        return {"status": "connected", "database": snowflake_manager.connection_params.database}
    else:
        return {"status": "disconnected"}

@api_router.post("/snowflake/setup")
async def setup_database():
    """Create database schema and tables"""
    if not snowflake_manager.connection:
        raise HTTPException(status_code=400, detail="No active Snowflake connection")
    
    setup_queries = [
        # Create database and schema
        "CREATE DATABASE IF NOT EXISTS FINANCIAL_DW",
        "USE DATABASE FINANCIAL_DW",
        "CREATE SCHEMA IF NOT EXISTS PUBLIC",
        "USE SCHEMA PUBLIC",
        
        # Create dimension tables
        """CREATE OR REPLACE TABLE customers (
            customer_id VARCHAR(50) PRIMARY KEY,
            first_name VARCHAR(100),
            last_name VARCHAR(100),
            email VARCHAR(200),
            phone VARCHAR(20),
            address VARCHAR(500),
            city VARCHAR(100),
            state VARCHAR(10),
            zip_code VARCHAR(20),
            date_of_birth DATE,
            account_opening_date DATE,
            risk_score INTEGER
        )""",
        
        """CREATE OR REPLACE TABLE merchants (
            merchant_id VARCHAR(50) PRIMARY KEY,
            merchant_name VARCHAR(200),
            merchant_category VARCHAR(100),
            address VARCHAR(500),
            city VARCHAR(100),
            state VARCHAR(10),
            zip_code VARCHAR(20),
            phone VARCHAR(20),
            email VARCHAR(200)
        )""",
        
        """CREATE OR REPLACE TABLE accounts (
            account_id VARCHAR(50) PRIMARY KEY,
            customer_id VARCHAR(50),
            account_type VARCHAR(50),
            account_number VARCHAR(50),
            balance DECIMAL(15,2),
            opening_date DATE,
            status VARCHAR(20)
        )""",
        
        # Create fact table
        """CREATE OR REPLACE TABLE transactions (
            transaction_id VARCHAR(50) PRIMARY KEY,
            customer_id VARCHAR(50),
            merchant_id VARCHAR(50),
            account_id VARCHAR(50),
            amount DECIMAL(15,2),
            transaction_type VARCHAR(50),
            category VARCHAR(100),
            description TEXT,
            transaction_date TIMESTAMP,
            status VARCHAR(20)
        )""",
        
        # Create analytical views
        """CREATE OR REPLACE VIEW daily_transaction_summary AS
        SELECT 
            DATE(transaction_date) as transaction_date,
            COUNT(*) as transaction_count,
            SUM(amount) as total_amount,
            AVG(amount) as avg_amount,
            COUNT(DISTINCT customer_id) as unique_customers
        FROM transactions 
        WHERE status = 'completed'
        GROUP BY DATE(transaction_date)""",
        
        """CREATE OR REPLACE VIEW customer_spending_summary AS
        SELECT 
            c.customer_id,
            c.first_name || ' ' || c.last_name as customer_name,
            COUNT(t.transaction_id) as transaction_count,
            SUM(t.amount) as total_spent,
            AVG(t.amount) as avg_transaction,
            c.risk_score
        FROM customers c
        LEFT JOIN transactions t ON c.customer_id = t.customer_id
        WHERE t.status = 'completed' OR t.status IS NULL
        GROUP BY c.customer_id, c.first_name, c.last_name, c.risk_score"""
    ]
    
    try:
        for query in setup_queries:
            snowflake_manager.execute_query(query, fetch_results=False)
        
        return {"message": "Database schema created successfully", "tables_created": 4, "views_created": 2}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Schema creation failed: {str(e)}")

@api_router.post("/data/generate")
async def generate_data(request: DataGenerationRequest):
    """Generate and load sample data into Snowflake"""
    if not snowflake_manager.connection:
        raise HTTPException(status_code=400, detail="No active Snowflake connection")
    
    try:
        # Generate sample data
        sample_data = generate_sample_data(
            num_customers=request.customers,
            num_merchants=request.merchants,
            num_transactions=request.transactions
        )
        
        # Load data to Snowflake
        results = {}
        
        # Load customers
        customers_df = pd.DataFrame(sample_data['customers'])
        results['customers'] = snowflake_manager.load_dataframe(customers_df, 'customers', 'replace')
        
        # Load merchants
        merchants_df = pd.DataFrame(sample_data['merchants'])
        results['merchants'] = snowflake_manager.load_dataframe(merchants_df, 'merchants', 'replace')
        
        # Load accounts
        accounts_df = pd.DataFrame(sample_data['accounts'])
        results['accounts'] = snowflake_manager.load_dataframe(accounts_df, 'accounts', 'replace')
        
        # Load transactions
        transactions_df = pd.DataFrame(sample_data['transactions'])
        results['transactions'] = snowflake_manager.load_dataframe(transactions_df, 'transactions', 'replace')
        
        return {
            "message": "Sample data generated and loaded successfully",
            "data_counts": {
                "customers": len(sample_data['customers']),
                "merchants": len(sample_data['merchants']),
                "accounts": len(sample_data['accounts']),
                "transactions": len(sample_data['transactions'])
            },
            "load_results": results
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Data generation failed: {str(e)}")

@api_router.post("/query/execute")
async def execute_query(request: QueryRequest):
    """Execute custom SQL query"""
    if not snowflake_manager.connection:
        raise HTTPException(status_code=400, detail="No active Snowflake connection")
    
    try:
        # Add LIMIT if not present and limit is specified
        query = request.sql_query.strip()
        if request.limit and not query.upper().count('LIMIT'):
            query += f" LIMIT {request.limit}"
        
        result = snowflake_manager.execute_query(query)
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@api_router.get("/analytics/dashboard")
async def get_dashboard_data():
    """Get dashboard analytics data"""
    if not snowflake_manager.connection:
        raise HTTPException(status_code=400, detail="No active Snowflake connection")
    
    try:
        # Get transaction summary
        total_transactions = snowflake_manager.execute_query("SELECT COUNT(*) as count FROM transactions")
        total_amount = snowflake_manager.execute_query("SELECT SUM(amount) as total FROM transactions WHERE status = 'completed'")
        avg_transaction = snowflake_manager.execute_query("SELECT AVG(amount) as avg FROM transactions WHERE status = 'completed'")
        
        # Get daily trends
        daily_trends = snowflake_manager.execute_query("""
            SELECT DATE(transaction_date) as date, 
                   COUNT(*) as transactions, 
                   SUM(amount) as volume 
            FROM transactions 
            WHERE status = 'completed' 
            GROUP BY DATE(transaction_date) 
            ORDER BY date DESC 
            LIMIT 30
        """)
        
        # Get category breakdown
        category_breakdown = snowflake_manager.execute_query("""
            SELECT category, 
                   COUNT(*) as transaction_count, 
                   SUM(amount) as total_amount 
            FROM transactions 
            WHERE status = 'completed' 
            GROUP BY category 
            ORDER BY total_amount DESC
        """)
        
        # Get top customers
        top_customers = snowflake_manager.execute_query("""
            SELECT customer_name, transaction_count, total_spent 
            FROM customer_spending_summary 
            ORDER BY total_spent DESC 
            LIMIT 10
        """)
        
        return {
            "summary": {
                "total_transactions": total_transactions["data"][0][0] if total_transactions["data"] else 0,
                "total_amount": float(total_amount["data"][0][0]) if total_amount["data"] and total_amount["data"][0][0] else 0,
                "avg_transaction": float(avg_transaction["data"][0][0]) if avg_transaction["data"] and avg_transaction["data"][0][0] else 0
            },
            "daily_trends": daily_trends,
            "category_breakdown": category_breakdown,
            "top_customers": top_customers
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Dashboard data fetch failed: {str(e)}")

@api_router.get("/examples/queries")
async def get_example_queries():
    """Get example SQL queries for learning"""
    examples = [
        {
            "title": "Basic Transaction Query",
            "description": "Get all transactions for a specific customer",
            "query": "SELECT * FROM transactions WHERE customer_id = 'CUSTOMER_ID_HERE' ORDER BY transaction_date DESC;"
        },
        {
            "title": "Daily Transaction Summary",
            "description": "Get transaction count and total amount by day",
            "query": "SELECT DATE(transaction_date) as date, COUNT(*) as transactions, SUM(amount) as total_amount FROM transactions GROUP BY DATE(transaction_date) ORDER BY date DESC;"
        },
        {
            "title": "Top Spending Customers",
            "description": "Find customers with highest total spending",
            "query": "SELECT customer_name, total_spent, transaction_count FROM customer_spending_summary ORDER BY total_spent DESC LIMIT 10;"
        },
        {
            "title": "Fraud Detection Query",
            "description": "Find suspicious transactions (high amount, high-risk customers)",
            "query": "SELECT t.*, c.risk_score FROM transactions t JOIN customers c ON t.customer_id = c.customer_id WHERE t.amount > 1000 AND c.risk_score > 70 ORDER BY t.amount DESC;"
        },
        {
            "title": "Category Analysis",
            "description": "Analyze spending by transaction category",
            "query": "SELECT category, COUNT(*) as transaction_count, SUM(amount) as total_amount, AVG(amount) as avg_amount FROM transactions WHERE status = 'completed' GROUP BY category ORDER BY total_amount DESC;"
        },
        {
            "title": "Monthly Trends",
            "description": "Get monthly transaction trends",
            "query": "SELECT YEAR(transaction_date) as year, MONTH(transaction_date) as month, COUNT(*) as transactions, SUM(amount) as volume FROM transactions GROUP BY YEAR(transaction_date), MONTH(transaction_date) ORDER BY year DESC, month DESC;"
        }
    ]
    
    return {"examples": examples}

# Include the router in the main app
app.include_router(api_router)

app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("shutdown")
async def shutdown_db_client():
    if snowflake_manager.connection:
        snowflake_manager.connection.close()
    client.close()
