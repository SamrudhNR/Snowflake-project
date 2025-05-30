# 🏦 Financial Transaction Data Warehouse & ETL Pipeline

A comprehensive beginner-friendly Snowflake project featuring a financial data warehouse with ETL processes, real-time analytics, and an interactive dashboard.

## 🎯 Project Overview

This project teaches essential **data warehousing** and **ETL concepts** using **Snowflake** as the cloud data platform, with a Python-powered backend and React frontend dashboard.

### What You'll Learn
- ✅ **Snowflake Setup** - Free trial account creation and connection
- ✅ **Data Warehousing** - Star schema design with fact and dimension tables
- ✅ **Data Modeling** - Financial transaction data structure
- ✅ **ETL Processes** - Extract, Transform, Load pipelines
- ✅ **Financial Analytics** - Transaction analysis and KPI calculations
- ✅ **SQL Practice** - Interactive query interface with examples
- ✅ **Real-time Dashboards** - Data visualization and reporting

## 🏗️ Architecture

### Data Model (Star Schema)
```
📊 FACT TABLE: transactions
├── transaction_id (PK)
├── customer_id (FK)
├── merchant_id (FK)
├── account_id (FK)
├── amount
├── transaction_type
├── category
├── description
├── transaction_date
└── status

🔍 DIMENSION TABLES:
├── customers (customer demographics)
├── merchants (business information)
├── accounts (account details)
└── transaction_categories (classifications)

📈 ANALYTICAL VIEWS:
├── daily_transaction_summary
└── customer_spending_summary
```

### Tech Stack
- **Backend:** FastAPI + Snowflake Python Connector
- **Frontend:** React + Tailwind CSS
- **Database:** Snowflake (cloud data warehouse)
- **Analytics:** Pandas + SQL
- **Sample Data:** Faker (realistic financial data generation)

## 🚀 Getting Started

### Prerequisites
1. **Snowflake Account** (Free Trial)
   - Go to [signup.snowflake.com](https://signup.snowflake.com/)
   - Sign up for 30-day free trial
   - Choose any cloud provider (AWS/Azure/GCP)
   - Note your account identifier (e.g., `abc123.us-east-1`)

### Quick Start
1. **Access the Application**
   ```
   Frontend: http://localhost:3000
   Backend API: http://localhost:8001/api
   ```

2. **Setup Workflow**
   - Navigate to the "🔧 Setup" tab
   - Enter your Snowflake credentials
   - Click "Connect to Snowflake"
   - Create database schema
   - Generate sample data

3. **Explore Features**
   - **📊 Dashboard:** View financial analytics and KPIs
   - **💻 Query:** Practice SQL with interactive interface

## 📚 Features Deep Dive

### 1. Database Schema Setup
Automatically creates a complete financial data warehouse:
```sql
-- Dimension Tables
CREATE TABLE customers (...)
CREATE TABLE merchants (...)
CREATE TABLE accounts (...)

-- Fact Table
CREATE TABLE transactions (...)

-- Analytical Views
CREATE VIEW daily_transaction_summary AS ...
CREATE VIEW customer_spending_summary AS ...
```

### 2. Sample Data Generation
Generates realistic financial data using Faker:
- **100 Customers** with demographics and risk scores
- **50 Merchants** across various categories
- **Multiple Accounts** per customer (checking, savings, credit)
- **1000+ Transactions** with realistic patterns

### 3. ETL Pipeline Features
- **Extract:** CSV file processing and API simulation
- **Transform:** Data validation, cleansing, and enrichment
- **Load:** Staging to production with proper data types
- **Monitor:** Data quality checks and error handling

### 4. Analytics Dashboard
- **Summary KPIs:** Total transactions, volume, averages
- **Category Analysis:** Spending breakdown by transaction type
- **Customer Insights:** Top spenders and transaction patterns
- **Trend Analysis:** Daily/monthly transaction patterns

### 5. Interactive SQL Practice
Pre-built example queries for learning:
- Basic transaction queries
- Aggregation and grouping
- Join operations across tables
- Fraud detection patterns
- Time-based analysis

## 🔍 Example Queries

### Basic Transaction Analysis
```sql
-- Daily transaction summary
SELECT DATE(transaction_date) as date, 
       COUNT(*) as transactions, 
       SUM(amount) as total_volume
FROM transactions 
WHERE status = 'completed'
GROUP BY DATE(transaction_date)
ORDER BY date DESC;
```

### Fraud Detection
```sql
-- High-risk transactions
SELECT t.*, c.risk_score 
FROM transactions t 
JOIN customers c ON t.customer_id = c.customer_id 
WHERE t.amount > 1000 AND c.risk_score > 70 
ORDER BY t.amount DESC;
```

### Customer Analytics
```sql
-- Top spending customers
SELECT customer_name, 
       total_spent, 
       transaction_count 
FROM customer_spending_summary 
ORDER BY total_spent DESC 
LIMIT 10;
```

## 📊 Business Intelligence Use Cases

### 1. Financial KPIs
- Transaction volume trends
- Average transaction values
- Customer acquisition costs
- Revenue by merchant category

### 2. Risk Management
- Fraud detection algorithms
- Customer risk scoring
- Unusual transaction patterns
- Geographic spending analysis

### 3. Customer Analytics
- Spending behavior analysis
- Customer lifetime value
- Segmentation by demographics
- Retention and churn analysis

### 4. Operational Insights
- Payment processing efficiency
- Merchant performance metrics
- Account type preferences
- Peak transaction times

## 🛠️ API Endpoints

### Connection Management
- `POST /api/snowflake/connect` - Connect to Snowflake
- `GET /api/snowflake/status` - Check connection status

### Database Setup
- `POST /api/snowflake/setup` - Create schema and tables
- `POST /api/data/generate` - Generate sample data

### Analytics
- `GET /api/analytics/dashboard` - Get dashboard data
- `POST /api/query/execute` - Execute custom SQL queries
- `GET /api/examples/queries` - Get example queries

## 📖 Learning Path

### Beginner Level
1. **Setup & Connection** - Connect to Snowflake successfully
2. **Schema Understanding** - Explore table structures
3. **Basic Queries** - SELECT, WHERE, ORDER BY
4. **Data Exploration** - Count records, find patterns

### Intermediate Level
1. **Joins & Relationships** - Connect tables meaningfully
2. **Aggregations** - GROUP BY, SUM, AVG, COUNT
3. **Time-based Analysis** - Date functions and trends
4. **Conditional Logic** - CASE statements and filters

### Advanced Level
1. **Window Functions** - ROW_NUMBER, RANK, LAG/LEAD
2. **Complex Analytics** - Cohort analysis, retention
3. **Performance Optimization** - Clustering, partitioning
4. **Data Modeling** - Slowly changing dimensions

## 🔒 Security & Best Practices

### Data Security
- All credentials entered via secure form
- No hardcoded credentials in code
- Connection strings managed via environment variables
- Sample data only (no real financial information)

### Cost Management
- Uses smallest warehouse size (X-Small)
- Automatic warehouse suspension
- Query result caching enabled
- Resource monitoring included

### Development Best Practices
- Modular code architecture
- Error handling and logging
- API documentation included
- Responsive frontend design

## 🚀 Next Steps & Extensions

### Enhance the Project
1. **Advanced Analytics**
   - Machine learning fraud detection
   - Customer segmentation algorithms
   - Predictive analytics models

2. **Data Pipeline Improvements**
   - Scheduled ETL jobs
   - Real-time streaming data
   - Data quality monitoring

3. **Visualization Enhancements**
   - Interactive charts (Chart.js/D3.js)
   - Geographic analysis maps
   - Real-time dashboards

4. **Integration Expansions**
   - Additional data sources
   - External APIs integration
   - Export functionality

### Production Considerations
- Authentication and authorization
- Rate limiting and quotas
- Data backup and recovery
- Monitoring and alerting

## 📞 Support & Resources

### Snowflake Documentation
- [Snowflake Documentation](https://docs.snowflake.com/)
- [SQL Reference](https://docs.snowflake.com/en/sql-reference.html)
- [Python Connector Guide](https://docs.snowflake.com/en/user-guide/python-connector.html)

### Learning Resources
- [Snowflake University](https://university.snowflake.com/)
- [Hands-On Lab](https://quickstarts.snowflake.com/)
- [Community Support](https://community.snowflake.com/)

## 🎉 Project Benefits

This project provides hands-on experience with:
- **Modern data stack** architecture
- **Cloud data warehousing** concepts
- **ETL pipeline** development
- **Financial data** analysis patterns
- **Full-stack development** skills
- **SQL proficiency** building
- **Data visualization** techniques

Perfect for beginners wanting to learn Snowflake and data engineering fundamentals through a practical, real-world financial analytics project!

---

## 💻 Technical Setup

### Prerequisites
- Python 3.11+
- Node.js (latest LTS)
- MongoDB (for metadata storage)
- Snowflake Account (free trial)

### Installation & Running

The application is already set up and running:
- **Frontend:** http://localhost:3000
- **Backend API:** http://localhost:8001/api

### Project Structure
```
.
├── backend/
│   ├── server.py          # FastAPI + Snowflake integration
│   ├── requirements.txt   # Python dependencies
│   └── .env              # Environment variables
├── frontend/
│   ├── src/
│   │   ├── App.js        # React dashboard
│   │   ├── App.css       # Tailwind styles
│   │   └── index.js      # Entry point
│   ├── package.json      # Node.js dependencies
│   └── .env             # Frontend configuration
└── README.md
```

**Happy Learning! 🎓**

Start by accessing the application at http://localhost:3000 and following the setup instructions in the "🔧 Setup" tab.